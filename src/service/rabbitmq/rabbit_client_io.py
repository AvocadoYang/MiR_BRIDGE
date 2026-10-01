import asyncio
import json
import uuid
from typing import Callable, Literal, Optional, TypeVar

import aiormq
from aio_pika import DeliveryMode, Message
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    ConsumerTag,
    ExchangeParamType,
)

from src.helper.helper import format_date
from src.logger import heartbeat_logger, logger
from src.types.amr import AMR_INFO, REGISTER_TABLE
from src.types.cmd_id import blacklist
from src.types.rabbitmq import PUBLISH_OPTIONS, RABBIT_CREATE_EX_OPTION, RABBIT_CREATE_QUEUE_OPTIONS

from .connect_impl import Connect_impl
from .queues import HANDSHAKE_EX, HEARTBEAT_EX, IO_EX, RES_EX, help2init_queue_exchange_relationship
from .transaction_wrapper import ALL_REQUEST_MSG_FORMATE, ALL_RESPONSE_MSG_FORMATE

T = TypeVar('T')


class Rabbit_client_async(Connect_impl):
    def __init__(
        self,
        register_table: REGISTER_TABLE,
    ):
        self._exchanges: dict[str, AbstractExchange] = {}
        self._channels: dict[str, AbstractChannel] = {}
        self._amr_exchanges: dict[str, dict[str, AbstractExchange]] = {}
        self._channel_locks: dict[str, asyncio.Lock] = {}
        self._system_channel: Optional[AbstractChannel] = None
        super().__init__(register_table=register_table)

        self.rabbit_is_connect.subscribe(self.rabbitmq_connect_handler)

    async def resource_init(self):
        logger.bind(title='system').info('create RabbitMQ [EX] resource')
        if self.connection is None:
            return False
        try:
            self._system_channel = await self.connection.channel()
            await self._system_channel.set_qos(prefetch_count=10)
        except Exception as e:
            logger.bind(title='system').error(f'create system channel failed: {e}')
            return False

        h_ex = await self.create_exchange(
            self._system_channel, HEARTBEAT_EX, type='topic', options={'durable': True}
        )
        assert h_ex is not None
        self._exchanges[HEARTBEAT_EX] = h_ex

        res_ex = await self.create_exchange(
            self._system_channel, RES_EX, type='topic', options={'durable': True}
        )
        assert res_ex is not None
        self._exchanges[RES_EX] = res_ex

        io_ex = await self.create_exchange(
            self._system_channel, IO_EX, type='topic', options={'durable': True}
        )
        assert io_ex is not None
        self._exchanges[IO_EX] = io_ex

        handshake_ex = await self.create_exchange(
            self._system_channel, HANDSHAKE_EX, type='topic', options={'durable': True}
        )
        assert handshake_ex is not None
        self._exchanges[HANDSHAKE_EX] = handshake_ex

        for help2Init in help2init_queue_exchange_relationship():
            queue = await self.create_queue(
                channel=self._system_channel, amrId='', queue_name=help2Init['q_name']
            )

    async def get_channel(self, mac_address: str) -> Optional[AbstractChannel]:
        """
        get this AMR's own channel, creating and caching it on first use.
        concurrent callers for the same mac_address (e.g. the AMR's own connect
        handler racing a lazy publish) are serialized so the channel is only
        ever created once.
        """
        channel = self._channels.get(mac_address)
        if channel is not None and not channel.is_closed:
            return channel

        lock = self._channel_locks.setdefault(mac_address, asyncio.Lock())
        async with lock:
            # re-check: another caller may have created it while we awaited the lock
            channel = self._channels.get(mac_address)
            if channel is not None and not channel.is_closed:
                return channel

            result = await self.create_channel(mac_address=mac_address)
            channel = result['channel']
            if channel is None:
                return None

            channel.close_callbacks.add(self._on_amr_channel_close_binder(mac_address))
            self._channels[mac_address] = channel
            return channel

    async def close_channel(self, mac_address: str):
        channel = self._channels.pop(mac_address, None)
        self._amr_exchanges.pop(mac_address, None)
        self._channel_locks.pop(mac_address, None)
        if channel is not None and not channel.is_closed:
            await channel.close()

    def _on_amr_channel_close_binder(self, mac_address: str):
        async def _on_close_listener(sender, exc: Optional[BaseException]):
            self._channels.pop(mac_address, None)
            self._amr_exchanges.pop(mac_address, None)

        return _on_close_listener

    async def _get_amr_exchange(
        self, mac_address: str, exchange_name: str
    ) -> Optional[AbstractExchange]:
        amr_exchanges = self._amr_exchanges.setdefault(mac_address, {})
        if exchange_name in amr_exchanges:
            return amr_exchanges[exchange_name]

        channel = await self.get_channel(mac_address)
        if channel is None:
            return None

        try:
            exchange = await channel.get_exchange(exchange_name, ensure=False)
        except Exception as e:
            amrId = self.register_table.get(mac_address, {}).get('amrId', mac_address)
            logger.bind(title=amrId).error(e)
            return None

        amr_exchanges[exchange_name] = exchange
        return exchange

    async def create_exchange(
        self,
        channel: AbstractChannel,
        exchange_name: str,
        type: Literal['direct', 'fanout', 'topic', 'headers'] = 'direct',
        options: RABBIT_CREATE_EX_OPTION = {},
    ):
        try:
            if channel is None:
                raise IOError('channel is None')

            durable = options.get('durable', True)
            internal = options.get('internal', False)
            ex_arguments = options.get('arguments', {}).copy()

            exchange = await channel.declare_exchange(
                name=exchange_name,
                type=type,
                durable=durable,
                internal=internal,
                arguments=ex_arguments,
            )
            if exchange is None:
                raise RuntimeError(f'Failed to create exchange {exchange_name}')

            # log.info(
            #     f'exchange "{exchange_name}" is ready. Options: '
            #     f'durable={durable}, internal={internal}, '
            #     f'arguments={ex_arguments}'
            # )

            return exchange

        except IOError as e:
            logger.warning(e)
        except RuntimeError as e:
            logger.error(e)

    async def create_queue(
        self,
        channel: AbstractChannel,
        queue_name: str,
        amrId: str,
        options: RABBIT_CREATE_QUEUE_OPTIONS = {},
    ):
        try:
            if channel is None:
                raise IOError('channel is None')
            durable = options.get('durable', True)
            exclusive = options.get('exclusive', False)
            auto_delete = options.get('autoDelete', False)

            queue_arguments = options.get('arguments', {}).copy()

            if options.get('quorum'):
                queue_arguments['x-queue-type'] = 'quorum'

            queue = await channel.declare_queue(
                name=queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=queue_arguments,
            )

            # logger.bind(title=amrId).info(
            #     f'Queue "{queue_name}" is created. Options: '
            #     f'durable={durable}, exclusive={exclusive}, '
            #     f'autoDelete={auto_delete}, arguments={queue_arguments}'
            # )

            return queue

        except IOError as e:
            logger.warning(e)

    async def create_queue_and_bind(
        self,
        channel: AbstractChannel,
        amrId: str,
        queue_name: str,
        exchange: ExchangeParamType,
        routing_key: str,
        q_options: RABBIT_CREATE_QUEUE_OPTIONS = {},
    ):
        queue = await self.create_queue(
            channel=channel, amrId=amrId, queue_name=queue_name, options=q_options
        )
        assert queue is not None
        await queue.bind(exchange=exchange, routing_key=routing_key)
        # logger.bind(title=amrId).info(
        #     f'binding queue "{queue_name}" in exchange "{exchange}" with key "{routing_key}"'
        # )
        return queue

    def rabbitmq_connect_handler(self, is_connect: bool):
        if is_connect:
            asyncio.create_task(self.resource_init())
        else:
            logger.info('delete RabbitMQ [EX] resource')
            self._exchanges.clear()
            self._channels.clear()
            self._amr_exchanges.clear()
            self._channel_locks.clear()
            self._system_channel = None

    async def consume_queue(
        self,
        amrId: str,
        queue: AbstractQueue,
        *,
        cb: Callable[[T], None],
    ) -> ConsumerTag:

        async def _wrapped(msg: AbstractIncomingMessage):
            try:
                async with msg.process():
                    data = json.loads(msg.body)
                    payload = data['payload']
                    cmd_id = payload['cmd_id']
                    if data['flag'] == 'RES':
                        if cmd_id not in blacklist:
                            logger.bind(title=amrId).log(
                                'MQ',
                                f'Receive [res] message ({cmd_id}) - {json.dumps(payload)}',
                            )
                    else:
                        if cmd_id not in blacklist:
                            logger.bind(title=amrId).log(
                                'MQ',
                                f'Receive [req] message ({cmd_id}) - {json.dumps(payload)}',
                            )
                    cb(data)
            except Exception:
                pass

        tag = await queue.consume(_wrapped)
        logger.bind(title=amrId).info(f'start consume queue [ {queue.name} ]')
        return tag

    async def stop_consume_queue(self, queue: AbstractQueue, consumer_tag: ConsumerTag, amrId: str):
        try:
            await queue.cancel(consumer_tag)
            logger.bind(title=amrId).info(f'stop consume queue {queue.name}')
        except Exception as e:
            logger.warning(f'cancel consumer {consumer_tag} failed: {e}')

    async def res_publish(
        self,
        exchange_name: str,
        routing_key: str,
        last_receive_req: dict[str, str],
        mac_address: str,
        message: ALL_RESPONSE_MSG_FORMATE,
        options: PUBLISH_OPTIONS = PUBLISH_OPTIONS(),
    ):
        try:
            flag = 'RES'
            if message.cmd_id not in last_receive_req:
                raise Exception("can't get the corresponding request")
            req_session = last_receive_req[message.cmd_id]
            r_msg = {
                'id': message.id,
                'seder': 'MiR_Bridge',
                'serialNum': mac_address,
                'session': req_session,
                'flag': flag,
                'timestamp': format_date(),
                'payload': message.model_dump(),
            }
            b_msg = json.dumps(r_msg, ensure_ascii=False).encode('utf-8')
            msg = Message(
                body=b_msg,
                content_type='application/json',
                expiration=options.expiration,
                delivery_mode=(
                    DeliveryMode.PERSISTENT if options.persistent else DeliveryMode.NOT_PERSISTENT
                ),
            )
            exchange = await self._get_amr_exchange(mac_address, exchange_name)
            if exchange is None:
                raise IOError(f'exchange {exchange_name} is None')
            await exchange.publish(message=msg, routing_key=routing_key)
            if message.cmd_id == 'HB':
                heartbeat_logger.bind(title=message.amrId, state='heartbeat').info(
                    f'Response heartbeat to QAMS {message.model_dump_json()}'
                )
                return True
            elif message.cmd_id not in blacklist:
                logger.bind(title=message.amrId).log(
                    'MQ', f'Send [res] message ({message.cmd_id}) - {message.model_dump_json()}'
                )
        except aiormq.exceptions.PublishError as e:
            print(f'send message failed: {e}')

    async def equipment_publish(
        self,
        exchange_name: str,
        routing_key: str,
        equipment_id: str,
        message: ALL_REQUEST_MSG_FORMATE,
        options: PUBLISH_OPTIONS = PUBLISH_OPTIONS(),
    ):
        """
        publish a REQ message for equipment (e.g. elevator). unlike req_publish, equipment has no
        mac address / session / register_table entry, so it publishes through the exchange
        declared on the system channel instead of a per-AMR channel.
        """

        exchange = self._exchanges.get(exchange_name)
        if exchange is None:
            raise IOError(f'exchange {exchange_name} is not ready (rabbitmq not connected yet?)')

        id = str(uuid.uuid4())
        r_msg = {
            'id': id,
            'sender': 'MiR_Bridge',
            'serialNum': equipment_id,
            'session': '',
            'flag': 'REQ',
            'timestamp': format_date(),
            'payload': {'id': id, **message.model_dump()},
        }
        msg = Message(
            body=json.dumps(r_msg, ensure_ascii=False).encode('utf-8'),
            content_type='application/json',
            expiration=options.expiration,
            delivery_mode=(
                DeliveryMode.PERSISTENT if options.persistent else DeliveryMode.NOT_PERSISTENT
            ),
        )
        await exchange.publish(message=msg, routing_key=routing_key)
        if message.cmd_id not in blacklist:
            logger.bind(title=equipment_id).log(
                'MQ', f'Send [req] message ({message.cmd_id}) - {message.model_dump()}'
            )

    async def req_publish(
        self,
        exchange_name: str,
        routing_key: str,
        amr_info: AMR_INFO,
        message: ALL_REQUEST_MSG_FORMATE,
        *,
        id: Optional[str] = None,
        options: PUBLISH_OPTIONS = PUBLISH_OPTIONS(),
    ):
        # a str(uuid.uuid4()) default is evaluated once at definition, so every call would share it
        id = id or str(uuid.uuid4())
        try:
            r_msg = {
                'id': id,
                'sender': 'MiR_Bridge',
                'serialNum': amr_info.mac_address,
                'session': amr_info.session,
                'flag': 'REQ',
                'timestamp': format_date(),
                'payload': {'id': id, **message.model_dump(), 'amrId': amr_info.amrId},
            }
            b_msg = json.dumps(r_msg, ensure_ascii=False).encode('utf-8')
            msg = Message(
                body=b_msg,
                content_type='application/json',
                expiration=options.expiration,
                delivery_mode=(
                    DeliveryMode.PERSISTENT if options.persistent else DeliveryMode.NOT_PERSISTENT
                ),
            )
            exchange = await self._get_amr_exchange(amr_info.mac_address, exchange_name)

            if exchange is None:
                raise IOError(f'exchange {exchange_name} is None')
            await exchange.publish(message=msg, routing_key=routing_key)
            if message.cmd_id not in blacklist:
                logger.bind(title=amr_info.amrId).log(
                    'MQ', f'Send [req] message ({message.cmd_id}) - {message.model_dump()}'
                )
        except aiormq.exceptions.PublishError as e:
            print(f'send message failed: {e}')
