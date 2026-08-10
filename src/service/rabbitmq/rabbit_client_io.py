import asyncio
import json
import uuid
from typing import Callable, Literal, TypeVar

import aiormq
from aio_pika import DeliveryMode, Message
from aio_pika.abc import (
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    ConsumerTag,
    ExchangeParamType,
)

from src.helper.helper import format_date
from src.logger import heartbeat_logger, logger
from src.types.amr import AMR_INFO
from src.types.cmd_id import blacklist
from src.types.rabbitmq import PUBLISH_OPTIONS, RABBIT_CREATE_EX_OPTION, RABBIT_CREATE_QUEUE_OPTIONS

from .connect_impl import Connect_impl
from .queues import CONTROL_EX, HEARTBEAT_EX, IO_EX, RES_EX
from .transaction_wrapper import ALL_REQUEST_MSG_FORMATE, ALL_RESPONSE_MSG_FORMATE

T = TypeVar('T')


class Rabbit_client_async(Connect_impl):
    def __init__(self):
        self._exchanges: dict[str, AbstractExchange] = {}
        super().__init__()

        self.rabbit_is_connect.subscribe(self.rabbitmq_connect_handler)

    async def resource_init(self):
        logger.bind(title='system').info('create RabbitMQ [EX] resource')
        if self.channel is None or self.connection is None:
            return False
        h_ex = await self.create_exchange(HEARTBEAT_EX, type='topic', options={'durable': True})
        assert h_ex is not None
        self._exchanges[HEARTBEAT_EX] = h_ex

        res_ex = await self.create_exchange(RES_EX, type='topic', options={'durable': True})
        assert res_ex is not None
        self._exchanges[RES_EX] = res_ex

        io_ex = await self.create_exchange(IO_EX, type='topic', options={'durable': True})
        assert io_ex is not None
        self._exchanges[IO_EX] = io_ex

        control_ex = await self.create_exchange(CONTROL_EX, type='topic', options={'durable': True})
        assert control_ex is not None
        self._exchanges[CONTROL_EX] = control_ex

    async def create_exchange(
        self,
        exchange_name: str,
        type: Literal['direct', 'fanout', 'topic', 'headers'] = 'direct',
        options: RABBIT_CREATE_EX_OPTION = {},
    ):
        try:
            if self.channel is None:
                raise IOError('channel is None')

            durable = options.get('durable', True)
            internal = options.get('internal', False)
            ex_arguments = options.get('arguments', {}).copy()

            exchange = await self.channel.declare_exchange(
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
        self, queue_name: str, amrId: str, options: RABBIT_CREATE_QUEUE_OPTIONS = {}
    ):
        try:
            if self.channel is None:
                raise IOError('channel is None')
            durable = options.get('durable', True)
            exclusive = options.get('exclusive', False)
            auto_delete = options.get('autoDelete', False)

            queue_arguments = options.get('arguments', {}).copy()

            if options.get('quorum'):
                queue_arguments['x-queue-type'] = 'quorum'

            queue = await self.channel.declare_queue(
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
        amrId: str,
        queue_name: str,
        exchange: ExchangeParamType,
        routing_key: str,
        q_options: RABBIT_CREATE_QUEUE_OPTIONS = {},
    ):
        queue = await self.create_queue(amrId=amrId, queue_name=queue_name, options=q_options)
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
        return tag

    def stop_consume_queue(self):
        pass

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
            if exchange_name not in self._exchanges:
                raise IOError(f'exchange {exchange_name} is None')
            exchange = self._exchanges[exchange_name]
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

    async def req_publish(
        self,
        exchange_name: str,
        routing_key: str,
        amr_info: AMR_INFO,
        message: ALL_REQUEST_MSG_FORMATE,
        *,
        id=str(uuid.uuid4()),
        options: PUBLISH_OPTIONS = PUBLISH_OPTIONS(),
    ):
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
            if exchange_name not in self._exchanges:
                raise IOError(f'exchange {exchange_name} is None')
            exchange = self._exchanges[exchange_name]
            await exchange.publish(message=msg, routing_key=routing_key)
            if message.cmd_id not in blacklist:
                logger.bind(title=amr_info.amrId).log(
                    'MQ', f'Send [req] message ({message.cmd_id}) - {message.model_dump()}'
                )
        except aiormq.exceptions.PublishError as e:
            print(f'send message failed: {e}')
