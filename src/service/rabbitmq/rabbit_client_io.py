import asyncio
import json
from typing import Callable, Literal, TypeVar

from aio_pika.abc import (
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    ConsumerTag,
    ExchangeParamType,
)

from src.logger import color, logger

from .cmd_id import blacklist
from .connect_impl import Connect_impl
from .queues import CONTROL_EX, HEARTBEAT_EX, IO_EX, RES_EX
from .type import RABBIT_CREATE_EX_OPTION, RABBIT_CREATE_QUEUE_OPTIONS

T = TypeVar('T')


class Rabbit_client_async(Connect_impl):
    def __init__(self):
        self._exchanges: dict[str, AbstractExchange] = {}
        super().__init__()

        self.rabbit_is_connect.subscribe(self.rabbitmq_connect_handler)

    async def resource_init(self):
        logger.info('create RabbitMQ [EX] resource')
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

            # logger.bind(type=amrId).info(
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
        # logger.bind(type=amrId).info(
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
                    amrId = payload['amrId']
                    if data['flag'] == 'RES':
                        if cmd_id not in blacklist:
                            logger.bind(type=amrId).log(
                                'MQ',
                                f'Receive [res] message ({color(cmd_id, (217, 160, 102))}) - {json.dumps(payload)}',
                            )
                    else:
                        if cmd_id not in blacklist:
                            logger.bind(type='amrId').log(
                                'MQ',
                                f'{color("Receive [req] message", (220, 20, 60))} ({color(cmd_id, (217, 160, 102))}) - {json.dumps(payload)}',
                            )
                    cb(data)
            except Exception:
                pass
            raise

        tag = await queue.consume(_wrapped)
        return tag

    def stop_consume_queue(self):
        pass
