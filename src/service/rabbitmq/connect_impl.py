import asyncio
from typing import Optional, TypedDict

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
)
from reactivex.subject import Subject

from src.configs import config
from src.logger import logger
from src.types.amr import REGISTER_TABLE


class CreateChannelResult(TypedDict):
    mac_address: str
    channel: Optional[AbstractChannel]


class Connect_impl:
    def __init__(
        self,
        register_table: REGISTER_TABLE,
    ):

        self.connection: Optional[AbstractConnection] = None

        self.register_table = register_table

        self._is_shutting_down: bool = False

        self._reconnect_task: Optional[asyncio.Task] = None

        self._show_connect_logger = True

        self.rabbit_is_connect: Subject[bool] = Subject()

    async def connect(self):
        try:
            self.connection = await aio_pika.connect(
                f'amqp://{config.RABBIT_MQ_USER}:{config.RABBIT_MQ_PASSWORD}@{config.RABBIT_MQ_HOST}:{config.RABBIT_MQ_PORT}/',
                heartbeat=10,
                timeout=5,
            )

            logger.bind(title='system').info(
                f"connected to rabbitMQ node: 'amqp://{config.RABBIT_MQ_USER}:{config.RABBIT_MQ_PASSWORD}@{config.RABBIT_MQ_HOST}:{config.RABBIT_MQ_PORT}/'",
            )
            self.connection.close_callbacks.add(self._on_close)

            self._show_connect_logger = True
            self.rabbit_is_connect.on_next(True)

            return True

        except (
            aio_pika.exceptions.AMQPConnectionError,
            asyncio.TimeoutError,
            OSError,
        ) as e:
            if self._show_connect_logger:
                logger.error(f'Connect failed: {e}.')
            return False

    async def create_channel(self, mac_address: str) -> CreateChannelResult:
        amrId = self.register_table[mac_address]['amrId']
        try:
            if self.connection is None:
                return {'mac_address': mac_address, 'channel': None}
            channel = await self.connection.channel()
            channel.close_callbacks.add(self._on_close_binder(mac_address=mac_address))
            await channel.set_qos(prefetch_count=10)
            return {'mac_address': mac_address, 'channel': channel}
        except Exception as e:
            logger.bind(title=amrId).error(e)
            return {'mac_address': mac_address, 'channel': None}

    async def close(self):
        self._is_shutting_down = True
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        self._reset()

    def _on_close_binder(self, mac_address: str):
        async def _on_close_listener(sender, exc: Optional[BaseException]):
            amrId = self.register_table[mac_address]['amrId']
            logger.bind(title=amrId).warning(f'{mac_address} channel close with error: {exc} ')

        return _on_close_listener

    async def _on_close(self, sender, exc: Optional[BaseException]):
        if self._is_shutting_down:
            logger.info('rabbitMQ connection closed gracefully (App Shutting Down)')
            return
        if exc:
            logger.error(f'rabbitMQ connection closed with error: {exc}')
        else:
            logger.warning(
                'RabbitMQ connection lost unexpectedly (Remote server disconnected or heartbeat timeout)'
            )
        self._reset()
        self.rabbit_is_connect.on_next(False)
        self._trigger_reconnect()

    def _trigger_reconnect(self):
        if self._is_shutting_down:
            return

        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self):
        while not self._is_shutting_down:
            success = await self.connect()
            if success:
                break

            if self._show_connect_logger:
                logger.info('Will retry to connect RabbitMQ in 3 seconds...')
                self._show_connect_logger = False

            await asyncio.sleep(3)

    def _reset(self):
        self.connection = None
