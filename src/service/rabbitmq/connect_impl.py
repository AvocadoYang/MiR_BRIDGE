import asyncio
from typing import Optional

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
)
from reactivex.subject import Subject

from src.configs import config
from src.logger import logger


class Connect_impl:
    def __init__(
        self,
    ):

        self.connection: Optional[AbstractConnection] = None
        self.channel: Optional[AbstractChannel] = None

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

            self.channel = await self.connection.channel()

            await self.channel.set_qos(prefetch_count=10)

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
        self.channel = None
