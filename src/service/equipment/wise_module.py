import asyncio
import contextlib
from typing import Optional

from src.logger import logger
from src.service.rabbitmq import Rabbit_client_async
from src.service.rabbitmq.queues import IO_EX
from src.service.rabbitmq.transaction_wrapper import Send_Advantech_IO_Status
from src.types.equipment import ADVANTECH_TABLE
from src.types.web import IoStep

from .wise4060 import WISE4060

CHANNELS = 4
DEFAULT_PULSE_MS = 500
DEFAULT_WAIT_MS = 1000


class IoStepError(Exception):
    """a write the dashboard asked for could not be done; the message is shown to the user as-is"""


class Wise_Module:
    """
    One WISE-4000 module from QAMS advantechWish.

    Keeps the device's single web session, polls every DI / relay each round and publishes the
    result (connected or not) to QAMS, so QAMS can tell a disconnected module, and a dead bridge
    (no message at all), apart. Writes go through the same session.
    """

    POLL_INTERVAL = 1.0
    OFF_RETRIES = 3

    def __init__(
        self,
        id: str,
        name: str,
        ip: str,
        rabbit_service: Rabbit_client_async,
        username: str = 'root',
        password: str = '00000000',
        timeout: float = 3.0,
    ):
        self.id = id
        self.name = name
        self.ip = ip
        self.title = f'wise-{name}'
        self.rabbit_service = rabbit_service
        self.device = WISE4060(ip=ip, username=username, password=password, timeout=timeout)

        self.connected = False
        self.di: list[bool] = [False] * CHANNELS
        self.do: list[bool] = [False] * CHANNELS
        self.error = '尚未連線'

        self._logged_in = False
        self._poll_task: Optional[asyncio.Task] = None
        logger.bind(title=self.title).info(f'create advantech module instance with ip: {ip}')

    # ── lifecycle ───────────────────────────────────────

    def start(self) -> None:
        if self._poll_task is not None and not self._poll_task.done():
            return
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def close(self) -> None:
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None
        # release the device's only session so the web UI or a rebuilt instance can log in
        await self.device.close()
        self._logged_in = False
        logger.bind(title=self.title).info('advantech module instance closed')

    # ── polling ─────────────────────────────────────────

    async def _poll_loop(self) -> None:
        while True:
            await self.refresh()
            await asyncio.sleep(self.POLL_INTERVAL)

    async def refresh(self) -> None:
        """read every DI / relay once and publish it; a failure is published as disconnected"""
        was_connected = self.connected
        try:
            await self._ensure_login()
            self.di = [ch.stat == 1 for ch in await self.device.get_all_di()][:CHANNELS]
            self.do = [ch.stat == 1 for ch in await self.device.get_all_do()][:CHANNELS]
            self.connected = True
            self.error = ''
        except Exception as e:
            # the next round logs in again from scratch
            self._logged_in = False
            self.connected = False
            self.error = str(e) or type(e).__name__

        # only log the edges, a module that stays down would otherwise log every second
        if self.connected and not was_connected:
            logger.bind(title=self.title).info('module connected')
        elif not self.connected and was_connected:
            logger.bind(title=self.title).error(f'module disconnected: {self.error}')

        await self.publish()

    async def publish(self) -> None:
        # a publish failure (e.g. rabbitmq not connected yet) says nothing about the device
        try:
            await self.rabbit_service.equipment_publish(
                exchange_name=IO_EX,
                routing_key=f'equipment.io.{self.id}.status',
                equipment_id=self.title,
                message=Send_Advantech_IO_Status(
                    moduleId=self.id,
                    ip=self.ip,
                    connected=self.connected,
                    di=self.di,
                    do=self.do,
                    error=self.error,
                ),
            )
        except Exception as e:
            logger.bind(title=self.title).debug(f'failed to publish advantech IO status: {e}')

    async def _ensure_login(self) -> None:
        if not self._logged_in:
            await self.device.login()
            self._logged_in = True

    # ── writes ──────────────────────────────────────────

    async def write_output(self, channel: int, on: bool) -> None:
        if not self.connected:
            raise IoStepError(f'{self.name} ({self.ip}) 沒有連線: {self.error}')
        try:
            await self._ensure_login()
            await self.device.set_do(channel, on)
        except Exception as e:
            self._logged_in = False
            raise IoStepError(f'{self.name} ({self.ip}) RL{channel} 寫入失敗: {e}') from e
        self.do[channel] = on
        # push the new state right away instead of waiting for the next poll round
        await self.publish()
        logger.bind(title=self.title).info(f'RL{channel} -> {"ON" if on else "OFF"}')

    async def pulse(self, channel: int, ms: int) -> None:
        """on -> wait -> off, like pressing an elevator button; the off is retried so a relay is
        never left on because of one dropped request"""
        await self.write_output(channel, True)
        await asyncio.sleep(ms / 1000)
        last_error: Optional[Exception] = None
        for _ in range(self.OFF_RETRIES):
            try:
                await self._ensure_login()
                await self.device.set_do(channel, False)
                self.do[channel] = False
                await self.publish()
                logger.bind(title=self.title).info(f'RL{channel} pulse {ms}ms done')
                return
            except Exception as e:
                self._logged_in = False
                last_error = e
                await asyncio.sleep(0.2)
        raise IoStepError(
            f'{self.name} ({self.ip}) RL{channel} 脈衝後關不掉, 請手動確認: {last_error}'
        )


def _module_for(table: ADVANTECH_TABLE, step: IoStep, index: int) -> Wise_Module:
    info = table.get(step.moduleId or '')
    if info is None:
        raise IoStepError(f'第 {index + 1} 步: 橋接服務沒有這台模組, 請確認模組設定')
    return info['module']


async def run_steps(table: ADVANTECH_TABLE, steps: list[IoStep], title: str = '') -> None:
    """run a custom button's steps in order; checked up front so a missing or disconnected module
    never leaves the elevator half way through a sequence"""

    for i, step in enumerate(steps):
        if step.action == 'WAIT':
            continue
        if step.channel is None:
            raise IoStepError(f'第 {i + 1} 步: 沒有指定輸出')
        module = _module_for(table, step, i)
        if not module.connected:
            raise IoStepError(f'第 {i + 1} 步: {module.name} ({module.ip}) 沒有連線')

    logger.bind(title='advantech').info(f'run button [{title}] with {len(steps)} steps')
    for i, step in enumerate(steps):
        try:
            if step.action == 'WAIT':
                await asyncio.sleep((step.ms or DEFAULT_WAIT_MS) / 1000)
                continue
            module = _module_for(table, step, i)
            assert step.channel is not None
            if step.action == 'PULSE':
                await module.pulse(step.channel, step.ms or DEFAULT_PULSE_MS)
            else:
                await module.write_output(step.channel, step.action == 'ON')
        except IoStepError as e:
            raise IoStepError(f'第 {i + 1} 步失敗: {e}') from e


__all__ = ['Wise_Module', 'IoStepError', 'run_steps']
