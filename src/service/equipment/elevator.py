import asyncio
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, List, Optional

from statemachine import State, StateChart

from src.logger import logger

from .elevator_io import ElevatorIO, Floor
from .wise4060 import WISE4060


@dataclass
class _Step:
    name: str
    perform: Callable[[ElevatorIO], Awaitable[None]]
    confirm: Optional[Callable[[ElevatorIO], Awaitable[bool]]] = None
    timeout: float = 30.0


@dataclass
class RequestResult:
    connected: bool
    exclusive_granted: bool
    steps: List[tuple] = field(default_factory=list)  # (step_name, confirmed | None)

    @property
    def success(self) -> bool:
        return (
            self.connected
            and self.exclusive_granted
            and all(confirmed is not False for _, confirmed in self.steps)
        )


class Elevator_Machine(StateChart):
    """
    Elevator state machine driven over a WISE4060 relay module.

    Every public request (go_to / hold_door / release_door) walks the same
    lifecycle: verify the connection -> request exclusive control -> wait for
    the elevator to confirm it granted exclusive control -> run the requested
    action(s) -> release exclusive control once all actions are done.
    """

    EXCLUSIVE_CONFIRM_TIMEOUT = 10.0
    POLL_INTERVAL = 0.3

    disconnected = State(initial=True)
    checking_connection = State()
    idle = State()
    requesting_exclusive = State()
    exclusive_active = State()
    performing_action = State()
    releasing = State()

    start_request = disconnected.to(checking_connection) | idle.to(checking_connection)
    connection_ok = checking_connection.to(requesting_exclusive)
    connection_failed = checking_connection.to(disconnected)
    exclusive_confirmed = requesting_exclusive.to(exclusive_active)
    exclusive_denied = requesting_exclusive.to(releasing)
    perform_next = exclusive_active.to(performing_action)
    step_done = performing_action.to(exclusive_active)
    finish = exclusive_active.to(releasing)
    released = releasing.to(idle)

    def __init__(
        self,
        locationId: str,
        ip: str,
        username: str = 'root',
        password: str = '00000000',
        timeout: float = 3.0,
    ):
        self.id = f'elevator-{locationId}'
        logger.bind(title=self.id).info(f'create elevator instant with ip: {ip}')
        self.ip = ip
        self.username = username
        self.password = password
        self.timeout = timeout

        self.device = WISE4060(ip=ip, username=username, password=password, timeout=timeout)
        self.io = ElevatorIO(self.device)
        self._logged_in = False

        self._lock = asyncio.Lock()
        self._steps: List[_Step] = []
        self._connected_ok = False
        self._exclusive_ok = False
        self._step_results: List[tuple] = []
        super().__init__()

    async def close(self) -> None:
        await self.device.close()
        self._logged_in = False

    async def ensure_connected(self) -> None:
        """Log in if needed. Call before touching `self.device` directly, outside
        of the go_to/hold_door/release_door request cycle."""
        if not self._logged_in or not await self.device.check_alive():
            await self.device.login()
            self._logged_in = True

    # ── public requests ────────────────────────────────

    async def go_to(
        self, floor: Floor, wait_arrival: bool = True, arrival_timeout: float = 30.0
    ) -> RequestResult:
        step = _Step(
            name=f'go_to_{floor.name}',
            perform=lambda io: io.go_to(floor),
            confirm=(lambda io: io.is_floor_arrived()) if wait_arrival else None,
            timeout=arrival_timeout,
        )
        return await self.request([step])

    async def hold_door(self) -> RequestResult:
        return await self.request([_Step(name='hold_door', perform=lambda io: io.hold_door())])

    async def release_door(self) -> RequestResult:
        return await self.request(
            [_Step(name='release_door', perform=lambda io: io.release_door())]
        )

    async def request(self, steps: List[_Step]) -> RequestResult:
        """Run a full connect -> exclusive -> action(s) -> release cycle for the given steps."""

        async with self._lock:
            if self.current_state_value is None:
                await self.activate_initial_state()

            self._steps = list(steps)
            self._connected_ok = False
            self._exclusive_ok = False
            self._step_results = []

            await self.send('start_request')

            return RequestResult(
                connected=self._connected_ok,
                exclusive_granted=self._exclusive_ok,
                steps=list(self._step_results),
            )

    # ── state callbacks ─────────────────────────────────

    async def on_enter_checking_connection(self):
        try:
            await self.ensure_connected()
            self._connected_ok = True
            await self.send('connection_ok')
        except Exception as e:
            self._connected_ok = False
            self._logged_in = False
            logger.bind(title=self.ip).error(f'elevator connection failed: {e}')
            await self.send('connection_failed')

    async def on_enter_requesting_exclusive(self):
        io = self.io
        await io.request_exclusive()
        granted = await self._poll(io.is_exclusive_active, self.EXCLUSIVE_CONFIRM_TIMEOUT)
        self._exclusive_ok = granted
        if granted:
            await self.send('exclusive_confirmed')
        else:
            logger.bind(title=self.ip).warning('exclusive mode request was not confirmed in time')
            await self.send('exclusive_denied')

    async def on_enter_exclusive_active(self):
        if self._steps:
            await self.send('perform_next')
        else:
            await self.send('finish')

    async def on_enter_performing_action(self):
        io = self.io
        step = self._steps.pop(0)
        await step.perform(io)

        confirmed = None
        confirm = step.confirm
        if confirm is not None:
            confirmed = await self._poll(lambda: confirm(io), step.timeout)
            if not confirmed:
                logger.bind(title=self.ip).warning(
                    f'elevator action "{step.name}" was not confirmed within {step.timeout}s'
                )
        self._step_results.append((step.name, confirmed))
        await self.send('step_done')

    async def on_enter_releasing(self):
        try:
            await self.io.clear()
        except Exception as e:
            logger.bind(title=self.ip).error(f'failed to release exclusive mode: {e}')
        await self.send('released')

    async def _poll(self, predicate: Callable[[], Awaitable[bool]], timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if await predicate():
                    return True
            except Exception as e:
                logger.bind(title=self.ip).error(f'elevator poll failed: {e}')
                return False
            await asyncio.sleep(self.POLL_INTERVAL)
        return False
