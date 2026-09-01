import asyncio
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


@dataclass
class RequestResult:
    connected: bool
    exclusive_granted: bool
    cancelled: bool = False
    steps: List[tuple] = field(default_factory=list)  # (step_name, confirmed | None)

    @property
    def success(self) -> bool:
        return (
            self.connected
            and self.exclusive_granted
            and not self.cancelled
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
        self._cancel_event = asyncio.Event()
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
        self, floor: Floor, wait_arrival: bool = True, background: bool = False
    ) -> Optional[RequestResult]:
        step = _Step(
            name=f'go_to_{floor.name}',
            perform=lambda io: io.go_to(floor),
            confirm=(lambda io: io.is_floor_arrived()) if wait_arrival else None,
        )
        return await self.request([step], background=background)

    async def hold_door(self, background: bool = False) -> Optional[RequestResult]:
        return await self.request(
            [_Step(name='hold_door', perform=lambda io: io.hold_door())], background=background
        )

    async def release_door(self, background: bool = False) -> Optional[RequestResult]:
        return await self.request(
            [_Step(name='release_door', perform=lambda io: io.release_door())],
            background=background,
        )

    async def request(
        self, steps: List[_Step], background: bool = False
    ) -> Optional[RequestResult]:
        """Run a full connect -> exclusive -> action(s) -> release cycle for the given steps.

        If `background` is True, this only waits for the elevator connection to be
        confirmed reachable, then lets the rest of the cycle (exclusive mode, the
        action(s), and release) continue as a background task. Returns None in that
        case - callers that need the outcome should use `background=False` (the default).
        """
        if not background:
            return await self._run_request(steps)

        await self.ensure_connected()
        asyncio.create_task(self._run_request(steps))
        return None

    async def _run_request(self, steps: List[_Step]) -> RequestResult:
        async with self._lock:
            if self.current_state_value is None:
                await self.activate_initial_state()

            self._steps = list(steps)
            self._connected_ok = False
            # self._exclusive_ok = False
            self._step_results = []
            self._cancel_event = asyncio.Event()

            await self.send('start_request')

            return RequestResult(
                connected=self._connected_ok,
                exclusive_granted=self._exclusive_ok,
                cancelled=self._cancel_event.is_set(),
                steps=list(self._step_results),
            )

    async def cancel_action(self) -> None:
        """Abort whatever the machine is currently doing or waiting on and return
        to idle. If exclusive control had been granted, it is released as part of
        the abort. Safe to call at any time, from any state."""
        logger.bind(title=self.id).warning('CANCEL_ACTION received, aborting current request')
        self._cancel_event.set()

    # ── state callbacks ─────────────────────────────────

    async def on_enter_checking_connection(self):
        logger.bind(title=self.id).info('checking elevator connection...')
        try:
            await self.ensure_connected()
            self._connected_ok = True
            await self.send('connection_ok')
        except Exception as e:
            self._connected_ok = False
            self._logged_in = False
            logger.bind(title=self.id).error(f'elevator connection failed: {e}')
            await self.send('connection_failed')

    async def on_enter_requesting_exclusive(self):
        logger.bind(title=self.id).info('requesting exclusive mode...')
        io = self.io
        await io.request_exclusive()

        granted = await self._poll(io.is_exclusive_active)
        self._exclusive_ok = granted
        if granted:
            await self.send('exclusive_confirmed')
        else:
            if self._cancel_event.is_set():
                logger.bind(title=self.id).info('exclusive mode request cancelled')
            else:
                logger.bind(title=self.id).warning('exclusive mode request was denied')
            await self.send('exclusive_denied')

    async def on_enter_exclusive_active(self):
        logger.bind(title=self.id).info('exclusive mode is active')
        if self._steps and not self._cancel_event.is_set():
            await self.send('perform_next')
        else:
            await self.send('finish')

    async def on_enter_performing_action(self):
        logger.bind(title=self.id).info('performing elevator action...')
        io = self.io
        step = self._steps.pop(0)
        await step.perform(io)

        confirmed = None
        confirm = step.confirm
        if confirm is not None:
            confirmed = await self._poll(lambda: confirm(io))
            if not confirmed:
                if self._cancel_event.is_set():
                    logger.bind(title=self.id).info(f'elevator action "{step.name}" cancelled')
                else:
                    logger.bind(title=self.id).warning(
                        f'elevator action "{step.name}" was not confirmed'
                    )
        self._step_results.append((step.name, confirmed))
        await self.send('step_done')

    async def on_enter_releasing(self):
        logger.bind(title=self.id).info('releasing exclusive mode...')
        try:
            await self.io.clear()
        except Exception as e:
            logger.bind(title=self.id).error(f'failed to release exclusive mode: {e}')
        await self.send('released')

    async def _poll(self, predicate: Callable[[], Awaitable[bool]]) -> bool:
        logger.bind(title=self.id).info('polling...')
        while not self._cancel_event.is_set():
            try:
                if await predicate():
                    return True
            except Exception as e:
                logger.bind(title=self.id).error(f'elevator poll failed: {e}')
                return False
            await asyncio.sleep(self.POLL_INTERVAL)
        return False
