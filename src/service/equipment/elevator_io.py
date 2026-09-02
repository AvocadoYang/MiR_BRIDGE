from enum import Enum

from .wise4060 import WISE4060


class Floor(Enum):
    A = 0b0010
    B = 0b0100


_DO_EXCLUSIVE_REQUEST = 0b0001
_DO_DOOR_HOLD = 0b0011
_DO_DOOR_RELEASE = 0b0101

_DI_EXCLUSIVE_ACTIVE = 0
_DI_FLOOR_ARRIVED = 1
_DI_DOOR_STATUS = 2


class ElevatorIO:
    """Raw DI/DO bit-level protocol on top of a WISE4060, per 電梯通訊協議規格書."""

    def __init__(self, device: WISE4060):
        self._device = device

    async def request_exclusive(self) -> None:
        await self._send(_DO_EXCLUSIVE_REQUEST)

    async def go_to(self, floor: Floor) -> None:
        await self._send(floor.value)

    async def hold_door(self) -> None:
        await self._send(_DO_DOOR_HOLD)

    async def release_door(self) -> None:
        await self._send(_DO_DOOR_RELEASE)

    async def clear(self) -> None:
        await self._device.set_all_do([False, False, False, False])

    async def is_exclusive_active(self) -> bool:
        return await self._device.is_di_high(_DI_EXCLUSIVE_ACTIVE)

    async def is_floor_arrived(self) -> bool:
        return await self._device.is_di_high(_DI_FLOOR_ARRIVED)

    async def is_door_open(self) -> bool:
        return await self._device.is_di_high(_DI_DOOR_STATUS)

    async def _send(self, bits: int) -> None:
        await self._device.set_all_do([(bits >> i) & 1 == 1 for i in range(4)])


__all__ = ['Floor', 'ElevatorIO']
