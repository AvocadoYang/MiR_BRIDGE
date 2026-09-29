import asyncio
from enum import Enum
from typing import Optional

from .wise4060 import WISE4060


class Floor(Enum):
    """2-bit floor code carried on ch2 (low) / ch3 (high), shared by DO and DI."""

    F3 = 0b01
    F5 = 0b10
    F6 = 0b11


# DO (control system -> elevator); each bit is a level signal held until changed
_DO_EXCLUSIVE = 0  # 1 = request exclusive mode, 0 = cancel exclusive mode
_DO_DOOR_OPEN = 1  # 1 = hold the door-open button, 0 = let go
# DI (elevator -> control system)
_DI_EXCLUSIVE_ACTIVE = 0  # 1 = exclusive mode active
_DI_DOOR_FULLY_OPEN = 1  # 1 = door fully open, 0 = not fully open
# DO ch2/ch3 = target floor, DI ch2/ch3 = current floor; 00 = none / unknown
_FLOOR_SHIFT = 2
_FLOOR_MASK = 0b11 << _FLOOR_SHIFT


def _decode_floor(bits: int) -> Optional[Floor]:
    code = (bits & _FLOOR_MASK) >> _FLOOR_SHIFT
    return Floor(code) if code else None


class ElevatorIO:
    """Raw DI/DO bit-level protocol on top of a WISE4060, per 電梯通訊協議規格書."""

    def __init__(self, device: WISE4060):
        self._device = device
        # DO bits are read-modify-written, so concurrent updates must not interleave
        self._do_lock = asyncio.Lock()

    # ── DO ──────────────────────────────────────────────

    async def request_exclusive(self) -> None:
        await self._update_do(1 << _DO_EXCLUSIVE, 1 << _DO_EXCLUSIVE)

    async def cancel_exclusive(self) -> None:
        await self._update_do(1 << _DO_EXCLUSIVE, 0)

    async def hold_door(self) -> None:
        await self._update_do(1 << _DO_DOOR_OPEN, 1 << _DO_DOOR_OPEN)

    async def release_door(self) -> None:
        await self._update_do(1 << _DO_DOOR_OPEN, 0)

    async def go_to(self, floor: Floor) -> None:
        await self._update_do(_FLOOR_MASK, floor.value << _FLOOR_SHIFT)

    async def clear_floor(self) -> None:
        await self._update_do(_FLOOR_MASK, 0)

    async def clear(self) -> None:
        async with self._do_lock:
            await self._device.set_all_do([False, False, False, False])

    # ── DI ──────────────────────────────────────────────

    async def is_exclusive_active(self) -> bool:
        return await self._device.is_di_high(_DI_EXCLUSIVE_ACTIVE)

    async def is_door_fully_open(self) -> bool:
        return await self._device.is_di_high(_DI_DOOR_FULLY_OPEN)

    async def current_floor(self) -> Optional[Floor]:
        return _decode_floor(await self._read_di_bits())

    async def is_floor_arrived(self, floor: Floor) -> bool:
        return await self.current_floor() == floor

    async def get_di_status(self) -> dict:
        """Read every DI channel and return the named states used by callers."""
        bits = await self._read_di_bits()
        floor = _decode_floor(bits)
        return {
            'is_exclusive': bool(bits >> _DI_EXCLUSIVE_ACTIVE & 1),
            'is_door_opened': bool(bits >> _DI_DOOR_FULLY_OPEN & 1),
            'current_floor': floor.name.removeprefix('F') if floor else None,
        }

    # ── helpers ─────────────────────────────────────────

    async def _read_di_bits(self) -> int:
        return sum(ch.stat << ch.ch for ch in await self._device.get_all_di())

    async def _update_do(self, mask: int, value: int) -> None:
        """Change only the DO bits in `mask`, keeping the rest as the device reports them."""
        async with self._do_lock:
            current = sum(ch.stat << ch.ch for ch in await self._device.get_all_do())
            bits = (current & ~mask) | (value & mask)
            await self._device.set_all_do([(bits >> i) & 1 == 1 for i in range(4)])


__all__ = ['Floor', 'ElevatorIO']
