from enum import Enum
from wise4060 import WISE4060


class Floor(Enum):
    A = 0b0010
    B = 0b0100
    C = 0b1000


_DO_EXCLUSIVE_REQUEST = 0b0001
_DO_DOOR_HOLD = 0b0011
_DO_DOOR_RELEASE = 0b0101

_DI_EXCLUSIVE_ACTIVE = 0
_DI_FLOOR_ARRIVED = 1


class ElevatorIO:
    def __init__(self, device: WISE4060):
        self._device = device

    def request_exclusive(self) -> None:
        self._send(_DO_EXCLUSIVE_REQUEST)

    def go_to(self, floor: Floor) -> None:
        self._send(floor.value)

    def hold_door(self) -> None:
        self._send(_DO_DOOR_HOLD)

    def release_door(self) -> None:
        self._send(_DO_DOOR_RELEASE)

    def clear(self) -> None:
        self._device.set_all_do([False, False, False, False])

    def is_exclusive_active(self) -> bool:
        return self._device.is_di_high(_DI_EXCLUSIVE_ACTIVE)

    def is_floor_arrived(self) -> bool:
        return self._device.is_di_high(_DI_FLOOR_ARRIVED)

    def _send(self, bits: int) -> None:
        self._device.set_all_do([(bits >> i) & 1 == 1 for i in range(4)])


if __name__ == '__main__':
    device: WISE4060 = WISE4060(ip='10.0.0.1', username='root', password='kenmec123')
    elevator = ElevatorIO(device)

    print('專屬啟動:', elevator.is_exclusive_active())
    print('到達訊號:', elevator.is_floor_arrived())

    elevator.request_exclusive()
    elevator.go_to(Floor.B)
    elevator.hold_door()
    elevator.clear()
