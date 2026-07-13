from enum import Enum


class PeripheralType(str, Enum):
    """字串型列舉：對應 PeripheralType 聯集字串"""

    CHARGING = 'CHARGING'
    DISPATCH = 'DISPATCH'
    STANDBY = 'STANDBY'
    STORAGE = 'STORAGE'
    EXTRA = 'EXTRA'
    ELEVATOR = 'ELEVATOR'
    ROBOTIC_ARM = 'ROBOTIC_ARM'
    CONVEYOR = 'CONVEYOR'
    LIFT_GATE = 'LIFT_GATE'
    GATE_WAIT_POINT = 'GATE_WAIT_POINT'
    PALLETIZER = 'PALLETIZER'
    ROTATE_TABLE = 'ROTATE_TABLE'
    PACKAGE = 'PACKAGE'
    STACK = 'STACK'


PERIPHERAL_TYPE_MAP = {
    PeripheralType.EXTRA: 0,
    PeripheralType.CHARGING: 7,
    PeripheralType.DISPATCH: 2,
    PeripheralType.STANDBY: 3,
    PeripheralType.STORAGE: 4,
    PeripheralType.ELEVATOR: 5,
    PeripheralType.ROBOTIC_ARM: 6,
    PeripheralType.CONVEYOR: 15,
    PeripheralType.LIFT_GATE: 8,
    PeripheralType.GATE_WAIT_POINT: 9,
    PeripheralType.PALLETIZER: 10,
    PeripheralType.ROTATE_TABLE: 11,
    PeripheralType.PACKAGE: 12,
    PeripheralType.STACK: 13,
}


class Footprint(int, Enum):
    """數值型列舉：對應 TypeScript enum Footprint"""

    HORIZONTAL = 0
    VERTICAL = 1
    SQUARE = 2  # 不可旋轉
    ROUND = 3  # 可旋轉
    ALONE = 4
