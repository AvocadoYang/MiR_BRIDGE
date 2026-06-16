from enum import Enum
from typing import TYPE_CHECKING, List, Optional, TypedDict, Union

if TYPE_CHECKING:
    from src.service import AMR


class REGISTER_TABLE(TypedDict):
    ip: str
    serialNum: str
    amrId: str
    is_enable: bool
    amr: Union['AMR', None]


## ============= Mission Type ========================


class ForkOperation(TypedDict):
    type: str
    control: List[str]
    wait: Union[int, float]
    is_define_id: str
    locationId: int
    is_define_yaw: int
    yaw: Union[int, float]
    tolerance: Union[int, float]
    lookahead: Union[int, float]
    waitOtherAmr: Optional[str]
    waitGenre: Optional[str]
    auto_preparatory_point: bool


class ForkIOFork(TypedDict):
    is_define_height: str
    height: Union[int, float]
    move: Union[int, float]
    shift: Union[int, float]
    tilt: Union[int, float]


class ForkIOCamera(TypedDict):
    config: int
    modify_dis: Union[int, float]


class ForkIO(TypedDict):
    fork: ForkIOFork
    camera: ForkIOCamera


class CargoLimit(TypedDict):
    load: Union[int, float]
    offload: Union[int, float]


class MissionStatus(TypedDict):
    feedback_id: str
    name: List[str]
    start: str
    end: str
    bookBlock: Optional[List[str]]
    # 使用 total=False 或 Optional 來處理 TS 中的選填欄位 (?)
    load_level: Optional[int]
    offload_level: Optional[int]


class ForkAction(TypedDict):
    operation: ForkOperation
    io: ForkIO
    cargo_limit: CargoLimit
    mission_status: MissionStatus


class Mission_Payload(TypedDict):
    Id: str
    Action: str
    Time: str
    Device: str
    Body: ForkAction


## =====================================================


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
