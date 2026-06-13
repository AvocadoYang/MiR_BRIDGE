from typing import TYPE_CHECKING, List, Optional, TypedDict, Union

if TYPE_CHECKING:
    from src.service import AMR


class AMR_INFO(TypedDict):
    full_name: str
    ip: str
    serialNum: str
    is_enable: bool


class AMR_INFO_DETAIL(TypedDict):
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
