from typing import List, TypedDict

from pydantic import BaseModel


class Pose(TypedDict):
    x: float
    y: float
    yaw: float


class Vector3(TypedDict):
    x: float
    y: float
    z: float


class Quaternion(TypedDict):
    x: float
    y: float
    z: float
    w: float


class TransformData(TypedDict):
    translation: Vector3
    rotation: Quaternion


class Stamp(TypedDict):
    secs: int
    nsecs: int


class Header(TypedDict):
    seq: int
    stamp: Stamp
    frame_id: str


class TransformStamped(TypedDict):
    header: Header
    child_frame_id: str
    transform: TransformData


class TFMessage(TypedDict):
    transforms: List[TransformStamped]


class Position(TypedDict):
    x: float
    y: float
    orientation: float


class Velocity(TypedDict):
    linear: float
    angular: float


class RobotError(TypedDict):
    timestamp: Stamp
    code: int
    description: str
    module: str
    nolog: bool
    non_resettable: bool


class Trolley(TypedDict):
    id: str
    length: float
    width: float
    height: float
    offset_locked_wheels: float


class HookStatus(TypedDict):
    trolley: Trolley
    trolley_attached: bool
    available: bool


class HookAngle(TypedDict):
    angle: float
    timestamp: Stamp


class HookData(TypedDict):
    angle: HookAngle
    height: float
    length: float
    brake_state: int
    gripper_state: int
    height_state: int


# --- 使用者互動相關 ---
class UserPrompt(TypedDict):
    has_request: bool
    guid: str
    user_group: str
    question: str
    options: List[str]  # 依據範例為空陣列，推測為字串清單
    timeout: Stamp


# --- 主結構：RobotStatus ---
class RobotStatus(TypedDict):
    header: Header
    battery_percentage: float
    battery_time_remaining: int
    battery_voltage: float
    distance_to_next_target: float
    errors: List[RobotError]
    footprint: str
    hook_status: HookStatus
    hook_data: HookData
    map_id: str
    unloaded_map_changes: bool
    mission_queue_id: int
    mission_text: str
    mode_id: int
    mode_text: str
    moved: float
    position: Position
    robot_name: str
    session_id: str
    software_version: str
    state_id: int
    state_text: str
    uptime: int
    velocity: Velocity
    user_prompt: UserPrompt
    safety_system_muted: bool
    joystick_low_speed_mode_enabled: bool
    joystick_web_session_id: str
    mode_key_state: str


class CONNECT_STATUS(TypedDict):
    qams_is_connect: bool
    mir_service_is_connect: bool
    rabbitmq_is_connect: bool


class AMR_INFO(BaseModel):
    amrId: str = ''
    mac_address: str = ''
    ip: str = ''
    is_enable: bool = False
    online: bool = False
    is_connect: bool = False
    session: str = ''
