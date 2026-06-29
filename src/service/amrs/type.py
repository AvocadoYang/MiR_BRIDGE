from typing import Any, List, TypedDict

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
    connect_w_qams: bool = False
    connect_w_amr: bool = False
    session: str = ''


# ------- IO-INFO --------------
class BatteryInfo(BaseModel):
    low_battery: bool = False
    set_charge: float = 20
    battery: float = 83.5
    voltage: float = 48.3
    current: float = -3.2
    charging: bool = False
    battery_Temp: float = 31.4
    battery_flag0: str = '0x00'
    battery_flag1: str = '0x01'
    battery_flag2: str = '0x00'
    write_battery_info_time: float = 1750000000
    battery_info_error: list[Any] = []


class Twist(BaseModel):
    set_speed_time: float = 1750000000
    set_linear_x: float = 0.8
    set_linear_y: float = 0.0
    set_angular_z: float = 0.2
    odom_x: float = 12.34
    odom_y: float = 56.78
    odom_yaw: float = 1.57
    odom_linear_x: float = 0.78
    odom_angular_z: float = 0.19


class Fork(BaseModel):
    set_clamp: float = 1
    current_clamp: float = 1
    set_height: float = 1200
    current_height: float = 1198
    set_move: float = 100
    current_move: float = 99
    set_tilt: float = 5
    current_tilt: float = 5
    set_shift: float = 30
    current_shift: float = 30


class IOInfo(BaseModel):
    connect_status: bool = True
    Query: str = 'STATUS'
    Set: str = 'MOVE'
    MultiSet: str = 'MULTI_MOVE'
    error_code: str = '0'
    error_info: str = 'fake'

    enable_ultrasoumd: bool = True
    ultrasound: str = '00001111'

    enable_baffle: bool = True
    baffle_left: Any = False
    baffle_right: Any = True

    manual_mode: bool = True
    enforce_charge: bool = False

    battery_info: BatteryInfo = BatteryInfo()
    twist: Twist = Twist()
    fork: Fork = Fork()

    charge_relay_status: bool = False

    voltage: float = 48.3
    current: float = -3.2

    front_2d_layer: float = 2
    enable_2d_lidar: bool = True
    obstacle_2d_signal: bool = False
    obstacle_rear_2d_signal: bool = False
    obstacle_3d_signal: bool = False

    enable_recovery: bool = True
    enable_reboot: bool = False
    enable_tip: bool = True

    set_tip: float = 10
    tip_left: bool = False
    tip_right: bool = False

    set_height: float = 1200
    current_height: float = 1198

    set_linear_x: float = 0.8
    linear_x: float = 0
    angular_z: float = 0.2

    odom_x: float = 12.34
    odom_y: float = 56.78
    odom_w: float = 0.707

    emergency_signal: str = 'NORMAL'
    emergency_stop: bool = False
    bumper: bool = False
    recovery: bool = False
