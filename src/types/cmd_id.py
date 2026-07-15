from enum import Enum


class CMD_ID(Enum):
    HEARTBEAT = 'HB'
    POSE = 'PS'
    WRITE_STATUS = 'WS'
    FEEDBACK = 'FB'
    READ_STATUS = 'RS'
    WRITE_CANCEL = 'WC'
    ERROR_INFO = 'EI'
    UPDATE_MAP = 'UM'
    IO_INFO = 'IO'
    CURRENT_ID = 'CI'
    EMERGENCY_STOP = 'ET'
    FORCE_RESET = 'FR'
    CHECK_POSITION = 'CP'
    CARGO_VERITY = 'CV'
    STACK_INFO = 'SI'
    REGISTER = 'RG'
    SHORTEST_PATH = 'SP'
    REROUTE_PATH = 'RP'
    ALLOW_PATH = 'AP'
    REGISTERED = 'RD'
    PTVP_SWITCH = 'PTVP_SWITCH'

    HAS_CARGO = 'HC'
    HAS_MISSION = 'HM'
    SYNC_DATA = 'SYNC'

    ETX = 'ETX'

    ## for Mir
    PURE_MOVE_ACTION = 'PURE_MOVE_ACTION'
    MIR_AMR_STATUS = 'MIR_AMR_STATUS'
    POINT_CLOUD = 'POINT_CLOUD'


blacklist = [
    CMD_ID.HEARTBEAT.value,
    CMD_ID.POSE.value,
    CMD_ID.FEEDBACK.value,
    CMD_ID.IO_INFO.value,
    CMD_ID.CURRENT_ID.value,
    CMD_ID.ERROR_INFO.value,
    CMD_ID.REGISTERED.value,
    CMD_ID.CHECK_POSITION.value,
    CMD_ID.HAS_CARGO.value,
    CMD_ID.HAS_MISSION.value,
    CMD_ID.MIR_AMR_STATUS.value,
    CMD_ID.EMERGENCY_STOP.value,
    CMD_ID.POINT_CLOUD.value,
]
