from typing import Any, Dict, TypedDict, Union

from .action import Heartbeat
from .cmd_id import CMD_ID
from .type import Error_Info, Pose


class Base_Response_Formate(TypedDict):
    cmd_id: str
    return_code: str
    id: str
    amrId: str


class Write_Status_Response(Base_Response_Formate):
    lastSendGoalId: str
    missionType: str


ALL_RESPONSE_MSG_FORMATE = Union[Base_Response_Formate, Heartbeat, Write_Status_Response]


def send_heartbeat_res(heartbeat: int, id: str, amrId: str) -> Heartbeat:
    return {'id': id, 'cmd_id': CMD_ID.HEARTBEAT.value, 'amrId': amrId, 'heartbeat': heartbeat}


def send_pose_localization(is_accurate: bool):
    return {'cmd_id': CMD_ID.CHECK_POSITION.value, 'isAccurate': is_accurate}


def send_pose(pose: Pose):
    return {'cmd_id': CMD_ID.POSE.value, **pose}


def send_error_info(error_info: Error_Info):
    return {'cmd_id': CMD_ID.ERROR_INFO.value, **error_info}


def send_io_info(io: str):
    return {'cmd_id': CMD_ID.IO_INFO.value, 'io': io}


def send_current_id(current_id: str):
    return {'cmd_id': CMD_ID.CURRENT_ID.value, 'currentId': current_id}


def send_amr_is_registered(is_registered: bool):
    return {'cmd_id': CMD_ID.REGISTERED.value, 'isRegistered': is_registered}


def send_amr_has_mission(has_mission: bool):
    return {'cmd_id': CMD_ID.HAS_MISSION.value, 'hasMission': has_mission}


def send_cargo_verify(cargo_verity: str):
    return {'cmd_id': CMD_ID.CARGO_VERITY.value, 'checkResult': cargo_verity}


def send_cargo_info(cargo_info: str):
    return {'cmd_id': CMD_ID.STACK_INFO.value, 'checkResult': cargo_info}


def send_feedback(feedback_json: str):
    return {'cmd_id': CMD_ID.FEEDBACK.value, 'feedback': feedback_json}


def send_mission_result(msg: Dict[str, Dict[str, Any]]):
    return {'cmd_id': CMD_ID.READ_STATUS.value, **msg}


## response fn


def base_response(data: Base_Response_Formate) -> Base_Response_Formate:
    return {**data}


def send_write_status_response(data: Write_Status_Response) -> Write_Status_Response:
    return {**data}
