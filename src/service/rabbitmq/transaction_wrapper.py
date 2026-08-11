from typing import List, Literal, Union

from pydantic import BaseModel

from src.types.cmd_id import CMD_ID
from src.types.messages import ALL_CONTROL_TYPE
from src.types.rabbitmq import Error_Info
from src.types.ros import PointField


def send_pose_localization(is_accurate: bool):
    return {"cmd_id": CMD_ID.CHECK_POSITION.value, "isAccurate": is_accurate}


class Send_Pose(BaseModel):
    cmd_id: str = CMD_ID.POSE.value
    x: float
    y: float
    yaw: float


class Send_Point_Cloud(BaseModel):
    cmd_id: str = CMD_ID.POINT_CLOUD.value
    height: int
    width: int
    fields: List[PointField]
    is_bigendian: bool
    point_step: int
    row_step: int
    data: str
    is_dense: bool


class Send_MiR_AMR_STATUE(BaseModel):
    cmd_id: str = CMD_ID.MIR_AMR_STATUS.value
    active_groups_id: str
    active_map_id: str
    status: str


class Read_Status(BaseModel):
    feedback_id: str
    action_status: int
    result_status: int
    result_message: str


class Send_Read_Status(BaseModel):
    cmd_id: str = CMD_ID.READ_STATUS.value
    read: Read_Status


class Send_IO_INFO(BaseModel):
    cmd_id: str = CMD_ID.IO_INFO.value
    io: str


class Send_Ready_To_Joystick_Cmd(BaseModel):
    cmd_id: str = CMD_ID.UPDATE_READY_TO_JOYSTICK_CMD.value
    joystick_available: bool
    status_text: str
    joystick_owner_session_id: str = ""
    unavailable_reason: (
        Literal[
            "joystick owned by others",
            "move joystick to own joystick ownership",
            "turn on manual mode",
            "turn off manual break",
            "press the resume button",
            "robot is in protective stop",
        ]
        | None
    ) = None


def send_error_info(error_info: Error_Info):
    return {"cmd_id": CMD_ID.ERROR_INFO.value, **error_info}


def send_io_info(io: str):
    return {"cmd_id": CMD_ID.IO_INFO.value, "io": io}


def send_current_id(current_id: str):
    return {"cmd_id": CMD_ID.CURRENT_ID.value, "currentId": current_id}


def send_amr_is_registered(is_registered: bool):
    return {"cmd_id": CMD_ID.REGISTERED.value, "isRegistered": is_registered}


def send_amr_has_mission(has_mission: bool):
    return {"cmd_id": CMD_ID.HAS_MISSION.value, "hasMission": has_mission}


def send_cargo_verify(cargo_verity: str):
    return {"cmd_id": CMD_ID.CARGO_VERITY.value, "checkResult": cargo_verity}


def send_cargo_info(cargo_info: str):
    return {"cmd_id": CMD_ID.STACK_INFO.value, "checkResult": cargo_info}


def send_feedback(feedback_json: str):
    return {"cmd_id": CMD_ID.FEEDBACK.value, "feedback": feedback_json}


ALL_REQUEST_MSG_FORMATE = Union[
    Send_Pose,
    Send_MiR_AMR_STATUE,
    Send_Read_Status,
    Send_IO_INFO,
    Send_Point_Cloud,
    Send_Ready_To_Joystick_Cmd,
]

## response fn


class Base_Response_Formate(BaseModel):
    cmd_id: str
    return_code: str = "200"
    id: str
    amrId: str


class Write_Status_Response(Base_Response_Formate):
    mi_mission_id: str
    lastSendGoalId: str
    missionType: str


class Heartbeat_Response(Base_Response_Formate):
    heartbeat: int


ALL_RESPONSE_MSG_FORMATE = Union[
    Base_Response_Formate, Heartbeat_Response, Write_Status_Response
]


def base_transaction_res(action: ALL_CONTROL_TYPE, return_code: str):
    payload = action["payload"]
    return Base_Response_Formate(
        id=payload["id"],
        cmd_id=payload["cmd_id"],
        amrId=payload["amrId"],
        return_code=return_code,
    )
