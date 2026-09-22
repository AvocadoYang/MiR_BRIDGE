from typing import Literal, TypedDict, Union

from .mission import Mission_Payload


class Payload_Base(TypedDict):
    id: str
    amrId: str


###
# All handshake type from QAMS
###
class Heartbeat(Payload_Base):
    cmd_id: Literal['HB']
    heartbeat: int


class HEARTBEAT(TypedDict):
    id: str
    sender: str
    serialNum: str
    session: str
    flag: Literal['REQ', 'RES']
    amrId: str
    payload: Heartbeat


class Update_Pose(Payload_Base):
    cmd_id: Literal['UM']
    isUpdate: bool


# ----------
class Write_Status(Payload_Base):
    cmd_id: Literal['WS']
    status: Mission_Payload
    actionType: str
    locationId: str


# ----------
class Write_Cancel(Payload_Base):
    cmd_id: Literal['WC']
    feedback_id: str


# ----------
class Pure_Move_Action(Payload_Base):
    cmd_id: Literal['PURE_MOVE_ACTION']
    amrId: str
    serialNum: str
    location_uuid: str
    feedback_id: str


# ----------
class Joystick_Control(Payload_Base):
    cmd_id: Literal['JOYSTICK']
    x: float
    y: float
    web_session_id: str


# ----------


class ALL_HANDSHAKE_TYPE(TypedDict):
    id: str
    sender: str
    serialNum: str
    session: str
    flag: Literal['REQ', 'RES']
    amrId: str

    payload: Union[Update_Pose, Write_Status, Write_Cancel, Pure_Move_Action, Joystick_Control]


###
# All response type from QAMS
###


class Register_Res(TypedDict):
    cmd_id: Literal['RG']
    id: str
    applicant: str
    amrId: str
    qamsSerialNum: str
    return_code: str
    message: str


class Connection_Health_Res(TypedDict):
    cmd_id: Literal['CH']
    id: str
    return_code: str
    message: str


class REGISTER_RESPONSE(TypedDict):
    id: str
    sender: str
    serialNum: str
    session: str
    flag: Literal['RES']
    amrId: str
    payload: Union[Register_Res, Connection_Health_Res]


###
# All io type from QAMS
###


# ----------
class Emergency_Stop(Payload_Base):
    cmd_id: Literal['ET']
    payload: str


class ALL_IO_TYPE(TypedDict):
    id: str
    sender: str
    serialNum: str
    session: str
    flag: Literal['REQ', 'RES']
    amrId: str
    payload: Emergency_Stop
