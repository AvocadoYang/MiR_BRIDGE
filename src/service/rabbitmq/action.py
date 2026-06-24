from typing import Literal, TypedDict, Union

from src.dtypes import Mission_Payload


class Payload_Base(TypedDict):
    id: str
    amrId: str


###
# All control type from QAMS
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
class Emergency_Stop(Payload_Base):
    cmd_id: Literal['ET']
    payload: str


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


class ALL_CONTROL_TYPE(TypedDict):
    id: str
    sender: str
    serialNum: str
    session: str
    flag: Literal['REQ', 'RES']
    amrId: str

    payload: Union[Update_Pose, Emergency_Stop, Write_Status, Write_Cancel, Pure_Move_Action]
