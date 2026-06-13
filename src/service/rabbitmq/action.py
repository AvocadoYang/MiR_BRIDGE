from typing import List, Literal, TypedDict, Union

from src.dtypes import Mission_Payload


class Base(TypedDict):
    id: str
    sender: str
    serialNum: str
    session: str
    flag: Literal['REQ', 'RES']
    amrId: str


class Payload_Base(TypedDict):
    id: str
    amrId: str


###
# All response types to QAMS
###


class Heartbeat(Payload_Base):
    cmd_id: Literal['HB']
    id: str
    amrId: str
    heartbeat: int


class HEARTBEAT(Base):
    payload: Heartbeat


ALL_RES_TYPE = Union[Heartbeat]


###
# All control type from QAMS
###


class Update_Pose(Payload_Base):
    cmd_id: Literal['UM']
    isUpdate: bool


class UPDATE_POSE(Base):
    payload: Update_Pose


# ----------
class Emergency_Stop(Payload_Base):
    cmd_id: Literal['ET']
    payload: str


class EMERGENCY_STOP(Base):
    payload: Emergency_Stop


# ----------
class Force_Reset(Payload_Base):
    cmd_id: Literal['FR']
    payload: bool


class FORCE_RESET(Base):
    payload: Force_Reset


# ----------
class Has_Cargo(Payload_Base):
    cmd_id: Literal['HC']
    hasCargo: bool


class HAS_CARGO(Base):
    payload: Has_Cargo


# ----------
class PVTP_Switch(Payload_Base):
    cmd_id: Literal['PTVP_SWITCH']
    enable: bool


class PVTP_SWITCH(Base):
    payload: PVTP_Switch


# ----------
class Write_Status(Payload_Base):
    cmd_id: Literal['WS']
    status: Mission_Payload
    actionType: str
    locationId: str


class WRITE_STATUS(Base):
    payload: Write_Status


# ----------


class Write_Cancel(Payload_Base):
    cmd_id: Literal['WC']
    feedback_id: str


class WRITE_CANCEL(Base):
    payload: Write_Cancel


# ----------


class Shortest_Path(Payload_Base):
    cmd_id: Literal['SP']
    shortestPath: List[str]
    rotateFlag: List[int]


class SHORTEST_PATH(Base):
    payload: Shortest_Path


# ----------


class Allow_Path(Payload_Base):
    cmd_id: Literal['AP']
    isAllow: bool
    locationId: str


class ALLOW_PATH(Base):
    payload: Allow_Path


# ----------


class Reroute_Path(Payload_Base):
    cmd_id: Literal['RP']
    reroutePath: List[str]
    rotateFlag: List[int]


class REROUTE_PATH(Base):
    payload: Reroute_Path


ALL_CONTROL_TYPE = Union[
    HAS_CARGO,
    UPDATE_POSE,
    EMERGENCY_STOP,
    FORCE_RESET,
    PVTP_SWITCH,
    WRITE_STATUS,
    WRITE_CANCEL,
    SHORTEST_PATH,
    ALLOW_PATH,
    REROUTE_PATH,
]
