from typing import List, Literal, Optional

from pydantic import BaseModel, Field, RootModel


class REGISTER_AMR_INFO(BaseModel):
    amrId: str
    serialNum: str
    ip: str
    is_enable: bool


class REGISTER_ADVANTECH_INFO(BaseModel):
    id: str
    name: str
    ip: str


class DELETE_AMR_INFO(BaseModel):
    serialNum: str
    amrId: str


class Work_Status(BaseModel):
    amrId: str
    serialNum: str
    status: int


class Maps(BaseModel):
    guid: str
    session_id: str
    name: str
    group_name: str
    base_map: str
    resolution: float
    origin_x: float
    origin_y: float
    origin_theta: float


class ALL_Groups(BaseModel):
    id: str
    name: str


class REGISTER_TABLE_RESPONSE(BaseModel):
    register_table: dict[str, REGISTER_AMR_INFO]


class AMRItem(BaseModel):
    amrId: str
    ip: str


class AMRMapResponse(RootModel[dict[str, AMRItem]]):
    pass


class AdvantechStatusItem(BaseModel):
    id: str
    name: str
    ip: str
    connected: bool
    di: List[bool]
    do: List[bool]
    error: str


class AdvantechStatusResponse(RootModel[List[AdvantechStatusItem]]):
    pass


class WriteOutputRequest(BaseModel):
    moduleId: str
    channel: int = Field(ge=0, le=3)
    on: bool


class IoStep(BaseModel):
    action: Literal['PULSE', 'ON', 'OFF', 'WAIT']
    moduleId: Optional[str] = None
    channel: Optional[int] = Field(default=None, ge=0, le=3)
    ms: Optional[int] = Field(default=None, ge=50, le=60000)


class RunStepsRequest(BaseModel):
    """a custom button pressed on the dashboard: run the steps in order, stop at first failure"""

    name: str = ''
    steps: List[IoStep] = Field(min_length=1, max_length=50)
