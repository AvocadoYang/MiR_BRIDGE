from pydantic import BaseModel, RootModel


class REGISTER_AMR_INFO(BaseModel):
    amrId: str
    serialNum: str
    ip: str
    is_enable: bool


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
