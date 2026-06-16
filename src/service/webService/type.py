from pydantic import BaseModel, RootModel


class AMR_INFO(BaseModel):
    amrId: str
    serialNum: str
    ip: str


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


class REGISTER_TABLE_RESPONSE(BaseModel):
    register_table: dict[str, AMR_INFO]


class AMRItem(BaseModel):
    amrId: str
    ip: str


class AMRMapResponse(RootModel[dict[str, AMRItem]]):
    pass
