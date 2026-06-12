from pydantic import BaseModel, RootModel


class AMR_INFO(BaseModel):
    amrId: str
    serialNum: str
    ip: str


class REGISTER_TABLE_RESPONSE(BaseModel):
    register_table: dict[str, AMR_INFO]


class AMRItem(BaseModel):
    amrId: str
    ip: str


class AMRMapResponse(RootModel[dict[str, AMRItem]]):
    pass
