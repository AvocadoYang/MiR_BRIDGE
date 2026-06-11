from typing import TypedDict

from src.service import AMR


class AMR_INFO(TypedDict):
    full_name: str
    ip: str
    serialNum: str


class AMR_INFO_DETAIL(TypedDict):
    ip: str
    serialNum: str
    amrId: str
    amr: AMR
