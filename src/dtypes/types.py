from typing import TypedDict, Union

from src.service import AMR


class AMR_INFO(TypedDict):
    full_name: str
    ip: str
    serialNum: str


class AMR_INFO_DETAIL(TypedDict):
    ip: str
    serialNum: str
    amrId: str
    amr: Union[AMR, None]
