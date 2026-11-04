from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from src.service.equipment import Wise_Module


class ADVANTECH_REGISTER_INFO(TypedDict):
    """one row of QAMS advantechWish: a WISE-4000 module the bridge keeps a session with"""

    id: str
    name: str
    ip: str
    module: 'Wise_Module'


ADVANTECH_TABLE = dict[str, ADVANTECH_REGISTER_INFO]
