from typing import TYPE_CHECKING, TypedDict, Union

if TYPE_CHECKING:
    from src.service.equipment import Elevator_Machine


class ELEVATOR_REGISTER_INFO(TypedDict):
    locationId: str
    ip: str
    areaType: str
    elevator: Union['Elevator_Machine', None]


ELEVATOR_TABLE = dict[str, ELEVATOR_REGISTER_INFO]
