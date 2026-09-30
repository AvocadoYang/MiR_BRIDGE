from typing import Literal, Union

from pydantic import BaseModel


class ADD_AMR_ACTION(BaseModel):
    type: Literal['ADD_AMR'] = 'ADD_AMR'
    amrId: str
    mac_address: str
    ip: str
    is_enable: bool


class UPDATE_AMR_ACTION(BaseModel):
    type: Literal['UPDATE_AMR'] = 'UPDATE_AMR'
    amrId: str
    mac_address: str
    ip: str
    is_enable: bool


class DELETE_AMR_ACTION(BaseModel):
    type: Literal['DELETE_AMR'] = 'DELETE_AMR'
    amrId: str
    mac_address: str


class ADD_ELEVATOR_ACTION(BaseModel):
    type: Literal['ADD_ELEVATOR'] = 'ADD_ELEVATOR'
    locationId: str
    ip: str
    areaType: str


class UPDATE_ELEVATOR_ACTION(BaseModel):
    type: Literal['UPDATE_ELEVATOR'] = 'UPDATE_ELEVATOR'
    locationId: str
    ip: str
    areaType: str


class DELETE_ELEVATOR_ACTION(BaseModel):
    type: Literal['DELETE_ELEVATOR'] = 'DELETE_ELEVATOR'
    locationId: str


class ALL_WEB_ACTION:
    ADD_AMR_ACTION = ADD_AMR_ACTION
    UPDATE_AMR_ACTION = UPDATE_AMR_ACTION
    DELETE_AMR_ACTION = DELETE_AMR_ACTION
    ADD_ELEVATOR_ACTION = ADD_ELEVATOR_ACTION
    UPDATE_ELEVATOR_ACTION = UPDATE_ELEVATOR_ACTION
    DELETE_ELEVATOR_ACTION = DELETE_ELEVATOR_ACTION


All_Web_Action = ALL_WEB_ACTION()


ALL_Web_Action_Type = Union[
    ADD_AMR_ACTION,
    UPDATE_AMR_ACTION,
    DELETE_AMR_ACTION,
    ADD_ELEVATOR_ACTION,
    UPDATE_ELEVATOR_ACTION,
    DELETE_ELEVATOR_ACTION,
]
