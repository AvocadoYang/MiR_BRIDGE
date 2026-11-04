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


class ADD_ADVANTECH_ACTION(BaseModel):
    type: Literal['ADD_ADVANTECH'] = 'ADD_ADVANTECH'
    id: str
    name: str
    ip: str


class UPDATE_ADVANTECH_ACTION(BaseModel):
    type: Literal['UPDATE_ADVANTECH'] = 'UPDATE_ADVANTECH'
    id: str
    name: str
    ip: str


class DELETE_ADVANTECH_ACTION(BaseModel):
    type: Literal['DELETE_ADVANTECH'] = 'DELETE_ADVANTECH'
    id: str


class ALL_WEB_ACTION:
    ADD_AMR_ACTION = ADD_AMR_ACTION
    UPDATE_AMR_ACTION = UPDATE_AMR_ACTION
    DELETE_AMR_ACTION = DELETE_AMR_ACTION
    ADD_ADVANTECH_ACTION = ADD_ADVANTECH_ACTION
    UPDATE_ADVANTECH_ACTION = UPDATE_ADVANTECH_ACTION
    DELETE_ADVANTECH_ACTION = DELETE_ADVANTECH_ACTION


All_Web_Action = ALL_WEB_ACTION()


ALL_Web_Action_Type = Union[
    ADD_AMR_ACTION,
    UPDATE_AMR_ACTION,
    DELETE_AMR_ACTION,
    ADD_ADVANTECH_ACTION,
    UPDATE_ADVANTECH_ACTION,
    DELETE_ADVANTECH_ACTION,
]
