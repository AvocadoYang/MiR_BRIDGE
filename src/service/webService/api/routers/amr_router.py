import httpx
from fastapi import APIRouter
from pydantic import BaseModel

from src.actions import All_Web_Action
from src.logger import logger
from src.types.web import DELETE_AMR_INFO, REGISTER_AMR_INFO, AMRMapResponse, Work_Status

from ...handler import ConflictError, CustomSuccessRoute, NotFoundError
from ...httpx_set import headers
from ...state import AppRequest

router = APIRouter(prefix='/amr', route_class=CustomSuccessRoute)


@router.get('/all_mir_amr', response_model=AMRMapResponse)
async def read_root(request: AppRequest):
    res = {
        serialNum: {
            'amrId': info['amrId'],
            'ip': info['ip'],
        }
        for serialNum, info in request.state.register_table.items()
    }
    logger.bind(state='[GET]').info('get all mir amr')
    return res


@router.post('/create_mir_amr', response_model=REGISTER_AMR_INFO)
async def create_amr(request: AppRequest, create_info: REGISTER_AMR_INFO):
    register_table = request.state.register_table
    output = request.state.output
    if create_info.serialNum in register_table:
        raise ConflictError(resource=create_info.serialNum)
    create_payload = All_Web_Action.ADD_AMR_ACTION(
        mac_address=create_info.serialNum,
        ip=create_info.ip,
        amrId=create_info.amrId,
        is_enable=create_info.is_enable,
    )

    output.on_next(create_payload)
    logger.bind(state='[POST]').info(f'create new amr: {create_info.model_dump_json()}')
    return create_info


@router.put('/update_mir_amr', response_model=REGISTER_AMR_INFO)
async def update_amr(request: AppRequest, update_info: REGISTER_AMR_INFO):
    output = request.state.output
    if update_info.serialNum not in request.state.register_table:
        raise NotFoundError(
            f'can not found mac address {update_info.serialNum} in register table',
        )

    update_payload = All_Web_Action.UPDATE_AMR_ACTION(
        mac_address=update_info.serialNum,
        ip=update_info.ip,
        amrId=update_info.amrId,
        is_enable=update_info.is_enable,
    )

    output.on_next(update_payload)
    pretty_json = update_info.model_dump_json()
    logger.bind(state='[PUT]').info(f'update amr: {pretty_json}')
    return update_info


@router.delete('/delete_mir_amr', response_model=DELETE_AMR_INFO)
async def delete_amr(request: AppRequest, delete_info: DELETE_AMR_INFO):
    output = request.state.output
    if delete_info.serialNum not in request.state.register_table:
        raise NotFoundError(
            f'can not found mac address {delete_info.serialNum} in register table',
        )

    delete_payload = All_Web_Action.DELETE_AMR_ACTION(
        mac_address=delete_info.serialNum, amrId=delete_info.amrId
    )
    output.on_next(delete_payload)
    pretty_json = delete_info.model_dump_json()
    logger.bind(state='[DELETE]').info(f'delete amr: {pretty_json}')
    return delete_info


class State_Payload(BaseModel):
    state_id: int


@router.post('/switch-work-status')
async def switch_work_status(request: AppRequest, work_status: Work_Status):
    register_table = request.state.register_table
    if work_status.serialNum not in register_table:
        raise NotFoundError(
            f'can not found mac address {work_status.serialNum} in register table',
        )
    try:
        amr_info = register_table[work_status.serialNum]
        url = f'http://{amr_info["ip"]}/api/v2.0.0/status'
        async with httpx.AsyncClient() as client:
            payload = State_Payload(state_id=work_status.status)
            await client.put(url=url, json=payload.model_dump(), headers=headers, timeout=3)
    except (httpx.HTTPStatusError, Exception) as e:
        print(e)
    logger.bind(state='[PUT]').info(
        f'switch work state: {"work stop" if work_status.status == 4 else "working"}'
    )
    return []


class Localization(BaseModel):
    amrId: str
    serialNumber: str
    x: float
    y: float
    yaw: float


class Position_Payload(BaseModel):
    x: float
    y: float
    orientation: float


class Localization_Payload(BaseModel):
    position: Position_Payload


@router.post('/localization')
async def localization(request: AppRequest, position: Localization):
    register_table = request.state.register_table
    if position.serialNumber not in register_table:
        raise NotFoundError(
            f'can not found mac address {position.serialNumber} in register table',
        )
    amr = register_table[position.serialNumber]
    url = f'http://{amr["ip"]}/api/v2.0.0/status'
    try:
        async with httpx.AsyncClient() as client:
            Position = Position_Payload(x=position.x, y=position.y, orientation=position.yaw)
            payload = Localization_Payload(position=Position)
            await client.put(url=url, json=payload.model_dump(), headers=headers, timeout=3)

        logger.bind(state='[put]').info(f'localization {position.amrId} successfully')
    except (httpx.HTTPStatusError, Exception) as e:
        print(e)
    return position
