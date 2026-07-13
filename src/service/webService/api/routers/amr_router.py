import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel
from reactivex import Subject

from src.actions import All_Web_Action, ALL_Web_Action_Type
from src.logger import logger
from src.types.amr import REGISTER_TABLE
from src.types.web import DELETE_AMR_INFO, REGISTER_AMR_INFO, AMRMapResponse, Work_Status

from ...handler import ConflictError, CustomSuccessRoute, NotFoundError
from ...httpx_set import headers

router = APIRouter(prefix='/amr', route_class=CustomSuccessRoute)


@router.get('/all_mir_amr', response_model=AMRMapResponse)
async def read_root(request: Request):
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
async def create_amr(request: Request, create_info: REGISTER_AMR_INFO):
    register_table: dict[str, REGISTER_TABLE] = request.state.register_table
    output: Subject[ALL_Web_Action_Type] = request.state.output
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
async def update_amr(request: Request, update_info: REGISTER_AMR_INFO):
    if update_info.serialNum not in request.state.register_table:
        raise NotFoundError(
            f'can not found mac address {update_info.serialNum} in register table',
        )
    pretty_json = update_info.model_dump_json()
    logger.bind(state='[PUT]').info(f'update amr: {pretty_json}')
    return update_info


@router.delete('/delete_mir_amr', response_model=DELETE_AMR_INFO)
async def delete_amr(request: Request, delete_info: DELETE_AMR_INFO):
    output: Subject[ALL_Web_Action_Type] = request.state.output
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
async def switch_work_status(request: Request, work_status: Work_Status):
    register_table: dict[str, REGISTER_TABLE] = request.state.register_table
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
