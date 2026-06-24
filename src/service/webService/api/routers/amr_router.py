import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel

from src.dtypes import REGISTER_TABLE
from src.logger import logger

from ...handler import ConflictError, CustomSuccessRoute, NotFoundError
from ...httpx_set import headers
from ...type import AMR_INFO, AMRMapResponse, Work_Status

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


@router.post('/create_mir_amr', response_model=AMR_INFO)
async def create_amr(request: Request, create_info: AMR_INFO):
    if create_info.serialNum in request.state.register_table:
        raise ConflictError(resource=create_info.serialNum)
    logger.bind(state='[POST]').info(f'create new amr: {create_info.model_dump_json()}')
    return create_info


@router.put('/update_mir_amr', response_model=AMR_INFO)
async def update_amr(request: Request, update_info: AMR_INFO):
    if update_info.serialNum not in request.state.register_table:
        raise NotFoundError(
            f'can not found mac address {update_info.serialNum} in register table',
        )
    pretty_json = update_info.model_dump_json()
    logger.bind(state='[PUT]').info(f'update amr: {pretty_json}')
    return update_info


@router.delete('/delete_mir_amr', response_model=AMR_INFO)
async def delete_amr(request: Request, update_info: AMR_INFO):
    if update_info.serialNum not in request.state.register_table:
        raise NotFoundError(
            f'can not found mac address {update_info.serialNum} in register table',
        )

    pretty_json = update_info.model_dump_json()
    logger.bind(state='[DELETE]').info(f'delete new amr: {pretty_json}')
    return update_info


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
