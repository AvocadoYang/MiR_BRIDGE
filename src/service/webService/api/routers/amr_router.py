from fastapi import APIRouter, Request

from src.logger import logger

from ...handler import ConflictError, CustomSuccessRoute, NotFoundError
from ...type import AMR_INFO, AMRMapResponse

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
