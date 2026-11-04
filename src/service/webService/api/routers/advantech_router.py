from fastapi import APIRouter

from src.actions import All_Web_Action
from src.logger import logger
from src.service.equipment import IoStepError, Wise_Module, run_steps
from src.types.web import (
    REGISTER_ADVANTECH_INFO,
    AdvantechStatusResponse,
    RunStepsRequest,
    WriteOutputRequest,
)

from ...handler import ConflictError, CustomSuccessRoute, ExternalServiceError, NotFoundError
from ...state import AppRequest

router = APIRouter(prefix='/advantech', tags=['advantech'], route_class=CustomSuccessRoute)


def _get_module(request: AppRequest, moduleId: str) -> Wise_Module:
    info = request.state.advantech_table.get(moduleId)
    if info is None:
        raise NotFoundError(message=f"advantech module '{moduleId}' not found.")
    return info['module']


@router.get('/all_module', response_model=AdvantechStatusResponse)
async def read_all_modules(request: AppRequest):
    return [
        {
            'id': info['id'],
            'name': info['name'],
            'ip': info['ip'],
            'connected': info['module'].connected,
            'di': info['module'].di,
            'do': info['module'].do,
            'error': info['module'].error,
        }
        for info in request.state.advantech_table.values()
    ]


@router.post('/create_module', response_model=REGISTER_ADVANTECH_INFO)
async def create_module(request: AppRequest, create_info: REGISTER_ADVANTECH_INFO):
    if create_info.id in request.state.advantech_table:
        raise ConflictError(resource=create_info.id)
    request.state.output.on_next(All_Web_Action.ADD_ADVANTECH_ACTION(**create_info.model_dump()))
    logger.bind(state='[POST]').info(f'create advantech module: {create_info.model_dump_json()}')
    return create_info


@router.put('/update_module', response_model=REGISTER_ADVANTECH_INFO)
async def update_module(request: AppRequest, update_info: REGISTER_ADVANTECH_INFO):
    # a module the bridge missed (e.g. created while it was down) is added instead of rejected
    request.state.output.on_next(All_Web_Action.UPDATE_ADVANTECH_ACTION(**update_info.model_dump()))
    logger.bind(state='[PUT]').info(f'update advantech module: {update_info.model_dump_json()}')
    return update_info


@router.delete('/delete_module/{moduleId}')
async def delete_module(request: AppRequest, moduleId: str):
    if moduleId not in request.state.advantech_table:
        raise NotFoundError(f'can not found advantech module {moduleId}')
    request.state.output.on_next(All_Web_Action.DELETE_ADVANTECH_ACTION(id=moduleId))
    logger.bind(state='[DELETE]').info(f'delete advantech module: {moduleId}')
    return {'message': f'advantech module {moduleId} deleted successfully.'}


@router.post('/write_output')
async def write_output(request: AppRequest, payload: WriteOutputRequest):
    module = _get_module(request, payload.moduleId)
    try:
        await module.write_output(payload.channel, payload.on)
    except IoStepError as e:
        raise ExternalServiceError(service=module.title, message=str(e)) from e
    return {'action': 'write_output', **payload.model_dump()}


@router.post('/run_steps')
async def run_button_steps(request: AppRequest, payload: RunStepsRequest):
    """runs to the end before answering; QAMS waits with a timeout that covers every step"""
    try:
        await run_steps(request.state.advantech_table, payload.steps, title=payload.name)
    except IoStepError as e:
        raise ExternalServiceError(service='advantech', message=str(e)) from e
    return {'action': 'run_steps', 'name': payload.name, 'steps': len(payload.steps)}
