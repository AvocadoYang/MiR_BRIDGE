from fastapi import APIRouter
from pydantic import BaseModel

from src.actions import All_Web_Action
from src.logger import logger
from src.service.equipment import Elevator_Machine, Floor
from src.types.web import REGISTER_ELEVATOR_INFO, ElevatorMapResponse

from ...handler import (
    ConflictError,
    CustomSuccessRoute,
    ExternalServiceError,
    NotFoundError,
)
from ...state import AppRequest

router = APIRouter(prefix='/elevator', tags=['elevator'], route_class=CustomSuccessRoute)


def _get_elevator(request: AppRequest, locationId: str) -> Elevator_Machine:
    info = request.state.elevator_table.get(locationId)
    if info is None or info['elevator'] is None:
        raise NotFoundError(message=f"Elevator with locationId '{locationId}' not found.")
    return info['elevator']


@router.get('/all_elevator', response_model=ElevatorMapResponse)
async def read_all_elevators(request: AppRequest):
    res = {
        f'elevator-{locationId}': {
            'locationId': locationId,
            'ip': info['ip'],
        }
        for locationId, info in request.state.elevator_table.items()
    }
    return res


@router.post('/create_elevator', response_model=REGISTER_ELEVATOR_INFO)
async def create_elevator(request: AppRequest, create_info: REGISTER_ELEVATOR_INFO):
    if create_info.locationId in request.state.elevator_table:
        raise ConflictError(resource=create_info.locationId)

    create_payload = All_Web_Action.ADD_ELEVATOR_ACTION(
        locationId=create_info.locationId,
        ip=create_info.ip,
        areaType=create_info.areaType,
    )
    request.state.output.on_next(create_payload)
    logger.bind(state='[POST]').info(f'create new elevator: {create_info.model_dump_json()}')
    return create_info


@router.put('/update_elevator', response_model=REGISTER_ELEVATOR_INFO)
async def update_elevator(request: AppRequest, update_info: REGISTER_ELEVATOR_INFO):
    if update_info.locationId not in request.state.elevator_table:
        raise NotFoundError(
            f'can not found locationId {update_info.locationId} in elevator table',
        )

    update_payload = All_Web_Action.UPDATE_ELEVATOR_ACTION(
        locationId=update_info.locationId,
        ip=update_info.ip,
        areaType=update_info.areaType,
    )
    request.state.output.on_next(update_payload)
    logger.bind(state='[PUT]').info(f'update elevator: {update_info.model_dump_json()}')
    return update_info


@router.delete('/delete_elevator/{locationId}')
async def delete_elevator(request: AppRequest, locationId: str):
    if locationId not in request.state.elevator_table:
        raise NotFoundError(
            f'can not found locationId {locationId} in elevator table',
        )

    request.state.output.on_next(All_Web_Action.DELETE_ELEVATOR_ACTION(locationId=locationId))
    logger.bind(state='[DELETE]').info(f'delete elevator: {locationId}')
    return {'message': f'Elevator with locationId {locationId} deleted successfully.'}


_PHYSICAL_FLOOR_TO_LEVEL = {
    '3': Floor.F3,
    '5': Floor.F5,
    '6': Floor.F6,
}


class MoveAction(BaseModel):
    floor: str
    locationId: str


@router.post('/move')
async def move_elevator(request: AppRequest, payload: MoveAction):
    elevator = _get_elevator(request, payload.locationId)

    level = _PHYSICAL_FLOOR_TO_LEVEL.get(payload.floor)
    if level is None:
        raise NotFoundError(message=f"Unknown floor '{payload.floor}'")

    try:
        await elevator.go_to(floor=level, background=True)
    except Exception as e:
        raise ExternalServiceError(
            service=f'elevator-{payload.locationId}',
            message=f'Could not reach elevator: {e}',
        ) from e
    return {'action': 'move', **payload.model_dump()}


class ExclusiveRequest(BaseModel):
    exclusive: bool
    locationId: str


@router.post('/exclusive')
async def request_exclusive(request: AppRequest, payload: ExclusiveRequest):
    elevator = _get_elevator(request, payload.locationId)

    await elevator.exclusive_control(exclusive=payload.exclusive, background=True)

    return {'action': 'exclusive', **payload.model_dump()}


@router.get('/status/{locationId}')
async def get_status(request: AppRequest, locationId: str):
    """Return the elevator's most recently polled DI channel states. Values are
    cached from a background poll (every `Elevator_Machine.IO_POLL_INTERVAL`
    seconds), not read live on request."""
    elevator = _get_elevator(request, locationId)
    return {'action': 'status', 'locationId': locationId, 'io_status': elevator.io_status}


@router.post('/cancel_action/{locationId}')
async def cancel(request: AppRequest, locationId: str):
    elevator = _get_elevator(request, locationId)
    await elevator.cancel_action()
    return {'action': 'cancel', 'locationId': locationId}
