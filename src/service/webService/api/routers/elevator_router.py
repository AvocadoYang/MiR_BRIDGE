from fastapi import APIRouter
from pydantic import BaseModel

from src.logger import logger
from src.service.equipment import Floor
from src.types.web import REGISTER_ELEVATOR_INFO, ElevatorMapResponse

from ...handler import (
    ConflictError,
    CustomSuccessRoute,
    ExternalServiceError,
    NotFoundError,
)
from ...state import AppRequest

router = APIRouter(prefix='/elevator', tags=['elevator'], route_class=CustomSuccessRoute)


@router.get('/all_elevator', response_model=ElevatorMapResponse)
async def read_all_elevators(request: AppRequest):
    res = {
        f'elevator-{locationId}': {
            'locationId': locationId,
            'ip': info.ip,
        }
        for locationId, info in request.state.elevator_table.items()
    }
    return res


@router.post('/create_elevator', response_model=REGISTER_ELEVATOR_INFO)
async def create_elevator(request: AppRequest, create_info: REGISTER_ELEVATOR_INFO):
    if create_info.locationId in request.state.elevator_table:
        raise ConflictError(resource=create_info.locationId)

    # Create a new elevator instance and add it to the elevator table
    from src.service.equipment.elevator import Elevator_Machine

    new_elevator = Elevator_Machine(locationId=create_info.locationId, ip=create_info.ip)
    request.state.elevator_table[create_info.locationId] = new_elevator

    return create_info


@router.put('/update_elevator', response_model=REGISTER_ELEVATOR_INFO)
async def update_elevator(request: AppRequest, update_info: REGISTER_ELEVATOR_INFO):
    if update_info.locationId not in request.state.elevator_table:
        raise NotFoundError(
            f'can not found locationId {update_info.locationId} in elevator table',
        )

    # Update the existing elevator instance
    elevator = request.state.elevator_table[update_info.locationId]
    elevator.ip = update_info.ip

    return update_info


@router.delete('/delete_elevator/{locationId}')
async def delete_elevator(request: AppRequest, locationId: str):
    if locationId not in request.state.elevator_table:
        raise NotFoundError(
            f'can not found locationId {locationId} in elevator table',
        )

    # Remove the elevator instance from the table first so no new request can reach it,
    # then abort whatever it was doing and release its connection/background poll task.
    elevator = request.state.elevator_table.pop(locationId)
    try:
        await elevator.cancel_action()
        await elevator.close()
    except Exception as e:
        logger.bind(title=elevator.id).error(f'failed to cleanly close elevator on delete: {e}')

    return {'message': f'Elevator with locationId {locationId} deleted successfully.'}


_PHYSICAL_FLOOR_TO_LEVEL = {
    '5': Floor.A,
    '6': Floor.B,
}


class MoveAction(BaseModel):
    floor: str
    locationId: str


@router.post('/move')
async def move_elevator(request: AppRequest, payload: MoveAction):
    if payload.locationId not in request.state.elevator_table:
        raise NotFoundError(message=f"Elevator with locationId '{payload.locationId}' not found.")

    level = _PHYSICAL_FLOOR_TO_LEVEL.get(payload.floor)
    if level is None:
        raise NotFoundError(message=f"Unknown floor '{payload.floor}'")

    elevator = request.state.elevator_table[payload.locationId]
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
    if payload.locationId not in request.state.elevator_table:
        raise NotFoundError(message=f"Elevator with locationId '{payload.locationId}' not found.")
    elevator = request.state.elevator_table[payload.locationId]

    await elevator.exclusive_control(exclusive=payload.exclusive, background=True)

    return {'action': 'exclusive', **payload.model_dump()}


@router.get('/status/{locationId}')
async def get_status(request: AppRequest, locationId: str):
    """Return the elevator's most recently polled DI channel states. Values are
    cached from a background poll (every `Elevator_Machine.IO_POLL_INTERVAL`
    seconds), not read live on request."""
    if locationId not in request.state.elevator_table:
        raise NotFoundError(message=f"Elevator with locationId '{locationId}' not found.")
    elevator = request.state.elevator_table[locationId]
    return {'action': 'status', 'locationId': locationId, 'io_status': elevator.io_status}


@router.post('/cancel_action/{locationId}')
async def cancel(request: AppRequest, locationId: str):
    if locationId not in request.state.elevator_table:
        raise NotFoundError(message=f"Elevator with locationId '{locationId}' not found.")
    elevator = request.state.elevator_table[locationId]
    await elevator.cancel_action()
    return {'action': 'cancel', 'locationId': locationId}
