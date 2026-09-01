from fastapi import APIRouter
from pydantic import BaseModel

from src.service.equipment import Floor

from ...handler import (
    CustomSuccessRoute,
    ExternalServiceError,
    NotFoundError,
)
from ...state import AppRequest

router = APIRouter(prefix='/elevator', tags=['elevator'], route_class=CustomSuccessRoute)

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


@router.get('/control')
async def control(request: AppRequest):
    elevator = request.state.elevator_table['3000']
    info = await elevator.go_to(floor=Floor.A)
    return info


@router.get('/cancel_action')
async def cancel(request: AppRequest):
    elevator = request.state.elevator_table['3000']
    await elevator.cancel_action()
    return {'status': 'cancelled'}
