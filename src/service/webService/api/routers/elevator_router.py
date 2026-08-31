from fastapi import APIRouter

from ...handler import (
    CustomSuccessRoute,
)
from ...state import AppRequest

router = APIRouter(prefix='/elevator', route_class=CustomSuccessRoute)


@router.get('/control')
async def control(request: AppRequest):
    elevator = request.state.elevator_table['3000']
    await elevator.ensure_connected()
    info = await elevator.device.get_all_di()
    return info
