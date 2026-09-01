import httpx
from fastapi import APIRouter
from pydantic import BaseModel, Field

from src.logger import logger

from ...handler import CustomSuccessRoute
from ...state import AppRequest

router = APIRouter(prefix='/mission', tags=['mission'], route_class=CustomSuccessRoute)


class Pure_Move_Payload(BaseModel):
    amrId: str = Field(..., description='The full_name/ID of the target AMR.')
    location_uuid: str = Field(..., description='UUID of the destination location on the map.')


@router.post(
    '/pure-move',
    summary='Send a pure-move mission to an AMR',
    description='Dispatches a move-only mission, sending the given AMR to the given location.',
)
async def send_pure_move(request: AppRequest, payload: Pure_Move_Payload):
    try:
        register_table = request.state.register_table
        async with httpx.AsyncClient() as client:
            logger.bind(state='[POST]').info('send pure move action')

    except (httpx.HTTPStatusError, Exception) as e:
        print(e)

    return []
