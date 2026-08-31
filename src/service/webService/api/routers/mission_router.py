import httpx
from fastapi import APIRouter
from pydantic import BaseModel

from src.logger import logger

from ...handler import CustomSuccessRoute
from ...state import AppRequest

router = APIRouter(prefix='/mission', route_class=CustomSuccessRoute)


class Pure_Move_Payload(BaseModel):
    amrId: str
    location_uuid: str


@router.post('/pure-move')
async def send_pure_move(request: AppRequest, payload: Pure_Move_Payload):
    try:
        register_table = request.state.register_table
        async with httpx.AsyncClient() as client:
            logger.bind(state='[POST]').info('send pure move action')

    except (httpx.HTTPStatusError, Exception) as e:
        print(e)

    return []
