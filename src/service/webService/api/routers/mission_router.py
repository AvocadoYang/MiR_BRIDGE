import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel

from src.dtypes import REGISTER_TABLE
from src.logger import logger

from ...handler import CustomSuccessRoute

router = APIRouter(prefix='/mission', route_class=CustomSuccessRoute)


class Pure_Move_Payload(BaseModel):
    amrId: str
    location_uuid: str


@router.post('/pure-move')
async def send_pure_move(request: Request, payload: Pure_Move_Payload):
    try:
        register_table: dict[str, REGISTER_TABLE] = request.state.register_table
        async with httpx.AsyncClient() as client:
            logger.bind(state='[POST]').info('send pure move action')

    except (httpx.HTTPStatusError, Exception) as e:
        print(e)

    return []
