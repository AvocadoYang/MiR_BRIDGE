from fastapi import APIRouter

from ...handler import (
    CustomSuccessRoute,
)
from ...state import AppRequest

router = APIRouter(prefix='/elevator', route_class=CustomSuccessRoute)


@router.get('/control')
async def control(request: AppRequest):
    pass
