from fastapi import APIRouter

from .amr_router import router as amr_router
from .elevator_router import router as elevator_router
from .map_router import router as map_router
from .mission_router import router as mission_router

api_router = APIRouter()

api_router.include_router(amr_router)
api_router.include_router(map_router)
api_router.include_router(mission_router)
api_router.include_router(elevator_router)

__all__ = ['api_router']
