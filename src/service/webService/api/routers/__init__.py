from fastapi import APIRouter

from .advantech_router import router as advantech_router
from .amr_router import router as amr_router
from .map_router import router as map_router
from .mission_router import router as mission_router

api_router = APIRouter()

api_router.include_router(amr_router)
api_router.include_router(map_router)
api_router.include_router(mission_router)
api_router.include_router(advantech_router)

__all__ = ['api_router']
