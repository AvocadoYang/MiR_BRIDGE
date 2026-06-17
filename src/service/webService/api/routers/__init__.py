from fastapi import APIRouter

from .amr_router import router as amr_router
from .map_router import router as map_router

api_router = APIRouter()

api_router.include_router(amr_router)
api_router.include_router(map_router)

__all__ = ['api_router']
