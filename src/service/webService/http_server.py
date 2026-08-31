from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from reactivex import Subject

from src.actions import ALL_Web_Action_Type
from src.logger import logger
from src.types.amr import REGISTER_TABLE
from src.types.equipment import ELEVATOR_TABLE

from .api import api_router
from .handler import (
    AppException,
    create_error_response,
)
from .state import AppRequest


class WebServer:
    def __init__(
        self,
        register,
        register_table: REGISTER_TABLE,
        elevator_table: ELEVATOR_TABLE,
    ):
        self.output = Subject[ALL_Web_Action_Type]()

        self.register_table: REGISTER_TABLE = register_table
        self.elevator_table: ELEVATOR_TABLE = elevator_table
        self._app = FastAPI(lifespan=register)
        self.set_error_handler()

        @self._app.middleware('http')
        async def add_state_middleware(request: AppRequest, call_next):
            request.state.register_table = self.register_table
            request.state.elevator_table = self.elevator_table
            request.state.output = self.output
            response = await call_next(request)
            return response

        self.set_route()

    def set_route(self):
        self._app.include_router(api_router)

    def set_error_handler(self):
        @self._app.exception_handler(AppException)
        async def app_exception_handler(request: Request, exc: AppException):
            """Handle all custom application exceptions"""

            # Log the error with context
            logger.bind(state=f'[{request.method}]').warning(
                'Application error: {} - {}',
                exc.error_code,
                exc.message,
                extra={
                    'error_code': exc.error_code,
                    'status_code': exc.status_code,
                    'path': request.url.path,
                    'method': request.method,
                    'details': exc.details,
                },
            )

            return JSONResponse(
                status_code=exc.status_code,
                content=create_error_response(
                    status_code=exc.status_code, error_code=exc.error_code, message=str(exc.message)
                ),
            )
