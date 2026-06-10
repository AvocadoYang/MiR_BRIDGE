from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.logger import logger

from .error_handler import AppException, NotFoundError, create_error_response


class WebServer:
    def __init__(self, register):
        self._app = FastAPI(lifespan=register)
        self.set_error_handler()
        # self._app.add_middleware(ErrorHandlingMiddleware)

    async def run(self):
        self.set_route()

    def set_route(self):

        @self._app.get('/')
        async def read_root():
            raise NotFoundError(resource='??', resource_id=123)
            return {'Hello': 'FastAPI'}

    def set_error_handler(self):
        @self._app.exception_handler(AppException)
        async def app_exception_handler(request: Request, exc: AppException):
            """Handle all custom application exceptions"""

            # Log the error with context
            logger.warning(
                f'Application error: {exc.error_code} - {exc.message}',
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
                    status_code=exc.status_code, error_code='123', message=str(exc.details)
                ),
            )

    def create_error_response(
        status_code: int,
        error_code: str,
        message: str,
        details: Dict[str, Any] = None,
        request_id: str = None,
    ) -> Dict[str, Any]:
        """Create a consistent error response structure"""
        response = {
            'success': False,
            'error': {
                'code': error_code,
                'message': message,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            },
        }

        if details:
            response['error']['details'] = details

        if request_id:
            response['error']['request_id'] = request_id

        return response
