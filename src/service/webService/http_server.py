from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.logger import logger

from .handler import (
    AppException,
    ConflictError,
    CustomSuccessRoute,
    NotFoundError,
    create_error_response,
)
from .type import AMR_INFO, AMRMapResponse


class WebServer:
    def __init__(self, register, register_table):
        from src.dtypes import AMR_INFO_DETAIL

        self.register_table: dict[str, AMR_INFO_DETAIL] = register_table
        self._app = FastAPI(lifespan=register)
        self._app.router.route_class = CustomSuccessRoute
        self.set_error_handler()

    async def run(self):
        self.set_route()

    def set_route(self):

        @self._app.get('/all_mir_amr', response_model=AMRMapResponse)
        async def read_root():
            print(self.register_table)
            res = {
                serialNum: {
                    'amrId': info['amrId'],
                    'ip': info['ip'],
                }
                for serialNum, info in self.register_table.items()
            }
            return res

        @self._app.post('/create_mir_amr', response_model=AMR_INFO)
        async def create_amr(create_info: AMR_INFO):
            if create_info.serialNum in self.register_table:
                raise ConflictError(resource=create_info.serialNum)
            pretty_json = create_info.model_dump_json()
            logger.bind(state='[POST]').info(f'create new amr: {pretty_json}')
            return create_info

        @self._app.put('/update_mir_amr', response_model=AMR_INFO)
        async def update_amr(update_info: AMR_INFO):
            if update_info.serialNum not in self.register_table:
                raise NotFoundError(
                    f'can not found mac address {update_info.serialNum} in register table',
                )
            pretty_json = update_info.model_dump_json()
            logger.bind(state='[PUT]').info(f'update amr: {pretty_json}')
            return update_info

        @self._app.delete('/delete_mir_amr', response_model=AMR_INFO)
        async def delete_amr(update_info: AMR_INFO):
            if update_info.serialNum not in self.register_table:
                raise NotFoundError(
                    f'can not found mac address {update_info.serialNum} in register table',
                )

            pretty_json = update_info.model_dump_json()
            logger.bind(state='[DELETE]').info(f'delete new amr: {pretty_json}')
            return update_info

    def set_error_handler(self):
        @self._app.exception_handler(AppException)
        async def app_exception_handler(request: Request, exc: AppException):
            """Handle all custom application exceptions"""

            # Log the error with context
            logger.bind(state=f'[{request.method}]').warning(
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
                    status_code=exc.status_code, error_code=exc.error_code, message=str(exc.message)
                ),
            )
