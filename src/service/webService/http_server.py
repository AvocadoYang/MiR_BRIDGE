from typing import List

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

from src.logger import logger

from .handler import (
    AppException,
    ConflictError,
    CustomSuccessRoute,
    ExternalServiceError,
    NotFoundError,
    ValidationError,
    create_error_response,
)
from .httpx_set import headers
from .type import AMR_INFO, AMRMapResponse, Maps


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
            res = {
                serialNum: {
                    'amrId': info['amrId'],
                    'ip': info['ip'],
                }
                for serialNum, info in self.register_table.items()
            }
            logger.bind(state='[GET]').info('get all mir amr')
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

        @self._app.get('/sync_map', response_model=List[Maps])
        async def async_map():
            res: List[Maps] = []

            class Info(BaseModel):
                url: str
                guid: str
                name: str

            class Map_Info(BaseModel):
                info: List[Info]

            for item in list(self.register_table.values()):
                try:
                    url = f'http://{item["ip"]}/api/v2.0.0/maps'
                    async with httpx.AsyncClient() as client:
                        response = await client.get(url=url, headers=headers, timeout=2)
                        maps = response.json()
                        valid_maps = Map_Info(info=maps)
                        if len(valid_maps.info):

                            class MapDetail(BaseModel):
                                guid: str
                                session_id: str
                                name: str
                                base_map: str
                                resolution: float
                                origin_x: float
                                origin_y: float
                                origin_theta: float
                                positions: str
                                paths: str
                                path_guides: str
                                created_by_id: str
                                created_by: str

                            for map in valid_maps.info:
                                get_map_info_url = f'http://{item["ip"]}/api/v2.0.0/maps/{map.guid}'
                                info_res = await client.get(url=get_map_info_url, headers=headers)
                                map_detail = info_res.json()
                                valid_map_detail = MapDetail(**map_detail)
                                r: Maps = Maps(
                                    guid=valid_map_detail.guid,
                                    session_id=valid_map_detail.session_id,
                                    name=valid_map_detail.name,
                                    base_map=valid_map_detail.base_map,
                                    resolution=valid_map_detail.resolution,
                                    origin_x=valid_map_detail.origin_x,
                                    origin_y=valid_map_detail.origin_y,
                                    origin_theta=valid_map_detail.origin_theta,
                                )
                                res.append(r)
                    logger.bind(state='[GET]').info('return sync maps info')
                    return res

                except PydanticValidationError as e:
                    raise ValidationError(
                        message=f'msg: {e.errors()[0]["msg"]}, input: {e.errors()[0]["input"]}'
                    )
                except (httpx.HTTPStatusError, Exception):
                    raise ExternalServiceError(service=item['ip'])

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
