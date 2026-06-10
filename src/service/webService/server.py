from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


class WebServer:
    def __init__(self, register):
        self._app = FastAPI(lifespan=register)

    async def run(self):
        self.set_route()

    def set_route(self):

        @self._app.get('/')
        async def read_root():
            return {'Hello': 'FastAPI'}

        @self._app.exception_handler(StarletteHTTPException)
        async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
            if exc.status_code == 404:
                return JSONResponse(status_code=404, content={'error': 'Custom Not Found'})
            return JSONResponse(status_code=exc.status_code, content={'detail': exc.detail})
