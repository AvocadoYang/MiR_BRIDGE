import json
from contextlib import asynccontextmanager

import cowsay
import uvicorn
from fastapi import FastAPI

from src.configs import config
from src.helper.helper import format_date
from src.logger import logger
from src.service import Rabbit_client_async, WebServer


class MiR_BRIDGE:
    def __init__(self):
        self.rabbitmq: Rabbit_client_async = Rabbit_client_async()
        self.web_server: WebServer = WebServer(self.service_launch)

    @asynccontextmanager
    async def service_launch(self, app: FastAPI):
        success = await self.rabbitmq.connect()
        if not success:
            self.rabbitmq._trigger_reconnect()
        await self.web_server.run()
        logger.info('All service is running')
        try:
            yield
        finally:
            await self.rabbitmq.close()


if __name__ == '__main__':
    cow_text = cowsay.get_output_string(
        'cow',
        f'-- {format_date()} -- \n'
        f'start running "amr_core_node"!\n'
        f'config file:\n'
        f'{json.dumps(config.model_dump(), indent=2, ensure_ascii=False)} \n',
    )
    logger.opt(raw=True).info(cow_text + '\n')
    mir_bridge = MiR_BRIDGE()
    uvicorn.run(mir_bridge.web_server._app, host='0.0.0.0', port=4008, log_config=None)
