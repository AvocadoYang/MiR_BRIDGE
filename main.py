import json
import time
from contextlib import asynccontextmanager

import cowsay
import requests
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, ValidationError

from src.configs import config
from src.dtypes import AMR_INFO, AMR_INFO_DETAIL
from src.helper.helper import format_date
from src.logger import logger
from src.service import Rabbit_client_async, WebServer


class MiR_BRIDGE:
    def __init__(self):
        self.register_table: dict[str, AMR_INFO_DETAIL] = {}
        self.show_sync_register_table_error_log = True

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

    def sync_register_table(self):
        try:
            url = f'http://{config.MISSION_CONTROL_HOST}:{config.MISSION_CONTROL_PORT}/api/amr/mi-serial-amr'
            response = requests.get(url)
            data: list[AMR_INFO] = response.json()

            class AMR_INFO_SCHEMA(BaseModel):
                full_name: str
                ip: str
                serialNum: str

            ## valid formate
            for amr in data:
                amr_info = AMR_INFO_SCHEMA(**amr)
                amr_info.model_dump()

            self.register_table = {
                item['serialNum']: {'full_name': item['full_name'], 'ip': item['ip']}
                for item in data
            }

            tux_text = cowsay.get_output_string(
                'tux',
                f'AMR register info loaded successfully. \n {json.dumps(self.register_table, indent=2, ensure_ascii=False)} \n',
            )
            logger.opt(raw=True).info(tux_text + '\n')

            return True
        except ValidationError as e:
            if self.show_sync_register_table_error_log:
                logger.error(f'validate error : {e.errors()}')
                self.show_sync_register_table_error_log = False
        except Exception as e:
            if self.show_sync_register_table_error_log:
                logger.error(f'sync register table failed: {str(e)}')
                self.show_sync_register_table_error_log = False
        return False


if __name__ == '__main__':
    sync_register_table_success = False
    cow_text = cowsay.get_output_string(
        'cow',
        f'-- {format_date()} -- \n'
        f'start running "amr_core_node"!\n'
        f'config file:\n'
        f'{json.dumps(config.model_dump(), indent=2, ensure_ascii=False)} \n',
    )
    logger.opt(raw=True).info(cow_text + '\n')
    mir_bridge = MiR_BRIDGE()

    while not sync_register_table_success:
        success = mir_bridge.sync_register_table()
        if success:
            sync_register_table_success = True
        else:
            time.sleep(3)

    uvicorn.run(mir_bridge.web_server._app, host='0.0.0.0', port=4008, log_config=None)
