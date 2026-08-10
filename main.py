import asyncio
import json
import sys
import time
from contextlib import asynccontextmanager
from typing import List

import cowsay
import requests
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, RootModel, ValidationError

from src.actions import ALL_Web_Action_Type
from src.configs import config
from src.helper.helper import format_date
from src.logger import logger
from src.service import AMR, Rabbit_client_async, WebServer
from src.types.amr import REGISTER_TABLE
from src.service.equipment import Elevator_Machine


class MiR_BRIDGE:
    def __init__(self):
        self.register_table: dict[str, REGISTER_TABLE] = {}
        self.show_sync_register_table_error_log = True
        self.rabbitmq: Rabbit_client_async = Rabbit_client_async()
        self.web_server: WebServer = WebServer(self.service_launch, self.register_table)

        self.web_server.output.subscribe(self.web_server_action)

    @asynccontextmanager
    async def service_launch(self, app: FastAPI):
        """
        event loop entry point for driving all service
        """

        asyncio.create_task(self.create_amr_instance())
        success = await self.rabbitmq.connect()
        if not success:
            self.rabbitmq._trigger_reconnect()
        try:
            yield
        finally:
            amrs = [amr_info['amr'] for amr_info in self.register_table.values()]
            await asyncio.gather(
                *(amr.destroy() for amr in amrs if amr is not None),
                return_exceptions=True,
            )
            await self.rabbitmq.close()

    async def create_amr_instance(self):
        """
        create instance of AMR and try to get mir token for websocket
        """

        task = []
        for serialNum, amr_info in self.register_table.items():
            amr = AMR(
                mac_address=serialNum,
                ip=amr_info['ip'],
                amrId=amr_info['amrId'],
                is_enable=amr_info['is_enable'],
                rabbit_service=self.rabbitmq,
            )
            amr_info['amr'] = amr
            task.append(amr.start())
        logger.bind(title='system').info(f"currently obtaining mir's token for {len(task)} amr")

    def sync_register_table(self):
        """
        responsible for fetching AMR registration info; retries until successful.

        """

        class AMR_INFO_SCHEMA(BaseModel):
            full_name: str
            ip: str
            serialNum: str
            is_enable: bool

        class Scheme(RootModel[List[AMR_INFO_SCHEMA]]):
            pass

        try:
            url = f'http://{config.MISSION_CONTROL_HOST}:{config.MISSION_CONTROL_PORT}/api/amr/mi-serial-amr'
            response = requests.get(url)
            register_mi_amr_in_qams = response.json()

            valid_amr = Scheme.model_validate(register_mi_amr_in_qams)

            ## valid formate
            for amr in valid_amr.root:
                self.register_table[amr.serialNum] = {
                    'amrId': amr.full_name,
                    'ip': amr.ip,
                    'serialNum': amr.serialNum,
                    'is_enable': amr.is_enable,
                    'amr': None,
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

    def web_server_action(self, action: ALL_Web_Action_Type):
        match action.type:
            case 'ADD_AMR':
                amr = AMR(
                    mac_address=action.mac_address,
                    ip=action.ip,
                    amrId=action.amrId,
                    is_enable=action.is_enable,
                    rabbit_service=self.rabbitmq,
                )
                self.register_table[action.mac_address] = {
                    'amrId': action.amrId,
                    'ip': action.ip,
                    'serialNum': action.mac_address,
                    'is_enable': action.is_enable,
                    'amr': amr,
                }
                amr.start()
                return
            case 'DELETE_AMR':
                amr = self.register_table[action.mac_address]['amr']
                if amr is not None:
                    asyncio.create_task(amr.destroy())
                    del self.register_table[action.mac_address]
                    logger.bind(title=action.amrId).info('destroy amr instant in system')


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
    ele = Elevator_Machine(ip='10.0.0.1')

    mir_bridge = MiR_BRIDGE()

    try:
        while not sync_register_table_success:
            success = mir_bridge.sync_register_table()
            if success:
                sync_register_table_success = True
            else:
                time.sleep(3)
    except KeyboardInterrupt:
        logger.info('ctrl + c to close service')
        sys.exit(1)
    except Exception:
        pass

    try:
        uvicorn.run(mir_bridge.web_server._app, host='0.0.0.0', port=4008, log_config=None)
    except (KeyboardInterrupt, asyncio.CancelledError):
        # a second Ctrl+C during graceful shutdown interrupts uvicorn's lifespan
        # wait and surfaces as a raw CancelledError/KeyboardInterrupt traceback
        logger.info('service shutdown by user (ctrl+c)')
