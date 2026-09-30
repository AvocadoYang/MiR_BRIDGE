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
from src.service.equipment import Elevator_Machine
from src.types.amr import REGISTER_TABLE
from src.types.equipment import ELEVATOR_TABLE


class MiR_BRIDGE:
    def __init__(self):
        self.register_table: REGISTER_TABLE = {}
        self.elevator_table: ELEVATOR_TABLE = {}
        self.show_sync_register_table_error_log = True
        self.rabbitmq: Rabbit_client_async = Rabbit_client_async(self.register_table)
        self.web_server: WebServer = WebServer(
            self.service_launch, self.register_table, self.elevator_table
        )

        self.web_server.output.subscribe(self.web_server_action)

    @asynccontextmanager
    async def service_launch(self, app: FastAPI):
        """
        event loop entry point for driving all service
        """

        asyncio.create_task(self.create_amr_instance())
        asyncio.create_task(self.create_elevator_instance())
        success = await self.rabbitmq.connect()
        if not success:
            self.rabbitmq._trigger_reconnect()
        try:
            yield
        except asyncio.CancelledError:
            # force-exit (double SIGINT) skips lifespan.shutdown, then asyncio.run cancels
            # this task; swallow it so starlette reports shutdown.complete, not a traceback
            logger.info('lifespan cancelled during shutdown')
        finally:
            amrs = [amr_info['amr'] for amr_info in self.register_table.values()]
            await asyncio.gather(
                *(amr.destroy() for amr in amrs if amr is not None),
                *(
                    info['elevator'].close()
                    for info in self.elevator_table.values()
                    if info['elevator'] is not None
                ),
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

    async def create_elevator_instance(self):
        """
        create instance of Elevator from elevator_table and start polling its IO
        """

        for locationId, elevator_info in self.elevator_table.items():
            elevator = Elevator_Machine(
                locationId=locationId,
                ip=elevator_info['ip'],
                rabbit_service=self.rabbitmq,
            )
            elevator_info['elevator'] = elevator
            elevator.start_io_polling()
            logger.bind(title=locationId).info('start polling IO...')

    def add_elevator_instance(self, locationId: str, ip: str, areaType: str):
        elevator = Elevator_Machine(locationId=locationId, ip=ip, rabbit_service=self.rabbitmq)
        self.elevator_table[locationId] = {
            'locationId': locationId,
            'ip': ip,
            'areaType': areaType,
            'elevator': elevator,
        }
        elevator.start_io_polling()
        logger.bind(title=locationId).info('start polling IO...')

    async def close_elevator_instance(self, elevator: Elevator_Machine):
        try:
            await elevator.cancel_action()
            await elevator.close()
        except Exception as e:
            logger.bind(title=elevator.id).error(f'failed to cleanly close elevator: {e}')

    async def replace_elevator_instance(self, locationId: str, ip: str, areaType: str):
        """
        the device client is bound to the ip at construction, so an ip change needs a new instance
        """

        elevator_info = self.elevator_table.get(locationId)
        if elevator_info is None:
            return

        old_elevator = elevator_info['elevator']
        if old_elevator is not None:
            await self.close_elevator_instance(old_elevator)
        self.add_elevator_instance(locationId=locationId, ip=ip, areaType=areaType)
        logger.bind(title=locationId).info(f'elevator instance rebuilt, now pointing at {ip}')

    async def remove_elevator_instance(self, locationId: str):
        elevator_info = self.elevator_table.pop(locationId, None)
        if elevator_info is None:
            return
        if elevator_info['elevator'] is not None:
            await self.close_elevator_instance(elevator_info['elevator'])
        logger.bind(title=locationId).info('destroy elevator instant in system')

    async def replace_amr_instance(self, mac_address: str, ip: str, amrId: str, is_enable: bool):

        amr_info = self.register_table.get(mac_address)
        if amr_info is None:
            return

        if (
            amr_info['amr'] is not None
            and amr_info['ip'] == ip
            and amr_info['amrId'] == amrId
            and amr_info['is_enable'] == is_enable
        ):
            logger.bind(title=amrId).info('registration unchanged, keep amr instance')
            return

        old_amr = amr_info['amr']
        if old_amr is not None:
            await old_amr.destroy()

        amr = AMR(
            mac_address=mac_address,
            ip=ip,
            amrId=amrId,
            is_enable=is_enable,
            rabbit_service=self.rabbitmq,
        )
        self.register_table[mac_address] = {
            'amrId': amrId,
            'ip': ip,
            'serialNum': mac_address,
            'is_enable': is_enable,
            'amr': amr,
        }
        amr.start()
        logger.bind(title=amrId).info(f'amr instance rebuilt, now pointing at {ip}')

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

        class Elevator(BaseModel):
            locationId: str
            ip: str
            areaType: str

        class ElevatorSchema(RootModel[List[Elevator]]):
            pass

        try:
            amr_url = f'http://{config.MISSION_CONTROL_HOST}:{config.MISSION_CONTROL_PORT}/api/amr/mi-serial-amr'
            response = requests.get(amr_url)
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

            elevator_url = f'http://{config.MISSION_CONTROL_HOST}:{config.MISSION_CONTROL_PORT}/api/map/resource?data=locations'
            ele_response = requests.get(elevator_url)
            register_elevator_in_qams = ele_response.json()
            locations = ElevatorSchema.model_validate(register_elevator_in_qams)

            for location in locations.root:
                if (
                    location.areaType != 'MIR_VL_MARKER'
                    and location.areaType != 'MIR_STRIPE_MARKER'
                ) or location.ip == 'none':
                    continue
                self.elevator_table[location.locationId] = {
                    'locationId': location.locationId,
                    'ip': location.ip,
                    'areaType': location.areaType,
                    'elevator': None,
                }

            tux_text = cowsay.get_output_string('tux', 'register info loaded successfully.')
            logger.opt(raw=True).info(tux_text + '\n' + self._format_register_summary() + '\n')

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

    def _format_register_summary(self) -> str:
        """
        render register_table and elevator_table as plain-text tables for terminal output
        """

        def render(title: str, headers: list[str], rows: list[list[str]]) -> str:
            widths = [max(len(str(cell)) for cell in col) for col in zip(headers, *rows)]

            def line(cells: list[str]) -> str:
                return '  '.join(str(c).ljust(w) for c, w in zip(cells, widths)).rstrip()

            body = [line(row) for row in rows] or ['(none)']
            divider = '-' * (sum(widths) + 2 * (len(widths) - 1))
            return '\n'.join([f'[{title}] {len(rows)} registered', line(headers), divider, *body])

        amr_rows = [
            [f'[ {info["amrId"]} ]', info['ip'], info['serialNum'], str(info['is_enable'])]
            for info in self.register_table.values()
        ]
        elevator_rows = [
            [f'[ {info["locationId"]} ]', info['ip'], info['areaType']]
            for info in self.elevator_table.values()
        ]
        return '\n\n'.join(
            [
                render('AMR', ['amrId', 'ip', 'serialNum', 'is_enable'], amr_rows),
                render('Elevator', ['locationId', 'ip', 'areaType'], elevator_rows),
            ]
        )

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
            case 'UPDATE_AMR':
                asyncio.create_task(
                    self.replace_amr_instance(
                        mac_address=action.mac_address,
                        ip=action.ip,
                        amrId=action.amrId,
                        is_enable=action.is_enable,
                    )
                )
                return
            case 'DELETE_AMR':
                amr = self.register_table[action.mac_address]['amr']
                if amr is not None:
                    asyncio.create_task(amr.destroy())
                    del self.register_table[action.mac_address]
                    logger.bind(title=action.amrId).info('destroy amr instant in system')
                return
            case 'ADD_ELEVATOR':
                self.add_elevator_instance(
                    locationId=action.locationId, ip=action.ip, areaType=action.areaType
                )
                return
            case 'UPDATE_ELEVATOR':
                asyncio.create_task(
                    self.replace_elevator_instance(
                        locationId=action.locationId, ip=action.ip, areaType=action.areaType
                    )
                )
                return
            case 'DELETE_ELEVATOR':
                asyncio.create_task(self.remove_elevator_instance(action.locationId))
                return


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
