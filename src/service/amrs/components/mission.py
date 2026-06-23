import asyncio
from typing import List, cast

import httpx
from pydantic import BaseModel
from reactivex import Subject
from reactivex import operators as ops

from src.service.rabbitmq import ALL_CONTROL_TYPE, CMD_ID, Pure_Move_Action, Rabbit_client_async
from src.service.rabbitmq.queues import CONTROL_EX, RES_EX
from src.service.rabbitmq.transaction_wrapper import Read_Status, Send_Read_Status, transaction_res
from src.service.webService.httpx_set import headers

from ..type import AMR_INFO


class Mission_Param(BaseModel):
    input_name: str
    value: str


class Pure_Mission_Payload(BaseModel):
    mission_id: str = 'mirconst-guid-0000-0001-actionlist00'
    parameters: List[Mission_Param]


class Mission:
    def __init__(
        self,
        amr_info: AMR_INFO,
        rabbit_service: Rabbit_client_async,
        receive_request_record: dict[str, str],
        control_transaction_sub_: Subject[ALL_CONTROL_TYPE],
        amr_status_signal: Subject[str],
    ):

        self.amr_info = amr_info
        self.rb = rabbit_service
        self.receive_request_record = receive_request_record
        self.amr_status_signal = amr_status_signal

        control_transaction_sub_.subscribe(self.action_processor)

    def action_processor(self, action: ALL_CONTROL_TYPE):
        payload = action['payload']
        if payload['cmd_id'] == CMD_ID.PURE_MOVE_ACTION.value:
            asyncio.create_task(self.send_pure_move(action=action))

    async def send_pure_move(self, action: ALL_CONTROL_TYPE):

        rq_payload: Pure_Move_Action = cast(Pure_Move_Action, action['payload'])

        param: Mission_Param = Mission_Param(
            input_name='Position', value=rq_payload['location_uuid']
        )
        payload: Pure_Mission_Payload = Pure_Mission_Payload(parameters=[param])
        try:
            async with httpx.AsyncClient() as client:
                url = f'http://{self.amr_info.ip}/api/v2.0.0/mission_queue'
                res = await client.post(
                    url=url, headers=headers, json=payload.model_dump(), timeout=3
                )
                res_json = res.json()
                success = True if 'error_code' not in res_json else False
                await self.rb.res_publish(
                    exchange_name=RES_EX,
                    routing_key=f'qams.{self.amr_info.mac_address}.res.pure_move_action',
                    last_receive_req=self.receive_request_record,
                    mac_address=self.amr_info.mac_address,
                    message=transaction_res(
                        action=action, return_code=('200' if success else '304')
                    ),
                )
                if success:
                    com = await self.wait_mission_complete()
                    if com:
                        read_status: Read_Status = Read_Status(
                            feedback_id=rq_payload['feedback_id'],
                            action_status=3,
                            result_message='',
                            result_status=201,
                        )
                        msg: Send_Read_Status = Send_Read_Status(read=read_status)

                        await self.rb.req_publish(
                            exchange_name=CONTROL_EX,
                            routing_key=f'qams.{self.amr_info.mac_address}.handshake.readStatus',
                            amr_info=self.amr_info,
                            message=msg,
                        )
        except (httpx.HTTPStatusError, Exception) as e:
            print(e)

    async def wait_mission_complete(self):

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        subscription = self.amr_status_signal.pipe(
            ops.pairwise(),
            ops.first(lambda pair: pair[0] == 'Executing' and pair[1] == 'Ready'),
        ).subscribe(lambda _: future.set_result(True))

        try:
            return await future
        finally:
            subscription.dispose()
