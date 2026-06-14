import asyncio
import json

import websockets
from reactivex import Subject
from reactivex.subject import BehaviorSubject

from src.logger import logger
from src.service.rabbitmq import ALL_CONTROL_TYPE, CMD_ID, Rabbit_client_async
from src.service.rabbitmq.queues import RES_EX
from src.service.rabbitmq.transaction_wrapper import base_response

from .type import RobotStatus, TFMessage


class Status:
    def __init__(
        self,
        ip: str,
        amrId: str,
        mac_address: str,
        mir_service_connect_status: BehaviorSubject[bool],
        receive_request_record: dict[str, str],
        rabbit_service: Rabbit_client_async,
        control_transaction_sub_: Subject[ALL_CONTROL_TYPE],
    ):
        self.mir_token = ''
        self.ip = ip
        self.mac_address = mac_address
        self.amrId = amrId
        self.receive_request_record = receive_request_record
        self.mir_service_connect_status: BehaviorSubject[bool] = mir_service_connect_status
        self.rb = rabbit_service

        control_transaction_sub_.subscribe(self.action_processor)

    def action_processor(self, action: ALL_CONTROL_TYPE):
        payload = action['payload']
        if payload['cmd_id'] == CMD_ID.UPDATE_MAP.value:
            asyncio.create_task(
                self.rb.res_publish(
                    exchange_name=RES_EX,
                    routing_key=f'qams.{self.mac_address}.res.updateMap',
                    last_receive_req=self.receive_request_record,
                    mac_address=self.mac_address,
                    message=self.__transaction_res(action=action, return_code='200'),
                )
            )

    def __transaction_res(self, action: ALL_CONTROL_TYPE, return_code: str):
        payload = action['payload']
        return base_response(
            {
                'cmd_id': payload['cmd_id'],
                'amrId': payload['amrId'],
                'id': payload['id'],
                'return_code': return_code,
            }
        )

    async def ros_bridge_connect(self, mir_token: str):
        """
        create websocket connect with ROS Bridge
        """
        self.mir_token = mir_token

        url = f'ws://{self.ip}/rosbridge/'
        cookie_header = {'Cookie': f'mir-auth-token={self.mir_token}'}

        while True:
            try:
                async with websockets.connect(
                    url, additional_headers=cookie_header, ping_interval=1.5, ping_timeout=1.5
                ) as websocket:
                    self.ws = websocket

                    topics_to_subscribe = ['/tf', '/robot_status']

                    for topic in topics_to_subscribe:
                        sub_msg = {'op': 'subscribe', 'topic': topic}
                        await websocket.send(json.dumps(sub_msg))

                    logger.bind(title=self.amrId).info(
                        'ROS Bridge connect successfully, QAMS bridge was connect with amr.'
                    )
                    self.mir_service_connect_status.on_next(True)
                    async for message in websocket:
                        if isinstance(message, bytes):
                            message_str = message.decode('utf-8')
                        else:
                            message_str = message

                        await self._handle_ros_message(message_str)

            except websockets.ConnectionClosed as e:
                logger.bind(title=self.amrId).warning(
                    f'ROS Bridge disconnection ({e}), retry connect after 3s ...'
                )
            except Exception as e:
                logger.bind(title=self.amrId).error(
                    f'ROS Bridge connects failed: {e}, retry connect after 3s ...'
                )

            self.ws = None
            self.mir_service_connect_status.on_next(False)
            await asyncio.sleep(3)

    async def _handle_ros_message(self, raw_message: str):
        """
        parse ROS Bridge message to JSON formate
        """
        try:
            payload = json.loads(raw_message)
            topic = payload.get('topic')

            if topic == '/tf':
                pose_msg_data: TFMessage = payload.get('msg')

            if topic == '/robot_status':
                status_msg_data: RobotStatus = payload.get('msg')

            else:
                pass

        except json.JSONDecodeError:
            print(f'parse error: {raw_message}')
