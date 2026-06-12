import asyncio
import json

import httpx
import websockets

from src.logger import logger
from src.service.rabbitmq import Rabbit_client_async
from src.service.webService import headers

from .type import RobotStatus, TFMessage


class AMR:
    def __init__(self, mac_address: str, ip: str, amrId: str, rabbit_service: Rabbit_client_async):
        self.rabbit_service = rabbit_service
        self.mac_address: str = mac_address
        self.ip: str = ip
        self.amrId: str = amrId

        self.got_mir_token = False
        self.mir_token: str = ''

        rabbit_service.rabbit_is_connect.subscribe(self.rabbitmq_connect_handler)

    async def get_MiR_info(self):
        url = f'http://{self.ip}/api/v2.0.0/users/auth'
        while not self.got_mir_token:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(url=url, headers=headers, timeout=2)
                    self.mir_token = response.json()['token']
                    self.got_mir_token = True
                    await self.ros_bridge_connect()
                    return True
            except httpx.HTTPError:
                logger.bind(type=self.amrId).error(
                    'connect failed: did not get mir token，retry after 3s ...',
                )
            except Exception:
                pass
            await asyncio.sleep(3)

    async def ros_bridge_connect(self):
        """
        create websocket connect with ROS Bridge
        """
        if not self.got_mir_token or not self.mir_token:
            return False

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

                    logger.bind(type=self.amrId).info(
                        'ROS Bridge connect successfully, QAMS bridge was connect with amr.'
                    )

                    async for message in websocket:
                        if isinstance(message, bytes):
                            message_str = message.decode('utf-8')
                        else:
                            message_str = message

                        await self._handle_ros_message(message_str)

            except websockets.ConnectionClosed as e:
                logger.bind(type=self.amrId).warning(
                    f'ROS Bridge disconnection ({e}), retry connect after 3s ...'
                )
            except Exception as e:
                logger.bind(type=self.amrId).error(
                    f'ROS Bridge connects failed: {e}, retry connect after 3s ...'
                )

            self.ws = None
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
                # print(f'【{self.amrId} 收到其他 ROS 訊息】: {payload}')

        except json.JSONDecodeError:
            print(f'無法解析的非 JSON 原始訊息: {raw_message}')

    def rabbitmq_connect_handler(self, is_connect: bool):
        print(is_connect)
