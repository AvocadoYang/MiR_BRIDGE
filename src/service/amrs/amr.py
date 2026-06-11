import asyncio
import json

import httpx
import websockets

from src.logger import logger
from src.service.webService import headers


class AMR:
    def __init__(self, mac_address, ip, amrId):
        self.mac_address: str = mac_address
        self.ip: str = ip
        self.amrId: str = amrId

        self.got_mir_token = False
        self.mir_token: str = ''

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
                async with websockets.connect(url, additional_headers=cookie_header) as websocket:
                    self.ws = websocket
                    logger.bind(type=self.amrId).info('ROS Bridge connect successfully')

                    sub_robot = {'op': 'subscribe', 'topic': '/robot_pose'}
                    await websocket.send(json.dumps(sub_robot))

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

            if topic == '/robot_pose':
                msg_data = payload.get('msg')  # ROS Bridge 的資料通常包在 'msg' 欄位裡
                # print(f'【{self.amrId} 座標更新】: {msg_data}')
            else:
                pass
                # print(f'【{self.amrId} 收到其他 ROS 訊息】: {payload}')

        except json.JSONDecodeError:
            print(f'無法解析的非 JSON 原始訊息: {raw_message}')
