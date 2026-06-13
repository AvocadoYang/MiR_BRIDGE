import asyncio
import json

import httpx
import websockets
from aio_pika.abc import AbstractQueue, ConsumerTag
from pydantic import BaseModel, ValidationError
from reactivex import Subject

from src.configs import config
from src.logger import logger
from src.service.rabbitmq import (
    ALL_CONTROL_TYPE,
    HEARTBEAT,
    Rabbit_client_async,
    get_all_queue_exchange_relationship,
)
from src.service.webService import headers

from .type import RobotStatus, TFMessage


class AMR:
    def __init__(
        self,
        mac_address: str,
        ip: str,
        is_enable: bool,
        amrId: str,
        rabbit_service: Rabbit_client_async,
    ):
        self.is_enable = is_enable  ## check is enable in qams
        self.online: bool = False  ## check is connect with qams

        self.rabbit_service = rabbit_service
        self.mac_address: str = mac_address
        self.ip: str = ip
        self.amrId: str = amrId

        self.session: str = ''  ## connect session with qams
        self.mir_token: str = ''  ## mir token for websocket create

        self.got_mir_token = False

        ## own queues
        self.queues: dict[str, AbstractQueue] = {}
        self.consuming_queue: dict[str, ConsumerTag] = {}

        rabbit_service.rabbit_is_connect.subscribe(self.rabbitmq_connect_handler)

        ## subjecter of action
        self.heartbeat_output_: Subject[HEARTBEAT] = Subject()
        self.control_transaction_output_: Subject[ALL_CONTROL_TYPE] = Subject()

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
                    f'connect failed: did not get mir token from {url} ，retry after 3s ...',
                )
            except Exception:
                pass
            await asyncio.sleep(3)

    async def connect_with_qams(self):
        url = f'http://{config.MISSION_CONTROL_HOST}:{config.MISSION_CONTROL_PORT}/api/amr/mir-establish-connection'

        class Schema(BaseModel):
            applicant: str
            amrId: str
            session: str
            success: bool

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url=url, json={'serialNumber': self.mac_address}, timeout=2
                )
                data = Schema(**response.json())
                if data.success:
                    self.session = data.session
                    self.online = True
                    return

        except httpx.HTTPError as e:
            logger.bind(type=self.amrId).error(f'request error: {e}')
        except ValidationError as e:
            logger.bind(type=self.amrId).error(f'validate error: {e}')

        logger.bind(type=self.amrId).warning('failed to connect with qams, retry afater 3s...')
        await asyncio.sleep(3)
        asyncio.create_task(self.connect_with_qams())

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

        except json.JSONDecodeError:
            print(f'parse error: {raw_message}')

    def rabbitmq_connect_handler(self, is_connect: bool):
        if is_connect:
            asyncio.create_task(self.init_queues_and_bind_with_exchange())
            if not self.online:
                asyncio.create_task(self.connect_with_qams())
        else:
            self.online = False
            self.queues.clear()

    async def init_queues_and_bind_with_exchange(self):
        if not len(self.queues):
            queue_pairs = get_all_queue_exchange_relationship(self.mac_address)
            for pair in queue_pairs:
                queue = await self.rabbit_service.create_queue_and_bind(
                    amrId=self.amrId,
                    queue_name=pair['q_name'],
                    exchange=pair['bind_ex'],
                    routing_key=pair['key'],
                    q_options={'durable': True},
                )
                if queue is not None:
                    self.queues[pair['q_name']] = queue

    async def consumeTopic(self):
        pass
