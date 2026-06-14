import asyncio
import json
from typing import Tuple

import httpx
import websockets
from aio_pika.abc import AbstractQueue, ConsumerTag
from pydantic import BaseModel, ValidationError
from reactivex import Subject, combine_latest
from reactivex.operators import distinct_until_changed, do_action
from reactivex.subject import BehaviorSubject

from src.configs import config
from src.logger import logger
from src.service.rabbitmq import (
    ALL_CONTROL_TYPE,
    HEARTBEAT,
    Rabbit_client_async,
    dynamicListener_queues,
    get_all_queue_exchange_relationship,
    heartbeatPingQName,
    q2a_amrResponseQName,
    q2a_controlQName,
)
from src.service.webService import headers

from .heartbeat import Heartbeat
from .type import CONNECT_STATUS, RobotStatus, TFMessage


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

        self.receive_request_record: dict[str, str] = {}  ## record the last receive request

        ## subjecter of action
        self.heartbeat_output_: Subject[HEARTBEAT] = Subject()
        self.control_transaction_output_: Subject[ALL_CONTROL_TYPE] = Subject()

        ## all of components
        heartbeat_c = Heartbeat(
            amrId=self.amrId,
            mac_address=self.mac_address,
            receive_request_record=self.receive_request_record,
            rabbit_service=self.rabbit_service,
            heartbeat_sub=self.heartbeat_output_,
        )

        # Connection status tracker.
        # Will connect to QAMS only when both MiR service and RabbitMQ are connected.

        self.connect_status: CONNECT_STATUS = {
            'qams_is_connect': False,
            'rabbitmq_is_connect': False,
            'mir_service_is_connect': False,
        }
        self.qams_connect_status: BehaviorSubject[bool] = BehaviorSubject(False)
        self.rb_connect_status: BehaviorSubject[bool] = BehaviorSubject(False)
        self.mir_service_connect_status: BehaviorSubject[bool] = BehaviorSubject(False)

        combine_latest(
            self.qams_connect_status, self.rb_connect_status, self.mir_service_connect_status
        ).pipe(
            distinct_until_changed(
                lambda connect_list: connect_list,
                lambda pre_list, curr_list: (
                    (pre_list[0] == curr_list[0])
                    and (pre_list[1] == curr_list[1])
                    and (pre_list[2] == curr_list[2])
                ),
            ),
            do_action(lambda connect_list: self._check_and_log_status(connect_list)),
        ).subscribe(on_next=lambda connect_list: self.connect_behavior(connect_list))

        # rabbit_service.rabbit_is_connect.subscribe(self.rabbitmq_connect_handler)
        rabbit_service.rabbit_is_connect.subscribe(
            on_next=lambda is_connect: self.rb_connect_status.on_next(is_connect)
        )

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
                logger.bind(title=self.amrId).error(
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
            logger.bind(title=self.amrId).error(f'request error: {e}')
        except ValidationError as e:
            logger.bind(title=self.amrId).error(f'validate error: {e}')

        logger.bind(title=self.amrId).warning('failed to connect with qams, retry afater 3s...')
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

                    logger.bind(title=self.amrId).info(
                        'ROS Bridge connect successfully, QAMS bridge was connect with amr.'
                    )

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

    # def rabbitmq_connect_handler(self, is_connect: bool):
    #     if is_connect:
    #         asyncio.create_task(self.init_queues_and_bind_with_exchange())
    #         if not self.online:
    #             asyncio.create_task(self.connect_with_qams())
    #     else:
    #         self.online = False
    #         self.queues.clear()

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
            need_consume_queue = dynamicListener_queues(serialNum=self.mac_address)
            for queue_name in need_consume_queue:
                if queue_name == heartbeatPingQName(self.mac_address):
                    await self.rabbit_service.consume_queue(
                        self.queues[queue_name], cb=self.__heartbeat_consumer
                    )
                if queue_name == q2a_controlQName(self.mac_address):
                    await self.rabbit_service.consume_queue(
                        self.queues[queue_name], cb=self.__control_consumer
                    )
                if queue_name == q2a_amrResponseQName(self.mac_address):
                    pass

    def __heartbeat_consumer(self, msg: HEARTBEAT):
        self.receive_request_record[msg['payload']['cmd_id']] = msg['session']
        self.heartbeat_output_.on_next(msg)

    def __control_consumer(self, msg: ALL_CONTROL_TYPE):
        self.receive_request_record[msg['payload']['cmd_id']] = msg['session']

    def _check_and_log_status(self, states: Tuple[bool, bool, bool]):
        """
        connect status logger
        """
        qams_c, rabbit_c, amr_service_c = states
        self.connect_status['qams_is_connect'] = qams_c
        self.connect_status['rabbitmq_is_connect'] = rabbit_c
        self.connect_status['mir_service_is_connect'] = amr_service_c
        qams_r = 'qams: connect ✅' if qams_c else 'qams: disconnect ❌'
        rabbit_r = 'rabbitmq: connect ✅' if rabbit_c else 'rabbitmq: disconnect ❌'
        mir_service_r = 'mir_service: connect ✅' if amr_service_c else 'mir_service: disconnect ❌'
        logger.info(f'service status:  {qams_r} / {rabbit_r} / {mir_service_r}')

    def connect_behavior(self, connect_list: Tuple[bool, bool, bool]):
        print(connect_list, '@@@@@@@@@@@')
        # if False not in connect_list:
        #     self.rb.consume_topic()
        # if (not connect_list[0]) and (False not in connect_list[1:]):
        #     self.network.send(network.try_to_connect_qams())
        # if False in connect_list:
        #     self.rb.stop_consume_queue(dynamicListener)
        #     self.hb.send(heartbeat.Qams_heartbeat_watch_dog(open=False))
