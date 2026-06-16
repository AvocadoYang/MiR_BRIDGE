import asyncio
from typing import Tuple

import httpx
from aio_pika.abc import AbstractQueue
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
from .status import Status
from .type import AMR_INFO, CONNECT_STATUS


class AMR:
    def __init__(
        self,
        amrId: str,
        mac_address: str,
        ip: str,
        is_enable: bool,
        rabbit_service: Rabbit_client_async,
    ):
        self.amr_info: AMR_INFO = AMR_INFO(
            amrId=amrId, mac_address=mac_address, ip=ip, is_enable=is_enable
        )

        self.rabbit_service = rabbit_service

        self.mir_token: str = ''  ## mir token for websocket create

        self.show_get_mir_token_error_log = True  ## log switch of mir token getting function
        self.got_mir_token = False  ## loop controler of mir token gettin function

        ## own queues
        self.queues: dict[str, AbstractQueue] = {}
        # self.consuming_queue: dict[str, ConsumerTag] = {}

        self.receive_request_record: dict[str, str] = {}  ## record the last receive request

        ## subjecter of action
        self.heartbeat_input_: Subject[HEARTBEAT] = Subject()
        self.control_transaction_input_: Subject[ALL_CONTROL_TYPE] = Subject()

        # Connection status tracker.
        # will connect to QAMS only when both MiR service and RabbitMQ are connected.
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

        # listen rabbitmq server connect status
        rabbit_service.rabbit_is_connect.subscribe(
            on_next=lambda is_connect: self.rb_connect_status.on_next(is_connect)
        )

        ## all of components
        self.heartbeat_c = Heartbeat(
            amr_info=self.amr_info,
            receive_request_record=self.receive_request_record,
            rabbit_service=self.rabbit_service,
            heartbeat_sub=self.heartbeat_input_,
        )
        self.status_c = Status(
            amr_info=self.amr_info,
            receive_request_record=self.receive_request_record,
            mir_service_connect_status=self.mir_service_connect_status,
            rabbit_service=self.rabbit_service,
            control_transaction_sub_=self.control_transaction_input_,
        )

    async def get_MiR_info(self):
        url = f'http://{self.amr_info.ip}/api/v2.0.0/users/auth'
        while not self.got_mir_token:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(url=url, headers=headers, timeout=2)
                    self.mir_token = response.json()['token']
                    self.got_mir_token = True
                    self.show_get_mir_token_error_log = True
                    await self.status_c.ros_bridge_connect(self.mir_token)
            except (httpx.HTTPError, Exception):
                if self.show_get_mir_token_error_log:
                    logger.bind(title=self.amr_info.amrId).error(
                        f'connect failed: did not get mir token from {url} ，retry after 3s ...',
                    )
                    self.show_get_mir_token_error_log = False
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
                    url=url, json={'serialNumber': self.amr_info.mac_address}, timeout=2
                )
                data = Schema(**response.json())
                if data.success:
                    self.amr_info.session = data.session
                    self.qams_connect_status.on_next(True)
                    return

        except httpx.HTTPError as e:
            logger.bind(title=self.amr_info.amrId).error(f'request error: {e}')
        except ValidationError as e:
            logger.bind(title=self.amr_info.amrId).error(f'validate error: {e}')

        logger.bind(title=self.amr_info.amrId).warning(
            'failed to connect with qams, retry afater 3s...'
        )
        self.qams_connect_status.on_next(False)
        await asyncio.sleep(3)
        asyncio.create_task(self.connect_with_qams())

    async def init_queues_and_bind_with_exchange(self):
        if not len(self.queues):
            queue_pairs = get_all_queue_exchange_relationship(self.amr_info.mac_address)
            for pair in queue_pairs:
                queue = await self.rabbit_service.create_queue_and_bind(
                    amrId=self.amr_info.amrId,
                    queue_name=pair['q_name'],
                    exchange=pair['bind_ex'],
                    routing_key=pair['key'],
                    q_options={'durable': True},
                )
                if queue is not None:
                    self.queues[pair['q_name']] = queue
            need_consume_queue = dynamicListener_queues(serialNum=self.amr_info.mac_address)
            for queue_name in need_consume_queue:
                if queue_name == heartbeatPingQName(self.amr_info.mac_address):
                    await self.rabbit_service.consume_queue(
                        self.queues[queue_name], cb=self.__heartbeat_consumer
                    )
                if queue_name == q2a_controlQName(self.amr_info.mac_address):
                    await self.rabbit_service.consume_queue(
                        self.queues[queue_name], cb=self.__control_consumer
                    )
                if queue_name == q2a_amrResponseQName(self.amr_info.mac_address):
                    pass

    def __heartbeat_consumer(self, msg: HEARTBEAT):
        self.receive_request_record[msg['payload']['cmd_id']] = msg['session']
        self.heartbeat_input_.on_next(msg)

    def __control_consumer(self, msg: ALL_CONTROL_TYPE):
        self.receive_request_record[msg['payload']['cmd_id']] = msg['session']
        self.control_transaction_input_.on_next(msg)

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
        logger.bind(title=self.amr_info.amrId).info(
            f'service status:  {qams_r} / {rabbit_r} / {mir_service_r}'
        )

    ## (qams, rabbitmq, mir_service)
    def connect_behavior(self, connect_list: Tuple[bool, bool, bool]):
        qams_connect, rabbitmq_connect, mir_serive_connect = connect_list

        self.amr_info.is_connect = True if mir_serive_connect else False

        if qams_connect and rabbitmq_connect and mir_serive_connect:
            self.amr_info.online = True
            return

        if not rabbitmq_connect:
            self.queues.clear()
        if rabbitmq_connect and (len(self.queues) == 0):
            asyncio.create_task(self.init_queues_and_bind_with_exchange())
        if not qams_connect and rabbitmq_connect:
            asyncio.create_task(self.connect_with_qams())
        else:
            self.amr_info.online = False
