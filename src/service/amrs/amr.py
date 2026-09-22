import asyncio
import contextlib
import uuid
from pathlib import Path
from typing import List, Tuple, Union

import httpx
from aio_pika.abc import AbstractChannel, AbstractQueue, ConsumerTag
from pydantic import BaseModel, RootModel, ValidationError
from reactivex import Subject, combine_latest
from reactivex.abc import DisposableBase
from reactivex.operators import distinct_until_changed, do_action
from reactivex.subject import BehaviorSubject

from src.configs import config
from src.logger import logger
from src.service.rabbitmq import (
    CMD_ID,
    Rabbit_client_async,
    dynamicListener_queues,
    fixListener_queues,
    get_all_queue_exchange_relationship,
    heartbeatPingQName,
    q2a_handshakeQName,
    q2a_ioQName,
    q2a_registerResponseQName,
    q2a_ResponseQName,
)
from src.service.rabbitmq.queues import HANDSHAKE_EX, a2q_registerReqKey
from src.service.rabbitmq.transaction_wrapper import Send_Register_Request
from src.service.webService import headers
from src.types import (
    ALL_HANDSHAKE_TYPE,
    ALL_IO_TYPE,
    HEARTBEAT,
    PUBLISH_OPTIONS,
    REGISTER_RESPONSE,
    REGISTER_RETURN_CODE,
    ReturnCode,
)
from src.types.amr import AMR_INFO, CONNECT_STATUS
from src.types.map import Footprint, PeripheralType

from .components import Heartbeat, Mission, Status


class AMR:
    def __init__(
        self,
        amrId: str,
        mac_address: str,
        ip: str,
        is_enable: bool,
        rabbit_service: Rabbit_client_async,
    ):
        self.start_destroy_process = False
        self.map_resource_is_init: bool = False
        self.mir_info_task: Union[asyncio.Task, None] = None

        self.amr_info: AMR_INFO = AMR_INFO(
            amrId=amrId, mac_address=mac_address, ip=ip, is_enable=is_enable
        )

        self.rabbit_service = rabbit_service

        self.mir_token: str = ''  ## mir token for websocket create
        self.user_uuid: str = ''

        self.show_get_mir_token_error_log = True  ## log switch of mir token getting function
        self.show_qams_connect_error_log = True
        self.got_mir_token = False  ## loop controler of mir token gettin function

        ## qams register (RG) transaction state
        self._qams_connect_in_progress: bool = False
        self._register_request_id: Union[str, None] = None
        self._register_response_future: Union[asyncio.Future, None] = None

        ## own channel and queues
        self.channel: Union[AbstractChannel, None] = None
        self._channel_setup_in_progress: bool = False
        self.queues: dict[str, AbstractQueue] = {}
        self.consuming_queue: dict[str, ConsumerTag] = {}

        self.receive_request_record: dict[str, str] = {}  ## record the last receive request

        ## subjecter of action
        self.heartbeat_input_: Subject[HEARTBEAT] = Subject()
        self.control_transaction_input_: Subject[ALL_HANDSHAKE_TYPE] = Subject()
        self.io_transaction_input_: Subject[ALL_IO_TYPE] = Subject()

        # Connection status tracker.
        # will connect to QAMS only when both MiR service and RabbitMQ are connected.
        self.connect_status: CONNECT_STATUS = {
            'qams_is_connect': False,
            'rabbitmq_is_connect': False,
            'mir_service_is_connect': False,
        }
        self.qams_connect_status: BehaviorSubject[bool] = BehaviorSubject(False)
        self.rb_connect_status: BehaviorSubject[bool] = BehaviorSubject(
            False if self.rabbit_service.connection is None else True
        )
        self.mir_service_connect_status: BehaviorSubject[bool] = BehaviorSubject(False)

        # listen rabbitmq server connect status
        rabbit_service.rabbit_is_connect.subscribe(
            on_next=lambda is_connect: self.rb_connect_status.on_next(is_connect)
        )

        ## all of components

        ## heartbeat component
        self.heartbeat_c = Heartbeat(
            amr_info=self.amr_info,
            receive_request_record=self.receive_request_record,
            rabbit_service=self.rabbit_service,
            heartbeat_sub=self.heartbeat_input_,
        )

        ## status component
        self.status_c = Status(
            amr_info=self.amr_info,
            receive_request_record=self.receive_request_record,
            mir_service_connect_status=self.mir_service_connect_status,
            rabbit_service=self.rabbit_service,
            control_transaction_sub_=self.control_transaction_input_,
            io_transaction_sub_=self.io_transaction_input_,
        )

        ## mission component
        self.mission_c = Mission(
            amr_info=self.amr_info,
            receive_request_record=self.receive_request_record,
            rabbit_service=self.rabbit_service,
            control_transaction_sub_=self.control_transaction_input_,
            amr_status_signal=self.status_c.amr_status_signal,
        )

        self.subs: List[DisposableBase] = [
            combine_latest(
                self.qams_connect_status,
                self.rb_connect_status,
                self.mir_service_connect_status,
            )
            .pipe(
                distinct_until_changed(
                    lambda connect_list: connect_list,
                    lambda pre_list, curr_list: (
                        (pre_list[0] == curr_list[0])
                        and (pre_list[1] == curr_list[1])
                        and (pre_list[2] == curr_list[2])
                    ),
                ),
                do_action(lambda connect_list: self._check_and_log_status(connect_list)),
            )
            .subscribe(on_next=lambda connect_list: self.connect_behavior(connect_list)),
            self.heartbeat_c.qams_timeout_signal.subscribe(
                lambda action: self.qams_connect_status.on_next(False)
            ),
            # own rabbitmq channel lifecycle: tied directly to the rabbitmq connect signal,
            # independent of qams/mir_service status
            self.rb_connect_status.pipe(distinct_until_changed()).subscribe(
                on_next=self._handle_rb_connect_change
            ),
            self.qams_connect_status.pipe(distinct_until_changed()).subscribe(
                on_next=self._handle_qams_connect_change
            ),
        ]

    def start(self) -> asyncio.Task:
        """
        launch the background task that obtains the mir token; tracked so it can be cancelled on destroy()
        """

        self.mir_info_task = asyncio.create_task(self.get_MiR_info())
        return self.mir_info_task

    async def get_MiR_info(self):
        class InfoSchema(BaseModel):
            user_id: str
            ip: str
            login_time: str
            expiration_time: str
            token: str
            allowed_methods: Union[str, None]

        url = f'http://{self.amr_info.ip}/api/v2.0.0/users/auth'
        while not self.start_destroy_process:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(url=url, headers=headers, timeout=2)
                    valid_data = InfoSchema(**response.json())
                    self.mir_token = valid_data.token

                    self.user_uuid = valid_data.user_id
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
        """
        Entry point for a fresh QAMS (re)connection attempt over the RG (register) MQ
        transaction. No-op if an attempt (including its internal retries) is already
        running, so callers can invoke it freely without spawning parallel retry loops.
        """
        if self._qams_connect_in_progress:
            return
        self._qams_connect_in_progress = True
        await self._attempt_connect_with_qams()

    async def _attempt_connect_with_qams(self):
        class RegisterResponsePayload(BaseModel):
            cmd_id: str
            id: str
            applicant: str = ''
            amrId: str
            qamsSerialNum: str = ''
            return_code: str
            message: str = ''

        request_id = str(uuid.uuid4())
        response_future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._register_request_id = request_id
        self._register_response_future = response_future

        try:
            options = PUBLISH_OPTIONS()
            options.expiration = 3
            await self.rabbit_service.req_publish(
                exchange_name=HANDSHAKE_EX,
                routing_key=a2q_registerReqKey(self.amr_info.mac_address),
                amr_info=self.amr_info,
                message=Send_Register_Request(serialNumber=self.amr_info.mac_address),
                id=request_id,
                options=options,
            )

            response: REGISTER_RESPONSE = await asyncio.wait_for(response_future, timeout=5)
            if response is None:
                raise RuntimeError('receive unexpected result')
            payload = RegisterResponsePayload.model_validate(response['payload'])

            if (
                payload.return_code not in REGISTER_RETURN_CODE
                or payload.return_code == ReturnCode.NOT_IN_SYSTEM_LOGIN_ERROR
                or payload.return_code == ReturnCode.FORMAT_ERROR_LOGIN_ERROR
            ):
                self.qams_connect_status.on_next(False)
                raise RuntimeError(
                    f'register rejected by QAMS: return_code={payload.return_code}, '
                    f'message={payload.message}'
                )

            self.amr_info.session = response['session']
            if not self.map_resource_is_init:
                await self.set_amr_resource()
            self.qams_connect_status.on_next(True)
            self.show_qams_connect_error_log = True
            self._qams_connect_in_progress = False
            return

        except asyncio.TimeoutError:
            if self.show_qams_connect_error_log:
                logger.bind(title=self.amr_info.amrId).error(
                    'QAMS register response timeout, retry after 3s...'
                )
                self.show_qams_connect_error_log = False
        except ValidationError as e:
            if self.show_qams_connect_error_log:
                logger.bind(title=self.amr_info.amrId).error(
                    f'QAMS register validate error: {e}, retry after 3s...'
                )
                self.show_qams_connect_error_log = False
        except Exception as e:
            if self.show_qams_connect_error_log:
                logger.bind(title=self.amr_info.amrId).error(
                    f'QAMS register error: {e}, retry after 3s...'
                )
                self.show_qams_connect_error_log = False
        finally:
            self._register_request_id = None
            self._register_response_future = None

        self.qams_connect_status.on_next(False)
        self._qams_connect_in_progress = False
        if self.start_destroy_process:
            return
        await asyncio.sleep(3)
        asyncio.create_task(self.connect_with_qams())

    def _handle_rb_connect_change(self, is_connect: bool) -> None:
        asyncio.create_task(self._on_rabbitmq_connect_change(is_connect))

    async def _on_rabbitmq_connect_change(self, is_connect: bool):
        if is_connect:
            await self._ensure_channel()
        else:
            await self._teardown_channel()

    def _handle_qams_connect_change(self, is_connect: bool) -> None:
        if not is_connect:
            asyncio.create_task(self._stop_dynamic_consumers())

    async def _stop_dynamic_consumers(self):
        """
        cancel the handshake/response queue consumers as soon as qams is considered
        disconnected (e.g. heartbeat timeout), instead of silently keeping on answering
        QAMS on a session we've already declared dead. consume_dynamic_queue()
        re-subscribes once qams reconnects.
        """
        for queue_name, tag in list(self.consuming_queue.items()):
            queue = self.queues.get(queue_name)
            if queue is not None:
                await self.rabbit_service.stop_consume_queue(
                    queue=queue, consumer_tag=tag, amrId=self.amr_info.amrId
                )
            self.consuming_queue.pop(queue_name, None)

    async def _ensure_channel(self):
        """
        create (once) this AMR's own rabbitmq channel and bind its queues.
        guarded so a burst of rabbitmq-connect events never creates it twice;
        `Rabbit_client_async.get_channel` itself is also serialized per mac_address
        as a second line of defense against concurrent callers (e.g. a lazy
        publish racing this setup).
        """
        if self._channel_setup_in_progress:
            return
        if self.channel is not None and not self.channel.is_closed:
            return
        self._channel_setup_in_progress = True
        try:
            channel = await self.rabbit_service.get_channel(self.amr_info.mac_address)
            if channel is None:
                return
            self.channel = channel
            await self._bind_queues(channel)
            if not self.qams_connect_status.value and self.mir_service_connect_status.value:
                asyncio.create_task(self.connect_with_qams())
        finally:
            self._channel_setup_in_progress = False

    async def _teardown_channel(self):
        """
        release this AMR's own channel and forget its queues; called whenever
        rabbitmq disconnects, and also on AMR destroy()
        """
        self.queues.clear()
        self.consuming_queue.clear()
        if self.channel is not None:
            await self.rabbit_service.close_channel(self.amr_info.mac_address)
            self.channel = None

    async def _bind_queues(self, channel: AbstractChannel):
        if len(self.queues):
            return
        logger.bind(title=self.amr_info.amrId).info('init queue and bind with exchange')
        queue_pairs = get_all_queue_exchange_relationship(self.amr_info.mac_address)
        for pair in queue_pairs:
            queue = await self.rabbit_service.create_queue_and_bind(
                channel=channel,
                amrId=self.amr_info.amrId,
                queue_name=pair['q_name'],
                exchange=pair['bind_ex'],
                routing_key=pair['key'],
                q_options={'durable': True},
            )
            if queue is not None:
                self.queues[pair['q_name']] = queue
        need_consume_queue = fixListener_queues(serialNum=self.amr_info.mac_address)
        for queue_name in need_consume_queue:
            if queue_name == heartbeatPingQName(self.amr_info.mac_address):
                await self.rabbit_service.consume_queue(
                    amrId=self.amr_info.amrId,
                    queue=self.queues[queue_name],
                    cb=self.__heartbeat_consumer,
                )
            if queue_name == q2a_registerResponseQName(self.amr_info.mac_address):
                await self.rabbit_service.consume_queue(
                    amrId=self.amr_info.amrId,
                    queue=self.queues[queue_name],
                    cb=self.__register_response_consumer,
                )

    async def consume_dynamic_queue(self):
        need_consume_queues = dynamicListener_queues(self.amr_info.mac_address)
        for queue_name in need_consume_queues:
            if queue_name not in self.queues:
                logger.bind(title=self.amr_info.amrId).error(
                    f'try to consume not exist queue {queue_name}'
                )
                continue
            if queue_name in self.consuming_queue:
                logger.bind(title=self.amr_info.amrId).info(f'{queue_name} already be consume')
                continue

            if queue_name == q2a_ioQName(self.amr_info.mac_address):
                tag = await self.rabbit_service.consume_queue(
                    amrId=self.amr_info.amrId, queue=self.queues[queue_name], cb=self.__io_consumer
                )
            if queue_name == q2a_ResponseQName(self.amr_info.mac_address):
                tag = await self.rabbit_service.consume_queue(
                    amrId=self.amr_info.amrId,
                    queue=self.queues[queue_name],
                    cb=self.__response_consumer,
                )
                self.consuming_queue[queue_name] = tag
            if queue_name == q2a_handshakeQName(self.amr_info.mac_address):
                tag = await self.rabbit_service.consume_queue(
                    amrId=self.amr_info.amrId,
                    queue=self.queues[queue_name],
                    cb=self.__handshake_consumer,
                )
                self.consuming_queue[queue_name] = tag

    def __heartbeat_consumer(self, msg: HEARTBEAT):
        self.receive_request_record[msg['payload']['cmd_id']] = msg['session']
        self.heartbeat_input_.on_next(msg)

    def __handshake_consumer(self, msg: ALL_HANDSHAKE_TYPE):
        self.receive_request_record[msg['payload']['cmd_id']] = msg['session']
        self.control_transaction_input_.on_next(msg)

    def __register_response_consumer(self, msg: REGISTER_RESPONSE):
        payload = msg['payload']
        if payload.get('cmd_id') != CMD_ID.REGISTER.value:
            return
        if payload.get('id') != self._register_request_id:
            return
        if self._register_response_future is not None and not self._register_response_future.done():
            self._register_response_future.set_result(msg)

    def __io_consumer(self, msg: ALL_IO_TYPE):
        payload = msg['payload']
        if payload['cmd_id'] == 'ET':
            self.io_transaction_input_.on_next(msg)

    def __response_consumer(self, msg):
        pass

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
        qams_connect, rabbitmq_connect, mir_service_connect = connect_list

        self.amr_info.connect_w_amr = True if mir_service_connect else False

        if qams_connect and rabbitmq_connect and mir_service_connect:
            self.amr_info.connect_w_qams = True
            asyncio.create_task(self.consume_dynamic_queue())
            self.heartbeat_c.start_heartbeat_watchdog.on_next(True)
            return

        # channel/queue lifecycle is handled separately by _on_rabbitmq_connect_change,
        # driven directly off rb_connect_status
        if not qams_connect and rabbitmq_connect and mir_service_connect and len(self.queues):
            asyncio.create_task(self.connect_with_qams())
        else:
            self.amr_info.connect_w_qams = False

    async def set_amr_resource(self):

        ## maps type from qams

        class Map(BaseModel):
            id: str
            isUsing: bool
            fileName: str
            mapOriginX: float
            mapOriginY: float
            mapWidth: float
            mapHeight: float
            scale: float
            resolution: float
            scrollX: float
            scrollY: float
            floor: float
            map_group_name: str
            map_group_id: str

        class ALL_Maps(BaseModel):
            allMap: List[Map]
            systemFilePath: str

        ## location type from qams
        class Location(BaseModel):
            id: str
            locationId: str
            x: float
            y: float
            offset_x: float
            offset_y: float
            canRotate: bool
            rotate: float
            areaType: PeripheralType
            cost: int
            connectedRoadIds: List[str]
            footprint: Footprint
            neighborIds: List[str]

            map_id: str

        class ALL_Location(BaseModel):
            locations: List[Location]

        ## location type from mir
        class ALL_POSITIONS(BaseModel):
            guid: str
            url: str
            name: str
            map: str
            type_id: int

        class ALL_POSITION_SCHEMA(RootModel[List[ALL_POSITIONS]]):
            pass

        class NewPosition(BaseModel):
            guid: str
            name: str
            pos_x: float
            pos_y: float
            orientation: float
            type_id: int
            map_id: str
            created_by_id: str

        try:
            get_loc_url = f'http://{config.MISSION_CONTROL_HOST}:{config.MISSION_CONTROL_PORT}/api/test/map?type=locations'
            get_all_map_url = f'http://{config.MISSION_CONTROL_HOST}:{config.MISSION_CONTROL_PORT}/api/setting/mir/vehicle-maps'
            async with httpx.AsyncClient() as client:
                maps_res = await client.get(url=get_all_map_url, headers=headers, timeout=3)
                valid_maps_data = ALL_Maps(**maps_res.json())

                for map in valid_maps_data.allMap:
                    if map.map_group_id == 'None':
                        continue
                    try:
                        get_session_url = (
                            f'http://{self.amr_info.ip}/api/v2.0.0/sessions/{map.map_group_id}'
                        )
                        has_session = await client.get(
                            url=get_session_url, headers=headers, timeout=3
                        )
                        if 'error_code' in has_session.json():
                            logger.bind(title=self.amr_info.amrId).warning(
                                f'session {map.map_group_id} ({map.map_group_name}) '
                                'does not exist on mir, skip'
                            )
                            continue

                        get_map_url = f'http://{self.amr_info.ip}/api/v2.0.0/maps/{map.id}'
                        data = await client.get(url=get_map_url, headers=headers, timeout=3)
                        if 'error_code' in data.json():
                            logger.bind(title=self.amr_info.amrId).warning(
                                f'map {map.id} ({Path(map.fileName).stem}) '
                                'does not exist on mir, skip'
                            )
                    except Exception as e:
                        logger.bind(title=self.amr_info.amrId).error(
                            f'check map resource of {map.id} failed: {e}'
                        )

                locations_res = await client.get(url=get_loc_url, headers=headers, timeout=3)

                valid_data = ALL_Location(**locations_res.json())

            logger.bind(title=self.amr_info.amrId).info('resource sync successful')

        except (httpx.HTTPStatusError, Exception) as e:
            logger.bind(title=self.amr_info.amrId).error(e)

    async def destroy(self):
        self.start_destroy_process = True
        if self.mir_info_task is not None:
            self.mir_info_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.mir_info_task
        if self.channel is not None and not self.channel.is_closed:
            queue_pairs = get_all_queue_exchange_relationship(self.amr_info.mac_address)
            for pair in queue_pairs:
                await self.channel.queue_delete(pair['q_name'])
        await self._teardown_channel()
        for sub in self.subs:
            sub.dispose()
        await self.heartbeat_c.destroy()
        await self.status_c.destroy()
        await self.mission_c.destroy()
