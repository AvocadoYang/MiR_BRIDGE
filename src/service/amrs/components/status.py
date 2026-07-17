import asyncio
import json
import math
import random
import time
import uuid
from enum import IntEnum
from typing import List

import httpx
import websockets
from pydantic import BaseModel
from reactivex import Subject
from reactivex.abc import DisposableBase
from reactivex.subject import BehaviorSubject

from src.logger import logger
from src.service.rabbitmq import ALL_CONTROL_TYPE, CMD_ID, Rabbit_client_async
from src.service.rabbitmq.queues import IO_EX, RES_EX
from src.service.rabbitmq.transaction_wrapper import (
    Send_IO_INFO,
    Send_MiR_AMR_STATUE,
    Send_Point_Cloud,
    Send_Pose,
    Send_Ready_To_Joystick_Cmd,
    base_transaction_res,
)
from src.service.webService.httpx_set import headers
from src.types.amr import AMR_INFO, BatteryInfo, IOInfo, Twist
from src.types.rabbitmq import PUBLISH_OPTIONS
from src.types.ros import (
    CallService,
    LaserMapPointCloud,
    Pose,
    PublishMessage,
    Quaternion,
    RobotStatus,
    TFMessage,
)

# max speed applied when converting joystick x/y (-100..100) to linear/angular velocity
# TODO: tune against real robot behavior
JOYSTICK_MAX_LINEAR_X = 0.8
JOYSTICK_MAX_ANGULAR_Z = 0.5
JOYSTICK_TOKEN_TIMEOUT = 2.0
# MiR robotState value (test use 11) that puts the robot into joystick control mode
JOYSTICK_ROBOT_STATE = 11


class State_Payload(BaseModel):
    state_id: int


class StopType(IntEnum):
    DEFAULT = 0  # 可繼續動作
    FINISH_CURRENT_ACTION_THEN_PAUSE = 1  # 做完現在動作後停止
    PAUSE_IMMEDIATELY = 2  # 立刻停止


class PauseBody(BaseModel):
    StopType: StopType


class PausePayload(BaseModel):
    Id: str  # 傳送此指令的唯一值
    Action: str  # 若已有 Action Enum，可改成 Action
    Time: int  # 時間戳記
    Device: str  # 傳送對象 ID
    Body: PauseBody


class Status:
    def __init__(
        self,
        amr_info: AMR_INFO,
        mir_service_connect_status: BehaviorSubject[bool],
        receive_request_record: dict[str, str],
        rabbit_service: Rabbit_client_async,
        control_transaction_sub_: Subject[ALL_CONTROL_TYPE],
    ):
        self.amr_info = amr_info
        self.position: Pose = Pose(x=0, y=0, yaw=0)
        self.mir_token = ""

        self.receive_request_record = receive_request_record
        self.mir_service_connect_status: BehaviorSubject[bool] = (
            mir_service_connect_status
        )
        self.rb = rabbit_service

        self.amr_status_signal: Subject[str] = Subject()

        self.joystick_token: str | None = None
        self.joystick_token_ready = asyncio.Event()
        self.joystick_token_lock = asyncio.Lock()
        self.web_session_id: str | None = None

        # 從 /robot_status 訂閱的最新資料，主要用來判斷 joystick 是否 available 以及是否被其他人使用
        self.robot_state_text: str = ""
        self.robot_joystick_web_session_id: str = ""

        self.subs: List[DisposableBase] = [
            control_transaction_sub_.subscribe(self.action_processor)
        ]

    def action_processor(self, action: ALL_CONTROL_TYPE):
        payload = action["payload"]
        if payload["cmd_id"] == CMD_ID.UPDATE_MAP.value:
            asyncio.create_task(
                self.rb.res_publish(
                    exchange_name=RES_EX,
                    routing_key=f"qams.{self.amr_info.mac_address}.res.updateMap",
                    last_receive_req=self.receive_request_record,
                    mac_address=self.amr_info.mac_address,
                    message=base_transaction_res(action=action, return_code="200"),
                )
            )
        if payload["cmd_id"] == CMD_ID.FORCE_RESET.value:
            asyncio.create_task(self.request_error_reset())
            asyncio.create_task(
                self.rb.res_publish(
                    exchange_name=RES_EX,
                    routing_key=f"qams.{self.amr_info.mac_address}.res.forceReset",
                    last_receive_req=self.receive_request_record,
                    mac_address=self.amr_info.mac_address,
                    message=base_transaction_res(action=action, return_code="200"),
                )
            )
        if payload["cmd_id"] == CMD_ID.EMERGENCY_STOP.value:
            try:
                data = PausePayload.model_validate_json(payload["payload"])
                state = data.Body.StopType.value
                asyncio.create_task(self.reset_work_status(status=state))
            except Exception as e:
                logger.bind(title=self.amr_info.amrId).error(
                    f"emergency stop payload parse failed: {e}"
                )
        if payload["cmd_id"] == CMD_ID.JOYSTICK.value:
            asyncio.create_task(self.send_joystick_command(payload["x"], payload["y"]))

    async def ros_bridge_connect(self, mir_token: str):
        """
        create websocket connect with ROS Bridge
        """
        self.mir_token = mir_token

        url = f"ws://{self.amr_info.ip}/rosbridge/"
        cookie_header = {"Cookie": f"mir-auth-token={self.mir_token}"}

        while True:
            try:
                async with websockets.connect(
                    url,
                    additional_headers=cookie_header,
                    ping_interval=1.5,
                    ping_timeout=1.5,
                ) as websocket:
                    self.ws = websocket

                    topics_to_subscribe = [
                        # '/tf',
                        "/mirwebapp/laser_map_pointcloud",
                        "/robot_status",
                        "/PB/ready_to_send_mc_cmd",
                    ]

                    for topic in topics_to_subscribe:
                        sub_msg = {"op": "subscribe", "topic": topic}
                        await websocket.send(json.dumps(sub_msg))

                    logger.bind(title=self.amr_info.amrId).info(
                        "ROS Bridge connect successfully, QAMS bridge was connect with amr."
                    )

                    # await self.get_mir_amr_map_resource()

                    self.mir_service_connect_status.on_next(True)
                    async for message in websocket:
                        if isinstance(message, bytes):
                            message_str = message.decode("utf-8")
                        else:
                            message_str = message

                        await self._handle_ros_message(message_str)

            except websockets.ConnectionClosed as e:
                logger.bind(title=self.amr_info.amrId).warning(
                    f"ROS Bridge disconnection ({e}), retry connect after 3s ..."
                )
            except Exception as e:
                logger.bind(title=self.amr_info.amrId).error(
                    f"ROS Bridge connects failed: {e}, retry connect after 3s ..."
                )

            self.ws = None
            self.joystick_token = None
            self.joystick_token_ready.clear()
            self.web_session_id = None
            self.robot_state_text = ""
            self.robot_joystick_web_session_id = ""
            self.mir_service_connect_status.on_next(False)
            await asyncio.sleep(3)

    async def _handle_ros_message(self, raw_message: str):
        """
        parse ROS Bridge message to JSON formate
        """
        try:
            payload = json.loads(raw_message)
            topic = payload.get("topic")
            if topic == "/mirwebapp/laser_map_pointcloud":
                point_cloud: LaserMapPointCloud = payload.get("msg")
                point_cloud_msg = Send_Point_Cloud(
                    height=point_cloud["height"],
                    width=point_cloud["width"],
                    fields=point_cloud["fields"],
                    is_bigendian=point_cloud["is_bigendian"],
                    point_step=point_cloud["point_step"],
                    row_step=point_cloud["row_step"],
                    data=point_cloud["data"],
                    is_dense=point_cloud["is_dense"],
                )
                options = PUBLISH_OPTIONS()
                options.expiration = 2
                await self.rb.req_publish(
                    exchange_name=IO_EX,
                    routing_key=f"amr.io.{self.amr_info.amrId}.point_cloud",
                    amr_info=self.amr_info,
                    message=point_cloud_msg,
                    options=options,
                )

            if topic == "/robot_status":
                status_msg_data: RobotStatus = payload.get("msg")

                self.robot_state_text = status_msg_data["state_text"]
                self.robot_joystick_web_session_id = status_msg_data.get(
                    "joystick_web_session_id", ""
                )

                if self.joystick_token and not self.robot_joystick_web_session_id:
                    logger.bind(title=self.amr_info.amrId).info(
                        "joystick session expired on robot side, "
                        "will re-register on next joystick command"
                    )
                    self.joystick_token = None
                    self.web_session_id = None
                    self.joystick_token_ready.clear()

                pose: Pose = {
                    "x": status_msg_data["position"]["x"],
                    "y": status_msg_data["position"]["y"],
                    "yaw": status_msg_data["position"]["orientation"],
                }
                pose_msg = Send_Pose(**pose)
                options = PUBLISH_OPTIONS()
                options.expiration = 2
                await self.rb.req_publish(
                    exchange_name=IO_EX,
                    routing_key=f"amr.io.{self.amr_info.amrId}.pose",
                    amr_info=self.amr_info,
                    message=pose_msg,
                    options=options,
                )
                self.amr_status_signal.on_next(status_msg_data["state_text"])
                status_msg = Send_MiR_AMR_STATUE(status=status_msg_data["state_text"])
                await self.rb.req_publish(
                    exchange_name=IO_EX,
                    routing_key=f"amr.io.{self.amr_info.amrId}.mir_amr_status",
                    amr_info=self.amr_info,
                    message=status_msg,
                    options=options,
                )

                battery_info = BatteryInfo(
                    battery=status_msg_data["battery_percentage"],
                    voltage=status_msg_data["battery_voltage"],
                    write_battery_info_time=status_msg_data["battery_time_remaining"],
                )

                velocity = status_msg_data["velocity"]
                twist = Twist(
                    set_linear_x=velocity["linear"],
                    set_angular_z=velocity["angular"],
                    odom_linear_x=velocity["linear"],
                    odom_angular_z=velocity["angular"],
                )

                io = IOInfo(battery_info=battery_info, twist=twist)

                io_info = Send_IO_INFO(io=io.model_dump_json())
                await self.rb.req_publish(
                    exchange_name=IO_EX,
                    routing_key=f"amr.io.{self.amr_info.amrId}.ioInfo",
                    amr_info=self.amr_info,
                    message=io_info,
                    options=options,
                )

            if topic == "/tf":
                pose_msg_data: TFMessage = payload.get("msg")
                position = pose_msg_data["transforms"][0]
                pose: Pose = {
                    "x": position["transform"]["translation"]["x"],
                    "y": position["transform"]["translation"]["y"],
                    "yaw": self.sanitize_degree(
                        self.quaternion_to_yaw(position["transform"]["rotation"])
                    ),
                }
                self.position = pose
                msg = Send_Pose(**pose)
                options = PUBLISH_OPTIONS()
                options.expiration = 2
                # await self.rb.req_publish(
                #     exchange_name=IO_EX,
                #     routing_key=f'amr.io.{self.amr_info.amrId}.ioInfo',
                #     amr_info=self.amr_info,
                #     message=msg,
                #     options=options,
                # )
                return

            if topic == "/PB/ready_to_send_mc_cmd":
                ready_msg_data = payload.get("msg")["data"]
                joystick_owned_by_others = bool(
                    self.robot_joystick_web_session_id
                ) and (self.robot_joystick_web_session_id != self.web_session_id)
                joystick_available = (
                    bool(ready_msg_data) and not joystick_owned_by_others
                )
                ready_msg = Send_Ready_To_Joystick_Cmd(
                    joystick_available=joystick_available,
                    status_text=self.robot_state_text,
                )

                options = PUBLISH_OPTIONS()
                options.expiration = 2
                await self.rb.req_publish(
                    exchange_name=IO_EX,
                    routing_key=f"amr.io.{self.amr_info.amrId}.ready_to_joystick_cmd",
                    amr_info=self.amr_info,
                    message=ready_msg,
                    options=options,
                )

            if payload.get("op") == "service_response":
                values = payload.get("values") or {}
                if "joystick_token" in values:
                    token = values["joystick_token"]
                    # 空字串代表機器人明確拒絕(忙碌中)，存成 None 但仍叫醒 waiter，
                    # 讓 send_joystick_command 立刻分辨忙碌、不必等滿 timeout
                    self.joystick_token = token or None
                    self.joystick_token_ready.set()

            else:
                pass

        except json.JSONDecodeError:
            print(f"parse error: {raw_message}")

    def quaternion_to_yaw(self, quaternion: Quaternion):
        sinY_cosP = 2 * (quaternion["x"] * quaternion["z"])
        cosY_cosP = 1 - 2 * (quaternion["z"] * quaternion["z"])
        degree = math.atan2(sinY_cosP, cosY_cosP) * (180 / math.pi)
        return degree

    def sanitize_degree(self, deg: float):
        return ((deg % 360) + 360) % 360

    def _generate_web_session_id(self) -> str:
        # mimics the "<ms timestamp>-<0~100 random float>" shape observed
        # from a real joystick client's setRobotState call
        return f"{int(time.time() * 1000)}-{random.uniform(0, 100)}"

    async def request_error_reset(self):
        if self.ws is None:
            return

        msg: CallService = {
            "op": "call_service",
            "id": "call_service:/mirsupervisor/requestErrorReset:20",
            "service": "/mirsupervisor/requestErrorReset",
            "type": "std_srv/Empty",
            "args": {},
        }

        await self.ws.send(json.dumps(msg))

    async def reset_work_status(self, status: int):
        if not self.amr_info.connect_w_amr:
            return
        url = f"http://{self.amr_info.ip}/api/v2.0.0/status"
        try:
            async with httpx.AsyncClient() as client:
                payload = State_Payload(state_id=(4 if status == 2 else 3))
                res = await client.put(
                    url=url, json=payload.model_dump(), headers=headers, timeout=3
                )
        except (httpx.HTTPStatusError, Exception) as e:
            print(e)

    async def send_joystick_command(self, x: float, y: float):
        if self.ws is None:
            return

        if not self.joystick_token:
            async with self.joystick_token_lock:
                # re-check after acquiring the lock: another concurrent call may have
                # already fetched the token while we were waiting for it
                if not self.joystick_token:
                    if self.web_session_id is None:
                        self.web_session_id = self._generate_web_session_id()

                    self.joystick_token_ready.clear()
                    set_state_msg: CallService = {
                        "op": "call_service",
                        "id": f"call_service:/mirsupervisor/setRobotState:{uuid.uuid4()}",
                        "service": "/mirsupervisor/setRobotState",
                        "type": "mirSupervisor/SetState",
                        "args": {
                            "robotState": JOYSTICK_ROBOT_STATE,
                            "web_session_id": self.web_session_id,
                        },
                    }
                    await self.ws.send(json.dumps(set_state_msg))

                    try:
                        await asyncio.wait_for(
                            self.joystick_token_ready.wait(),
                            timeout=JOYSTICK_TOKEN_TIMEOUT,
                        )
                    except asyncio.TimeoutError:
                        logger.bind(title=self.amr_info.amrId).warning(
                            "no service_response within timeout, dropping joystick command"
                        )
                        return

        if not self.joystick_token:
            logger.bind(title=self.amr_info.amrId).warning(
                "robot returned empty token (busy / not available), dropping joystick command"
            )
            return

        msg: PublishMessage = {
            "op": "publish",
            "id": f"publish:/joystick_vel:{uuid.uuid4()}",
            "topic": "/joystick_vel",
            "msg": {
                "joystick_token": self.joystick_token,
                "speed_command": {
                    "linear": {"x": (y / 100) * JOYSTICK_MAX_LINEAR_X, "y": 0, "z": 0},
                    "angular": {
                        "x": 0,
                        "y": 0,
                        "z": -(x / 100) * JOYSTICK_MAX_ANGULAR_Z,
                    },
                },
            },
            "latch": False,
        }

        await self.ws.send(json.dumps(msg))

    async def destroy(self):
        for sub in self.subs:
            sub.dispose()
