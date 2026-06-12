from typing import TypedDict

HEARTBEAT_EX = 'amr.heartbeat.topic'
RES_EX = 'amr.res.topic'
IO_EX = 'amr.io.topic'
CONTROL_EX = 'amr.control.topic'

IO_QUEUE = 'qams.io.queue'
HEARTBEAT_PONG_QUEUE = 'qams.heartbeat.pong.queue'


def heartbeatPingQName(serialNum: str):
    return f'{serialNum}.heartbeat.ping.queue'


def a2q_handshakeQName(serialNum: str):
    return f'{serialNum}.qams.handshake.queue'


def q2a_amrResponseQName(serialNum: str):
    return f'{serialNum}.amr.handshake.res.queue'


def a2q_qamsResponseQName(serialNum: str):
    return f'{serialNum}.qams.control.res.queue'


def q2a_controlQName(serialNum: str):
    return f'{serialNum}.amr.control.queue'


dynamicListener = [heartbeatPingQName, q2a_controlQName, q2a_amrResponseQName]


class PublishOptions(TypedDict, total=False):
    expiration: str
    retries: int
    retryDelay: str
    persistent: bool


volatile = ['pose', 'errorInfo', 'currentId', 'poseAccurate', 'isRegistered']
HEARTBEAT_EX = 'amr.heartbeat.topic'
