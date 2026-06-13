from typing import List, TypedDict

HEARTBEAT_EX = 'amr.heartbeat.topic'
RES_EX = 'amr.res.topic'
IO_EX = 'amr.io.topic'
CONTROL_EX = 'amr.control.topic'

IO_QUEUE = 'qams.io.queue'
HEARTBEAT_PONG_QUEUE = 'qams.heartbeat.pong.queue'


def heartbeatPingQName(serialNum: str):
    return f'{serialNum}.heartbeat.ping.queue'


def heartbeatPingKey(serialNum: str):
    return f'amr.heartbeat.ping.{serialNum}'


def a2q_handshakeQName(serialNum: str):
    return f'{serialNum}.qams.handshake.queue'


def a2q_handshakeKey(serialNum: str):
    return f'qams.{serialNum}.handshake.*'


def q2a_amrResponseQName(serialNum: str):
    return f'{serialNum}.amr.handshake.res.queue'


def q2a_amrResponseKey(serialNum: str):
    return f'amr.{serialNum}.*.res'


def a2q_qamsResponseQName(serialNum: str):
    return f'{serialNum}.qams.control.res.queue'


def a2q_qamsResponseKey(serialNum: str):
    return f'qams.{serialNum}.res.*'


def q2a_controlQName(serialNum: str):
    return f'{serialNum}.amr.control.queue'


def q2a_controlKey(serialNum: str):
    return f'amr.{serialNum}.control.*'


class Queue_Ex_Pairs(TypedDict):
    q_name: str
    bind_ex: str
    key: str


def get_all_queue_exchange_relationship(serialNum: str) -> List[Queue_Ex_Pairs]:
    return [
        {
            'q_name': heartbeatPingQName(serialNum=serialNum),
            'bind_ex': HEARTBEAT_EX,
            'key': heartbeatPingKey(serialNum=serialNum),
        },
        {
            'q_name': q2a_controlQName(serialNum=serialNum),
            'bind_ex': CONTROL_EX,
            'key': q2a_controlKey(serialNum=serialNum),
        },
        {
            'q_name': a2q_handshakeQName(serialNum=serialNum),
            'bind_ex': CONTROL_EX,
            'key': a2q_handshakeKey(serialNum=serialNum),
        },
        {
            'q_name': q2a_amrResponseQName(serialNum=serialNum),
            'bind_ex': RES_EX,
            'key': q2a_amrResponseKey(serialNum=serialNum),
        },
        {
            'q_name': a2q_qamsResponseQName(serialNum=serialNum),
            'bind_ex': RES_EX,
            'key': a2q_qamsResponseKey(serialNum=serialNum),
        },
    ]


def dynamicListener_queues(serialNum):
    return [
        heartbeatPingQName(serialNum=serialNum),
        q2a_controlQName(serialNum=serialNum),
        q2a_amrResponseQName(serialNum=serialNum),
    ]


class PublishOptions(TypedDict, total=False):
    expiration: str
    retries: int
    retryDelay: str
    persistent: bool


volatile = ['pose', 'errorInfo', 'currentId', 'poseAccurate', 'isRegistered']
HEARTBEAT_EX = 'amr.heartbeat.topic'
