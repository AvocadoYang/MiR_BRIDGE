from typing import List

from src.types.rabbitmq import Queue_Ex_Pairs

HEARTBEAT_EX = 'amr.heartbeat.topic'
RES_EX = 'amr.res.topic'
IO_EX = 'amr.io.topic'
HANDSHAKE_EX = 'amr.handshake.topic'

IO_QUEUE = 'qams.io.queue'
HEARTBEAT_PONG_QUEUE = 'qams.heartbeat.pong.queue'


## queue
def heartbeatPingQName(serialNum: str):
    return f'{serialNum}.heartbeat.ping.queue'


def heartbeatPingKey(serialNum: str):
    return f'amr.heartbeat.ping.{serialNum}'


## queue
def a2q_handshakeQName(serialNum: str):
    return f'{serialNum}.a2q.handshake.queue'


def a2q_handshakeKey(serialNum: str):
    return f'qams.{serialNum}.handshake.*'


## queue
def q2a_handshakeQName(serialNum: str):
    return f'{serialNum}.q2a.handshake.queue'


def q2a_handshakeKey(serialNum: str):
    return f'amr.{serialNum}.handshake.*'


## queue
def a2q_ResponseQName(serialNum: str):
    return f'{serialNum}.a2q.handshake.res.queue'


def a2q_ResponseKey(serialNum: str):
    return f'qams.{serialNum}.res.*'


## queue
def q2a_ResponseQName(serialNum: str):
    return f'{serialNum}.q2a.handshake.res.queue'


def q2a_ResponseKey(serialNum: str):
    return f'amr.{serialNum}.*.res'


## queue
def q2a_registerResponseQName(serialNum: str):
    return f'{serialNum}.q2a.register.res.queue'


def q2a_registerResponseKey(serialNum: str):
    return f'amr.register.res.{serialNum}'


def a2q_registerReqKey(serialNum: str):
    return f'qams.register.req.{serialNum}'


def get_all_queue_exchange_relationship(serialNum: str) -> List[Queue_Ex_Pairs]:
    return [
        {
            'q_name': heartbeatPingQName(serialNum=serialNum),
            'bind_ex': HEARTBEAT_EX,
            'key': heartbeatPingKey(serialNum=serialNum),
        },
        {
            'q_name': q2a_handshakeQName(serialNum=serialNum),
            'bind_ex': HANDSHAKE_EX,
            'key': q2a_handshakeKey(serialNum=serialNum),
        },
        {
            'q_name': a2q_handshakeQName(serialNum=serialNum),
            'bind_ex': HANDSHAKE_EX,
            'key': a2q_handshakeKey(serialNum=serialNum),
        },
        {
            'q_name': q2a_ResponseQName(serialNum=serialNum),
            'bind_ex': RES_EX,
            'key': q2a_ResponseKey(serialNum=serialNum),
        },
        {
            'q_name': a2q_ResponseQName(serialNum=serialNum),
            'bind_ex': RES_EX,
            'key': a2q_ResponseKey(serialNum=serialNum),
        },
        {
            'q_name': q2a_registerResponseQName(serialNum),
            'bind_ex': RES_EX,
            'key': q2a_registerResponseKey(serialNum),
        },
    ]


def fixListener_queues(serialNum):
    return [heartbeatPingQName(serialNum), q2a_registerResponseQName(serialNum)]


def dynamicListener_queues(serialNum):
    return [
        q2a_handshakeQName(serialNum=serialNum),
        q2a_ResponseQName(serialNum=serialNum),
    ]


volatile = ['pose', 'errorInfo', 'currentId', 'poseAccurate', 'isRegistered']
HEARTBEAT_EX = 'amr.heartbeat.topic'
