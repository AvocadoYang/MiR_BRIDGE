from typing import List

from src.types.rabbitmq import Queue_Ex_Pairs

HEARTBEAT_EX = 'amr.heartbeat.topic'
RES_EX = 'amr.res.topic'
IO_EX = 'amr.io.topic'
HANDSHAKE_EX = 'amr.handshake.topic'

IO_QUEUE = 'qams.io.queue'
EQUIPMENT_IO_QUEUE = 'equipment.io.queue'
HEARTBEAT_PONG_QUEUE = 'qams.heartbeat.pong.queue'
REGISTER_REQ_QUEUE = 'qams.register.req.queue'


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
def a2q_ResponseQName(serialNum: str):
    return f'{serialNum}.a2q.handshake.res.queue'


def a2q_ResponseKey(serialNum: str):
    return f'qams.{serialNum}.res.*'


## queue
def q2a_handshakeQName(serialNum: str):
    return f'{serialNum}.q2a.handshake.queue'


def q2a_handshakeKey(serialNum: str):
    return f'amr.{serialNum}.handshake.*'


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


## queue
def q2a_ioQName(serialNum: str):
    return f'{serialNum}.q2a.io.queue'


def q2a_ioKey(serialNum: str):
    return f'q2a.io.*.{serialNum}'


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
            'q_name': q2a_ResponseQName(serialNum=serialNum),
            'bind_ex': RES_EX,
            'key': q2a_ResponseKey(serialNum=serialNum),
        },
        {
            'q_name': a2q_handshakeQName(serialNum=serialNum),
            'bind_ex': HANDSHAKE_EX,
            'key': a2q_handshakeKey(serialNum=serialNum),
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
        {
            'q_name': q2a_ioQName(serialNum=serialNum),
            'bind_ex': IO_EX,
            'key': q2a_ioKey(serialNum=serialNum),
        },
    ]


def help2init_queue_exchange_relationship():
    return [
        {'q_name': HEARTBEAT_PONG_QUEUE, 'bind_ex': HEARTBEAT_EX, 'key': 'qams.heartbeat.pong.*'},
        {'q_name': REGISTER_REQ_QUEUE, 'bind_ex': HANDSHAKE_EX, 'key': 'qams.register.req.*'},
        {'q_name': IO_QUEUE, 'bind_ex': IO_EX, 'key': 'amr.io.*.*'},
        {'q_name': EQUIPMENT_IO_QUEUE, 'bind_ex': IO_EX, 'key': 'equipment.io.*.*'},
    ]


def fixListener_queues(serialNum):
    return [heartbeatPingQName(serialNum), q2a_registerResponseQName(serialNum)]


def dynamicListener_queues(serialNum):
    return [
        q2a_ioQName(serialNum=serialNum),
        q2a_ResponseQName(serialNum=serialNum),
        q2a_handshakeQName(serialNum=serialNum),
    ]


volatile = ['pose', 'errorInfo', 'currentId', 'poseAccurate', 'isRegistered']
HEARTBEAT_EX = 'amr.heartbeat.topic'
