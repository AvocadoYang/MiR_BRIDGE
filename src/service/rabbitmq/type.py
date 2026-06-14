from dataclasses import dataclass
from typing import Any, List, TypedDict, Union


class RABBIT_CREATE_EX_OPTION(TypedDict, total=False):
    durable: bool
    internal: bool
    arguments: Any


class RABBIT_CREATE_QUEUE_OPTIONS(TypedDict, total=False):
    durable: bool
    quorum: bool
    exclusive: bool
    autoDelete: bool
    arguments: Any


class Pose(TypedDict):
    x: float
    y: float
    yaw: float


class Error_Info(TypedDict):
    warning_msg: List[str]
    warning_id: List[str]


@dataclass
class PUBLISH_OPTIONS:
    expiration: Union[int, None] = None
    retries: int = 3
    retry_delay: int = 1500
    persistent: bool = False
