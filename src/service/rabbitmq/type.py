from typing import Any, TypedDict


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
