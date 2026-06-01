import pathlib
from typing import Union

import yaml
from loguru import logger
from pydantic import BaseModel

cwd = pathlib.Path(__file__).parent / "config.yaml"


class Config(BaseModel):
    RABBIT_MQ_HOST_1: str = "127.0.0.1"
    RABBIT_MQ_PORT_1: int = 5672
    RABBIT_MQ_UI_PORT_1: int = 15672
    RABBIT_NODE_NAME_1: str
    RABBIT_MQ_HOST_2: Union[str, None] = None
    RABBIT_MQ_PORT_2: int = 5672
    RABBIT_MQ_UI_PORT_2: int = 15672
    RABBIT_NODE_NAME_2: Union[str, None] = None
    RABBIT_MQ_USER: str
    RABBIT_MQ_PASSWORD: str
    MISSION_CONTROL_HOST: str
    MISSION_CONTROL_PORT: int
    MAC: str
    AMR_TYPE: str
    AMR_SERVICE_BRIDGE_HOST: str = "127.0.0.1"
    AMR_SERVICE_BRIDGE_POST: int = 8532


def _load_yml_config(path: pathlib.Path):
    """Classmethod returns YAML config"""
    try:
        return yaml.safe_load(path.read_text())

    except FileNotFoundError as error:
        message = "Error: yml config file not found."
        logger.error(message)
        raise FileNotFoundError(error, message) from error
    except Exception as e:
        raise


config = Config(**_load_yml_config(cwd))
