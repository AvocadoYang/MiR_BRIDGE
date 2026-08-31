import pathlib
from typing import List

import yaml
from loguru import logger
from pydantic import BaseModel

cwd = pathlib.Path(__file__).parent / 'config.yaml'


class Config(BaseModel):
    FILE_PATH: str = 'kenmec/'
    RABBIT_MQ_HOST: str = '127.0.0.1'
    RABBIT_MQ_PORT: int = 5672
    RABBIT_MQ_USER: str
    RABBIT_MQ_PASSWORD: str
    MISSION_CONTROL_HOST: str
    MISSION_CONTROL_PORT: int
    MIR_ACCOUNT: str = 'distributor'
    MIR_PASSWORD: str = 'distributor'
    ELEVATORS: List[List[str]]


def _load_yml_config(path: pathlib.Path):
    """Classmethod returns YAML config"""
    try:
        return yaml.safe_load(path.read_text())

    except FileNotFoundError as error:
        message = 'Error: yml config file not found.'
        logger.error(message)
        raise FileNotFoundError(error, message) from error
    except Exception:
        raise


test = _load_yml_config(cwd)

config = Config(**_load_yml_config(cwd))
