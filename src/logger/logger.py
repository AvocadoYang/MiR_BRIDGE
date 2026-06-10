import os
import sys
from typing import Tuple

from loguru import logger


def heartbeat_filter(record):
    return record["extra"].get("type") == "heartbeat"


def color(text: str, rgb: Tuple[int, int, int]):
    r, g, b = rgb
    return f"\033[38;2;{r};{g};{b}m{text}\033[0m"


path_with_tilde = "~/kenmec/_logs/amr_core"
absolute_path = os.path.abspath(os.path.expanduser(path_with_tilde))
log_file_path = os.path.join(absolute_path, "{time:YYYY-MM-DD}.log")
heartbeat_file_path = os.path.join(absolute_path, "heartbeat_{time:YYYY-MM-DD}.log")

logger.remove()
logger.level("INFO", color="<fg 151>")
logger.level("MQ", no=15, color="<fg 146>")

logger.add(
    sys.stdout,
    filter=lambda record: record["extra"].get("type") != "heartbeat",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:7}</level> | {message}",
)


logger.add(
    log_file_path,
    filter=lambda record: record["extra"].get("type") != "heartbeat",
    rotation="00:00",
    retention="15 days",
    compression="zip",
    encoding="utf-8",
    enqueue=True,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
)


logger.add(
    heartbeat_file_path,
    filter=heartbeat_filter,
    rotation="00:00",
    retention="7 days",
    compression="zip",
    encoding="utf-8",
    enqueue=True,
)

heartbeat_logger = logger.bind(type="heartbeat")
