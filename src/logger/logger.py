import sys
from pathlib import Path
from typing import Tuple

from loguru import logger


def heartbeat_filter(record):
    return record['extra'].get('type') == 'heartbeat'


def color(text: str, rgb: Tuple[int, int, int]):
    r, g, b = rgb
    return f'\033[38;2;{r};{g};{b}m{text}\033[0m'


absolute_path = Path.home() / 'kenmec' / '_logs' / 'kenmec_bridge'


absolute_path.mkdir(parents=True, exist_ok=True)
(absolute_path / 'heartbeat').mkdir(parents=True, exist_ok=True)


log_file_path = str(absolute_path / '{time:YYYY-MM-DD}.log')
heartbeat_file_path = str(absolute_path / 'heartbeat' / 'heartbeat_{time:YYYY-MM-DD}.log')

logger.remove()

logger = logger.bind(title='')
logger = logger.bind(state='')
logger.level('INFO', color='<fg 151>')
logger.level('MQ', no=15, color='<fg 146>')

logger.add(
    sys.stdout,
    filter=lambda record: record['extra'].get('state') != 'heartbeat',
    format='<green>{time:YYYY-MM-DD HH:mm:ss}</green> <blue>{extra[title]}</blue> <magenta>{extra[state]}</magenta> | <level>{level:7}</level> | {message}',
)


logger.add(
    log_file_path,
    filter=lambda record: record['extra'].get('state') != 'heartbeat',
    rotation='00:00',
    retention='15 days',
    compression='zip',
    encoding='utf-8',
    enqueue=True,
    format='{time:YYYY-MM-DD HH:mm:ss} {extra[title]} | {level} | {message}',
)


logger.add(
    heartbeat_file_path,
    filter=heartbeat_filter,
    rotation='00:00',
    retention='7 days',
    compression='zip',
    encoding='utf-8',
    enqueue=True,
    format='<green>{time:YYYY-MM-DD HH:mm:ss}</green> <blue>{extra[title]}</blue> | <magenta>[Heartbeat]</magenta> | {message}',
)

# logger.add(
#     sys.stdout,
#     filter=heartbeat_filter,
#     format='<green>{time:YYYY-MM-DD HH:mm:ss}</green> <blue>{extra[title]}</blue> | <magenta>[Heartbeat]</magenta> | {message}',
# )

heartbeat_logger = logger.bind(type='heartbeat')
