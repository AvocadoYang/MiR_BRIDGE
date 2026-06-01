import cowsay
import json
from loguru import logger
from src.configs.config import config
from src.helper.helper import format_date


def main():
    cow_text = cowsay.get_output_string(
        "cow",
        f"-- {format_date()} -- \n"
        f'start running "amr_core_node"!\n'
        f"config file:\n"
        f"{json.dumps(config.model_dump(), indent=2, ensure_ascii=False)} \n",
    )
    logger.opt(raw=True).info(cow_text + "\n")


if __name__ == "__main__":
    main()
