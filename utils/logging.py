import logging
import os
from datetime import datetime


def setup_logger(
    filename: str, log_dir: str = "logs", level=logging.INFO
) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(filename))[0]

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    log_filename = f"{base_name}_{timestamp}.log"
    log_path = os.path.join(log_dir, log_filename)

    logger = logging.getLogger(base_name)
    logger.setLevel(level)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
