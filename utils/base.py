from datetime import datetime


def add_timestamp_to_filename(filename: str) -> str:
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    return f"filename_{timestamp}"
