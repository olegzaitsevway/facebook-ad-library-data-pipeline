import os
from datetime import datetime, timezone

import pandas as pd

from const import REPORT_FILENAME
from utils.logging import setup_logger

logger = setup_logger(__file__)


def compute_seconds_passed(row):
    now = datetime.now(timezone.utc)
    if pd.notnull(row["total_active_time_sec"]):
        return row["total_active_time_sec"]
    return (now - row["start_date"]).total_seconds()


def generate_report(input_file: str) -> str:
    df = pd.read_parquet(input_file)

    df["ad_link"] = "https://www.facebook.com/ads/library/?id=" + df["ad_id"]

    df["start_date"] = pd.to_datetime(df["start_date_ts"], unit="s", utc=True)
    df["end_date"] = df.apply(
        lambda row: None
        if (row["end_date_ts"] is None or row["end_date_ts"] == row["start_date_ts"])
        else datetime.fromtimestamp(row["end_date_ts"], tz=timezone.utc),
        axis=1,
    )

    df["seconds_passed"] = df.apply(compute_seconds_passed, axis=1)
    df["hours_passed"] = (df["seconds_passed"] / 3600).round(0).astype("int")

    active_ads = df[df["is_active"]]

    top_ads = active_ads.sort_values(by="hours_passed", ascending=False).head(10)

    report_cols = [
        "ad_id",
        "ad_link",
        "is_active",
        "start_date",
        "end_date",
        "hours_passed",
        "media_mix",
        "ad_text",
        "ad_lang_code",
    ]
    top_report = top_ads[report_cols]

    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    report_file_path = f"data/reports/{REPORT_FILENAME}_{timestamp}.csv"

    os.makedirs(os.path.dirname(report_file_path), exist_ok=True)
    top_report.to_csv(report_file_path, index=False)

    return report_file_path
