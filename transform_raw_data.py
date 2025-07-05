import json
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from langdetect import detect
from pydantic import BaseModel, field_validator, model_validator

from const import TRANSFORMED_DATA_FILENAME, VALIDATION_REPORT_FILENAME
from utils.logging import setup_logger

logger = setup_logger(__file__)


class DisplayFormat(str, Enum):
    VIDEO = "VIDEO"
    IMAGE = "IMAGE"
    DCO = "DCO"
    CAROUSEL = "CAROUSEL"


class MediaMix(str, Enum):
    VIDEO = "video-only"
    IMAGE = "image-only"
    BOTH = "both"
    NONE = "none"


class ValidatedAd(BaseModel):
    ad_id: str
    is_active: bool
    start_date_ts: int
    end_date_ts: Optional[int]
    total_active_time_sec: Optional[int]
    ad_group_id: Optional[str]
    grouped_ads_count: Optional[int]
    display_format: DisplayFormat
    media_mix: MediaMix
    ad_text: str
    ad_lang_code: str

    @field_validator("start_date_ts", "end_date_ts", mode="before")
    @classmethod
    def validate_unix_timestamp(cls, v):
        if v is None:
            return None
        if not isinstance(v, int):
            raise TypeError("Timestamp must be an integer")
        try:
            datetime.fromtimestamp(v, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            raise ValueError(f"Invalid UNIX timestamp: {v}")
        return v

    @field_validator("ad_text")
    @classmethod
    def validate_ad_text(cls, v):
        if not isinstance(v, str):
            raise TypeError("ad_text must be a string")
        return v

    @model_validator(mode="after")
    def check_dates_order(self) -> "ValidatedAd":
        if self.start_date_ts and self.end_date_ts:
            if self.end_date_ts < self.start_date_ts:
                raise ValueError("end_date cannot be earlier than start_date")
        return self


def detect_media_mix(
    ad: Dict[str, Any], display_format: "DisplayFormat"
) -> Tuple[bool, bool]:
    has_video = False
    has_image = False

    if display_format == DisplayFormat.VIDEO:
        has_video = True
    elif display_format == DisplayFormat.IMAGE:
        has_image = True
    elif display_format in [DisplayFormat.DCO, DisplayFormat.CAROUSEL]:
        for card in ad.get("snapshot", {}).get("cards", []):
            if card.get("video_hd_url"):
                has_video = True
            if card.get("original_image_url"):
                has_image = True

    return has_video, has_image


def get_media_mix(ad: Dict[str, Any], display_format: "DisplayFormat") -> str:
    has_video, has_image = detect_media_mix(ad, display_format)

    if has_video and has_image:
        return MediaMix.BOTH
    elif has_video:
        return MediaMix.VIDEO
    elif has_image:
        return MediaMix.IMAGE
    else:
        return MediaMix.NONE


def parse_ad(ad: Dict[str, Any], group_collation_count: List[int]) -> Dict[str, Any]:
    ad_archive_id = ad["ad_archive_id"]
    is_active = ad["is_active"]

    start_date = ad["start_date"]
    end_date = ad["end_date"]
    total_active_time = ad["total_active_time"]

    collation_id = ad.get("collation_id")
    collation_count = max(group_collation_count[0], ad.get("collation_count") or 0)
    group_collation_count[0] = collation_count

    display_format = ad["snapshot"]["display_format"]
    media_mix = get_media_mix(ad, display_format)

    ad_text = ""
    try:
        if display_format in [DisplayFormat.DCO, DisplayFormat.CAROUSEL]:
            ad_text = ad["snapshot"]["cards"][0]["body"]
        else:
            ad_text = ad["snapshot"]["body"]["text"]
    except (KeyError, IndexError, TypeError) as e:
        logger.error(
            f"Couldn't find ad text in raw data {ad.get('ad_archive_id', 'unknown')}: {e}"
        )

    ad_lang_code = "undetected"
    if ad_text:
        ad_lang_code = detect(ad_text)

    return {
        "ad_id": ad_archive_id,
        "is_active": is_active,
        "start_date_ts": start_date,
        "end_date_ts": end_date,
        "total_active_time_sec": total_active_time,
        "ad_group_id": collation_id,
        "grouped_ads_count": collation_count,
        "display_format": display_format,
        "media_mix": media_mix,
        "ad_text": ad_text,
        "ad_lang_code": ad_lang_code,
    }


def parse_ad_group(ad_group: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    parsed_ads = []
    group_collation_count = [0]

    for ad in ad_group:
        try:
            parsed_ad = parse_ad(ad, group_collation_count)
            parsed_ads.append(parsed_ad)
        except Exception as e:
            logger.error(f"Error parsing ad {ad.get('ad_archive_id', 'unknown')}: {e}")
            continue

    return parsed_ads


def validate_and_clean_data(parsed_ads: List[Dict[str, Any]]):
    valid_records = []
    invalid_records = []

    for ad in parsed_ads:
        try:
            validated_ad = ValidatedAd(**ad)
            valid_records.append(validated_ad.model_dump())
        except Exception as e:
            invalid_records.append({"record": ad, "validation_error": str(e)})

    return valid_records, invalid_records


def transform_raw_data(input_file: str) -> str:
    try:
        with open(input_file) as f:
            raw_ads = json.load(f)
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_file}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in input file: {e}")
        raise

    parsed_ads = []
    for ad_group in raw_ads:
        parsed = parse_ad_group(ad_group)
        parsed_ads.extend(parsed)

    logger.info(f"Parsed {len(parsed_ads)} ads from {len(raw_ads)} ad groups")

    valid_records, invalid_records = validate_and_clean_data(parsed_ads)

    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    validation_report_file_path = (
        f"data/validation/{VALIDATION_REPORT_FILENAME}_{timestamp}.json"
    )
    os.makedirs(os.path.dirname(validation_report_file_path), exist_ok=True)

    logger.info(
        f"Validation finished. Got valid: {len(valid_records)} and invalid: {len(invalid_records)} ads"
    )
    logger.info(f"Report could be found here: {validation_report_file_path}")

    with open(validation_report_file_path, "w", encoding="utf-8") as f:
        json.dump(invalid_records, f, indent=2)

    df = pd.DataFrame(valid_records)

    #
    df_cleaned_1 = df.drop_duplicates(subset=["ad_id"], keep="first")
    df_cleaned_2 = df_cleaned_1.drop_duplicates(subset=["ad_group_id"], keep="first")
    df_cleaned_3 = df_cleaned_2.drop_duplicates(subset=["ad_text"], keep="first")

    transformed_data_file_path = (
        f"data/transformed/{TRANSFORMED_DATA_FILENAME}_{timestamp}.parquet"
    )

    os.makedirs(os.path.dirname(transformed_data_file_path), exist_ok=True)
    df_cleaned_3.to_parquet(transformed_data_file_path, index=False)

    logger.info(f"Duplicates removed, left {len(df_cleaned_3)} ads")

    return transformed_data_file_path
