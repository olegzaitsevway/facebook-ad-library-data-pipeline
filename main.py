from collect_raw_data import collect_raw_data
from generate_report import generate_report
from transform_raw_data import transform_raw_data
from utils.logging import setup_logger

logger = setup_logger(__file__)

# TODO: Make this as parameter
# Also it would be good to make each of the steps run independantly
AD_LIBRARY_URL = "https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=US&is_targeted_country=false&media_type=all&q=microlearning&search_type=keyword_unordered&source=nav-header&start_date[min]=2024-01-01&start_date[max]"


def main(ad_lib_url: str) -> None:
    logger.info(f"Pipeline has started data collection from {ad_lib_url}")
    raw_data_file_path = collect_raw_data(ad_lib_url)

    logger.info(f"Collection finished, raw data file here: {raw_data_file_path}")
    transformed_data_file_path = transform_raw_data(raw_data_file_path)

    logger.info(
        f"Transformation finished, clean data file here: {transformed_data_file_path}"
    )
    report_file_path = generate_report(transformed_data_file_path)

    logger.info(f"Report with top 10 performing ads here: {report_file_path}")


if __name__ == "__main__":
    main(AD_LIBRARY_URL)
