import argparse
import json
import os
import time
from typing import Any, Dict, List, Optional

import jsonfinder  # type: ignore
from playwright.sync_api import Page, TimeoutError, sync_playwright

from const import RAW_DATA_JSON_FILENAME
from utils.base import add_timestamp_to_filename
from utils.logging import setup_logger

logger = setup_logger(__file__)

LOADER_SELECTOR = '[data-visualcompletion="loading-state"]'
INIT_DATA_KEY = "ad_library_main"
MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 2

raw_ads_data = []


def scroll_until_loaded(page, max_scrolls=1000):
    for i in range(max_scrolls):
        logger.info(f"Scrolling page for {i + 1} time")
        page.mouse.wheel(0, 10000)

        try:
            page.wait_for_selector(LOADER_SELECTOR, timeout=20000)
            page.wait_for_selector(
                LOADER_SELECTOR,
                state="detached",
                timeout=10000,
            )
        except TimeoutError:
            logger.info("Reached the end of the list or loading took too long")
            break

        time.sleep(0.5)


def get_ad_search_data(json_data):
    try:
        return json_data["data"][INIT_DATA_KEY]
    except (KeyError, TypeError):
        return None


def get_nested_value_by_key(data, key):
    if isinstance(data, dict):
        if key in data:
            return data[key]

        for value in data.values():
            result = get_nested_value_by_key(value, key)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = get_nested_value_by_key(item, key)
            if result is not None:
                return result
    return None


def find_init_data(page) -> Optional[Dict[str, Any]]:
    json_script_tags = page.query_selector_all('script[type="application/json"]')

    for script in json_script_tags:
        script_string = script.inner_text()

        if INIT_DATA_KEY in script_string:
            for _, _, obj in jsonfinder.jsonfinder(script_string):
                if obj is None:
                    continue

                init_data = get_nested_value_by_key(obj, INIT_DATA_KEY)

                if init_data:
                    logger.info(f"Found {INIT_DATA_KEY}")
                    return init_data

    return None


class InitDataNotFoundException(Exception):
    def __init__(self, message="Init data not found"):
        self.message = message


def parse_response_data(edges: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    return [
        edge["node"]["collated_results"]
        for edge in edges
        if (
            "node" in edge
            and isinstance(edge["node"], dict)
            and edge["node"].get("collated_results")
        )
    ]


def get_parsed_init_data(page: Page) -> List[List[Dict[str, Any]]]:
    init_data = find_init_data(page)

    if not init_data:
        raise InitDataNotFoundException()

    edges = init_data["search_results_connection"]["edges"]

    return parse_response_data(edges)


def find_init_data_with_retries(
    page: Page, page_url_str: str
) -> Optional[List[List[Dict[str, Any]]]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Attempt {attempt}: Navigating to page...")
            page.goto(page_url_str, wait_until="load", timeout=30000)
            parsed_init_data = get_parsed_init_data(page)
            break
        except TimeoutError:
            logger.error(f"Timeout navigating to {page_url_str}")
            return None
        except InitDataNotFoundException as e:
            logger.warning(f"Attempt {attempt}: Init data not found: {e}")
            if attempt < MAX_RETRIES:
                logger.info("Reloading page and retrying...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            else:
                logger.error("Max retries reached. Giving up.")
                return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None

    return parsed_init_data


def disable_cache(route, request):
    headers = request.headers.copy()
    headers["Cache-Control"] = "no-cache"
    route.continue_(headers=headers)


def handle_response(response):
    if "graphql" not in response.url:
        return

    ad_search_data = None

    try:
        json_body = response.json()
        ad_search_data = get_ad_search_data(json_body)
    except Exception:
        return

    if ad_search_data is not None:
        try:
            edges = ad_search_data["search_results_connection"]["edges"]
        except KeyError:
            logger.error(f"Coudn't find edges in response {ad_search_data}")
            return

        parsed_response_data = parse_response_data(edges)

        raw_ads_data.extend(parsed_response_data)


def collect_raw_data(page_url_str: str) -> str:
    with sync_playwright() as p:
        with p.chromium.launch(headless=True) as browser:
            with browser.new_context(
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}, locale="en-US"
            ) as context:
                page = context.new_page()
                page.on("response", handle_response)
                page.route("**/*", disable_cache)

                parsed_init_data = find_init_data_with_retries(page, page_url_str)

                if parsed_init_data:
                    raw_ads_data.extend(parsed_init_data)

                logger.info(f"Collected {len(raw_ads_data)} initial data ads")

                scroll_until_loaded(page)

                logger.info(f"Totally collected {len(raw_ads_data)} ads")

                filename_with_timestmap = add_timestamp_to_filename(
                    RAW_DATA_JSON_FILENAME
                )
                raw_data_file_path = f"data/raw/{filename_with_timestmap}.json"

                os.makedirs(os.path.dirname(raw_data_file_path), exist_ok=True)
                with open(raw_data_file_path, "w", encoding="utf-8") as f:
                    json.dump(raw_ads_data, f, indent=2)

                return raw_data_file_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect raw data from Facebook Ad Library."
    )
    parser.add_argument("url", type=str, help="Facebook Ad Library URL to scrape")

    args = parser.parse_args()
    collect_raw_data(args.url)
