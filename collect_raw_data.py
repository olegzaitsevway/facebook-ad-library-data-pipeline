import json
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from playwright.sync_api import TimeoutError, sync_playwright

from const import RAW_DATA_JSON_FILENAME
from utils.logging import setup_logger

logger = setup_logger(__file__)

LOADER_SELECTOR = '[data-visualcompletion="loading-state"]'
INIT_DATA_KEY = "ad_library_main"
MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 2


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


def find_key(d, target_key):
    if isinstance(d, dict):
        for key, value in d.items():
            if key == target_key:
                return value
            result = find_key(value, target_key)
            if result is not None:
                return result
    elif isinstance(d, list):
        for item in d:
            result = find_key(item, target_key)
            if result is not None:
                return result
    return None


def find_init_data(page) -> Optional[Dict[str, Any]]:
    script_tags = page.query_selector_all("script")

    ad_library_main = None

    for script in script_tags:
        try:
            content = script.inner_text()
            if INIT_DATA_KEY in content:
                json_text = content.strip()
                match = re.search(rf'{{.*"{INIT_DATA_KEY}".*}}', json_text, re.DOTALL)
                if match:
                    json_data = json.loads(match.group())
                    ad_library_main = find_key(json_data, INIT_DATA_KEY)
                    if ad_library_main:
                        logger.info(f"Found {INIT_DATA_KEY}")
                        break
        except Exception:
            continue

    return ad_library_main


class InitDataNotFoundException(Exception):
    def __init__(self, message="Init data not found"):
        self.message = message


def get_parsed_init_data(page) -> List[Dict[str, Any]]:
    init_data = find_init_data(page)

    if not init_data:
        raise InitDataNotFoundException()

    edges = init_data["search_results_connection"]["edges"]

    return [
        edge["node"]["collated_results"]
        for edge in edges
        if (
            "node" in edge
            and isinstance(edge["node"], dict)
            and edge["node"].get("collated_results")
        )
    ]


def disable_cache(route, request):
    headers = request.headers.copy()
    headers["Cache-Control"] = "no-cache"
    route.continue_(headers=headers)


def collect_raw_data(page_url_str: str) -> str:
    ads_data = []

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

            ads_data.extend(
                [
                    edge["node"]["collated_results"]
                    for edge in edges
                    if (
                        "node" in edge
                        and isinstance(edge["node"], dict)
                        and edge["node"].get("collated_results")
                    )
                ]
            )

    with sync_playwright() as p:
        with p.chromium.launch(headless=True) as browser:
            with browser.new_context(
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}, locale="en-US"
            ) as context:
                page = context.new_page()
                page.on("response", handle_response)
                page.route("**/*", disable_cache)

                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        logger.info(f"Attempt {attempt}: Navigating to page...")
                        page.goto(page_url_str, wait_until="load", timeout=30000)
                        parsed_init_data = get_parsed_init_data(page)
                        break
                    except TimeoutError:
                        logger.error(f"Timeout navigating to {page_url_str}")
                        return
                    except InitDataNotFoundException as e:
                        logger.warning(f"Attempt {attempt}: Init data not found: {e}")
                        if attempt < MAX_RETRIES:
                            logger.info("Reloading page and retrying...")
                            time.sleep(RETRY_DELAY_SECONDS)
                            continue
                        else:
                            logger.error("Max retries reached. Giving up.")
                            return
                    except Exception as e:
                        logger.error(f"Unexpected error: {e}")
                        return

                logger.info(f"Collected {len(parsed_init_data)} initial data ads")

                if parsed_init_data:
                    ads_data = parsed_init_data

                scroll_until_loaded(page)

                logger.info(f"Totally collected {len(ads_data)} ads")

                now = datetime.now()
                timestamp = now.strftime("%Y%m%d_%H%M%S")

                raw_data_file_path = (
                    f"data/raw/{RAW_DATA_JSON_FILENAME}_{timestamp}.json"
                )

                os.makedirs(os.path.dirname(raw_data_file_path), exist_ok=True)

                with open(raw_data_file_path, "w", encoding="utf-8") as f:
                    json.dump(ads_data, f, indent=2)

                return raw_data_file_path
