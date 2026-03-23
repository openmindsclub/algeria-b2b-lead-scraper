import csv
import logging
from typing import Set

import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)

DIRECTORY_URL = "https://www.algeriayp.com/browse-business-directory"
OUTPUT_CSV = "master_categories_list.csv"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def extract_category_slugs(html: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = soup.find_all("a", href=lambda href: href and "/category/" in href)

    categories: Set[str] = set()
    for link in links:
        href = link.get("href") or ""
        slug = href.split("/category/")[-1].split("/")[0].strip()
        if slug and len(slug) > 1:
            categories.add(slug)

    return categories


def save_categories_to_csv(categories: Set[str], filename: str) -> None:
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        for category in sorted(categories):
            writer.writerow([category])


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    headers = {"User-Agent": USER_AGENT}

    LOGGER.info("Phase 1: Master category mapper")
    LOGGER.info("Fetching directory page: %s", DIRECTORY_URL)

    try:
        with requests.Session() as session:
            response = session.get(DIRECTORY_URL, headers=headers, timeout=15)
            response.raise_for_status()

        categories = extract_category_slugs(response.text)
        LOGGER.info("Mapped %d unique categories", len(categories))

        save_categories_to_csv(categories, OUTPUT_CSV)
        LOGGER.info("Saved category list to %s", OUTPUT_CSV)

        return 0
    except requests.RequestException as exc:
        LOGGER.error("HTTP error while fetching directory: %s", exc)
        return 1
    except Exception as exc:
        LOGGER.exception("Unexpected error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())