import csv
import logging
import random
import time
from dataclasses import dataclass
from typing import Iterable, List, Set, Tuple

import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)

BASE_URL = "https://www.algeriayp.com"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

INPUT_CATEGORIES_CSV = "master_categories_list.csv"
OUTPUT_LEADS_CSV = "sponsor_leads_urls.csv"

REQUEST_TIMEOUT_SECONDS = 15
MAX_PAGES_PER_CATEGORY = 50

SLEEP_MIN_SECONDS = 1.5
SLEEP_MAX_SECONDS = 3.0


@dataclass(frozen=True)
class Lead:
    category: str
    company_name: str
    profile_url: str


def load_categories(filename: str) -> List[str]:
    categories: List[str] = []
    with open(filename, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            slug = (row[0] or "").strip()
            if slug:
                categories.append(slug)
    return categories


def build_category_page_url(category_slug: str, page_number: int) -> str:
    if page_number <= 1:
        return f"{BASE_URL}/category/{category_slug}"
    return f"{BASE_URL}/category/{category_slug}/{page_number}"


def parse_company_boxes(html: str) -> List[Tuple[str, str]]:
    """
    Returns list of (company_name, profile_path).
    profile_path is expected to be a path like "/company/...."
    """
    soup = BeautifulSoup(html, "html.parser")
    company_boxes = soup.find_all("div", class_="company")

    results: List[Tuple[str, str]] = []
    for box in company_boxes:
        link_tag = box.find("a")
        if not link_tag:
            continue

        name = link_tag.get_text(strip=True)
        href = (link_tag.get("href") or "").strip()
        if not name or not href:
            continue

        results.append((name, href))
    return results


def harvest_category(
    session: requests.Session,
    category_slug: str,
    seen_urls: Set[str],
) -> Iterable[Lead]:
    page_number = 1

    while True:
        # Limit switch 1: hard cap
        if page_number > MAX_PAGES_PER_CATEGORY:
            LOGGER.warning("Hard limit reached; stopping category=%s", category_slug)
            return

        url = build_category_page_url(category_slug, page_number)
        LOGGER.info("Scanning category=%s page=%d url=%s", category_slug, page_number, url)

        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            if resp.status_code != 200:
                LOGGER.info("Non-200 response (%d); ending category=%s", resp.status_code, category_slug)
                return

            companies = parse_company_boxes(resp.text)

            # Limit switch 2: empty page sensor
            if not companies:
                LOGGER.info("Empty page; ending category=%s page=%d", category_slug, page_number)
                return

            new_companies_on_page = 0

            for name, href in companies:
                profile_url = href if href.startswith("http") else f"{BASE_URL}{href}"

                # Limit switch 3 support: avoid pagination loops by de-duping
                if profile_url in seen_urls:
                    continue

                seen_urls.add(profile_url)
                new_companies_on_page += 1
                yield Lead(category=category_slug, company_name=name, profile_url=profile_url)

            # Limit switch 3: duplicate loop sensor
            if new_companies_on_page == 0:
                LOGGER.warning(
                    "Loop detected (no new companies); ending category=%s page=%d",
                    category_slug,
                    page_number,
                )
                return

            # Limit switch 4: natural end sensor
            if len(companies) < 10:
                LOGGER.info(
                    "Natural end reached (%d companies on page); ending category=%s",
                    len(companies),
                    category_slug,
                )
                return

            page_number += 1
            time.sleep(random.uniform(SLEEP_MIN_SECONDS, SLEEP_MAX_SECONDS))

        except requests.RequestException as exc:
            LOGGER.error("Request error category=%s page=%d: %s", category_slug, page_number, exc)
            return
        except Exception:
            LOGGER.exception("Unexpected error category=%s page=%d", category_slug, page_number)
            return


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        categories = load_categories(INPUT_CATEGORIES_CSV)
    except FileNotFoundError:
        LOGGER.error("Input file not found: %s. Run Phase 1 first.", INPUT_CATEGORIES_CSV)
        return 1

    LOGGER.info("Phase 2: Harvester")
    LOGGER.info("Loaded %d categories from %s", len(categories), INPUT_CATEGORIES_CSV)

    headers = {"User-Agent": USER_AGENT}
    total_leads_found = 0

    with requests.Session() as session:
        session.headers.update(headers)

        with open(OUTPUT_LEADS_CSV, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Category", "Company Name", "Profile URL"])

            for category in categories:
                seen_urls: Set[str] = set()

                for lead in harvest_category(session, category, seen_urls):
                    writer.writerow([lead.category, lead.company_name, lead.profile_url])
                    total_leads_found += 1

    LOGGER.info(
        "Harvest complete. Extracted %d total leads. Output written to %s",
        total_leads_found,
        OUTPUT_LEADS_CSV,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())