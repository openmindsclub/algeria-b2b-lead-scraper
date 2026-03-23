import csv
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import gspread
import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)

BASE_URL = "https://www.algeriayp.com"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

INPUT_CATEGORIES_CSV = "master_categories_list.csv"
BACKUP_DIR = "backups"

REQUEST_TIMEOUT_SECONDS = 15

DELTA_MAX_PAGES_PER_CATEGORY = 20
DELTA_SLEEP_MIN_SECONDS = 1.0
DELTA_SLEEP_MAX_SECONDS = 2.0

SCRAPE_SLEEP_MIN_SECONDS = 2.0
SCRAPE_SLEEP_MAX_SECONDS = 4.0

# Categories that must be in Algiers to be useful
ALGIERS_ONLY_CATEGORIES = {"Catering", "Food_Drink", "Printing", "Take_aways"}

# Assumption: Profile URL is column 13 in the Google Sheet (1-based).
PROFILE_URL_COL_INDEX_1BASED = 13


@dataclass(frozen=True)
class Lead:
    category: str
    company_name: str
    profile_url: str


def connect_google_sheet() -> gspread.Worksheet:
    spreadsheet_name = os.environ.get("SHEETS_SPREADSHEET_NAME", "").strip()
    if not spreadsheet_name:
        raise RuntimeError("Missing env var: SHEETS_SPREADSHEET_NAME")

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path:
        raise RuntimeError("Missing env var: GOOGLE_APPLICATION_CREDENTIALS")

    gc = gspread.service_account(filename=creds_path)
    spreadsheet = gc.open(spreadsheet_name)
    return spreadsheet.sheet1


def backup_sheet_to_csv(all_records: List[List[str]]) -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_filename = os.path.join(BACKUP_DIR, f"database_backup_{timestamp}.csv")

    with open(backup_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(all_records)

    return backup_filename


def build_existing_url_bank(all_records: List[List[str]]) -> Set[str]:
    existing_urls: Set[str] = set()
    idx0 = PROFILE_URL_COL_INDEX_1BASED - 1

    for row in all_records:
        if len(row) <= idx0:
            continue
        url = (row[idx0] or "").strip()
        if url.startswith("http"):
            existing_urls.add(url)

    return existing_urls


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
    soup = BeautifulSoup(html, "html.parser")
    company_boxes = soup.find_all("div", class_="company")

    results: List[Tuple[str, str]] = []
    for box in company_boxes:
        a = box.find("a")
        if not a:
            continue
        name = a.get_text(strip=True)
        href = (a.get("href") or "").strip()
        if not name or not href:
            continue
        results.append((name, href))
    return results


def delta_scan_new_leads(
    session: requests.Session,
    categories: List[str],
    existing_urls: Set[str],
) -> List[Lead]:
    new_leads: List[Lead] = []

    for category in categories:
        page_number = 1
        seen_on_this_run: Set[str] = set()

        while True:
            if page_number > DELTA_MAX_PAGES_PER_CATEGORY:
                break

            url = build_category_page_url(category, page_number)

            try:
                resp = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
                if resp.status_code != 200:
                    break

                companies = parse_company_boxes(resp.text)
                if not companies:
                    break

                new_companies_on_page = 0
                for name, href in companies:
                    profile_url = href if href.startswith("http") else f"{BASE_URL}{href}"

                    # Delta check
                    if profile_url in existing_urls or profile_url in seen_on_this_run:
                        continue

                    seen_on_this_run.add(profile_url)
                    existing_urls.add(profile_url)

                    new_leads.append(Lead(category=category, company_name=name, profile_url=profile_url))
                    new_companies_on_page += 1

                # If no new companies on this page, assume rest is old
                if new_companies_on_page == 0:
                    break

                # Natural end
                if len(companies) < 10:
                    break

                page_number += 1
                time.sleep(random.uniform(DELTA_SLEEP_MIN_SECONDS, DELTA_SLEEP_MAX_SECONDS))

            except requests.RequestException as exc:
                LOGGER.warning("Delta scan request error category=%s page=%d: %s", category, page_number, exc)
                break

    return new_leads


def get_text_next_to_label(soup: BeautifulSoup, label_keywords: List[str]) -> str:
    if not soup.body:
        return ""

    pattern = r"(" + "|".join(map(re.escape, label_keywords)) + r")"
    label_tag = soup.body.find(string=re.compile(pattern, re.IGNORECASE))
    if not (label_tag and label_tag.parent and label_tag.parent.parent):
        return ""

    full_block = label_tag.parent.parent.get_text(" ", strip=True)
    clean_text = re.sub(pattern, "", full_block, flags=re.IGNORECASE)
    clean_text = clean_text.replace(":", " ").strip()
    clean_text = re.sub(r"\s+", " ", clean_text).strip()
    return clean_text


def parse_city_from_address(full_address: str) -> str:
    if not full_address or full_address == "No address":
        return "Unknown"
    if "," in full_address:
        parts = [p.strip() for p in full_address.split(",") if p.strip()]
        if len(parts) >= 2:
            return parts[-2]
        return parts[-1] if parts else "Unknown"
    return full_address.strip() or "Unknown"


def is_valid_website(website: str) -> bool:
    if not website:
        return False
    website = website.strip()
    if len(website) > 100:
        return False
    if " " in website:
        return False
    if "." not in website:
        return False
    return True


def should_skip_bouncer(category: str, city: str, phones: str) -> bool:
    if category not in ALGIERS_ONLY_CATEGORIES:
        return False

    city_lower = (city or "").lower()
    is_algiers = ("alger" in city_lower) or ("algiers" in city_lower)
    if not is_algiers:
        return True

    if not phones:
        return True

    return False


def scrape_profile(session: requests.Session, lead: Lead) -> Dict[str, str]:
    resp = session.get(lead.profile_url, timeout=REQUEST_TIMEOUT_SECONDS)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to load profile (status={resp.status_code})")

    soup = BeautifulSoup(resp.text, "html.parser")

    full_address = get_text_next_to_label(soup, ["Address", "Adresse"]) or "No address"
    full_address = (
        full_address.replace("View MapGet Directions", "")
        .replace("Show Map", "")
        .strip()
    )

    city = parse_city_from_address(full_address)

    phone_tags = soup.find_all("a", href=lambda x: x and x.startswith("tel:"))
    phones = {p.get_text(strip=True).replace(" ", "") for p in phone_tags if p.get_text(strip=True)}
    phones_str = ", ".join(sorted(phones))

    if not phones_str:
        phones_str = get_text_next_to_label(
            soup,
            ["Contact number", "Mobile phone", "Téléphone", "Numéro", "Phone"],
        )

    email_tag = soup.find("a", href=lambda x: x and x.startswith("mailto:"))
    email_raw = ""
    if email_tag:
        href = (email_tag.get("href") or "").strip()
        email_raw = href.replace("mailto:", "").split("?")[0].strip()

    if not email_raw:
        email_raw = get_text_next_to_label(soup, ["E-mail address", "Email", "Courriel"])
        if "Send Enquiry" in email_raw or "Envoyer" in email_raw:
            email_raw = "Hidden (use website contact form)"
        elif "@" not in email_raw:
            email_raw = ""

    website_tag = soup.find("a", string=lambda x: x and "www." in str(x).lower())
    website = website_tag.get_text(strip=True) if website_tag else ""
    if not website:
        website = get_text_next_to_label(soup, ["Website address", "Site web"])
    if not is_valid_website(website):
        website = ""

    manager = get_text_next_to_label(soup, ["Manager", "Contact Person", "Directeur", "Gérant"])
    if not manager or len(manager) > 50:
        manager = "Not Listed"

    est_year_raw = get_text_next_to_label(soup, ["Establishment year", "Année de création", "Fondé"])
    est_year = "".join(ch for ch in est_year_raw if ch.isdigit())
    if not est_year or len(est_year) != 4:
        est_year = "Not Listed"

    return {
        "full_address": full_address,
        "city": city,
        "phones": phones_str.strip(),
        "email": email_raw.strip(),
        "website": website.strip(),
        "manager": manager.strip(),
        "est_year": est_year.strip(),
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    LOGGER.info("Phase 4: Monthly delta updater")

    try:
        worksheet = connect_google_sheet()
        LOGGER.info("Connected to Google Sheets.")
    except Exception as exc:
        LOGGER.error("Database connection failed: %s", exc)
        return 1

    # Download and backup database
    all_records = worksheet.get_all_values()
    LOGGER.info("Downloaded %d rows from the sheet.", len(all_records))

    backup_path = backup_sheet_to_csv(all_records)
    LOGGER.info("Backup written to %s", backup_path)

    existing_urls = build_existing_url_bank(all_records)
    LOGGER.info("Memory bank loaded: %d existing profile URLs", len(existing_urls))

    # Delta scan
    try:
        categories = load_categories(INPUT_CATEGORIES_CSV)
    except FileNotFoundError:
        LOGGER.error("Missing %s. Run Phase 1 first.", INPUT_CATEGORIES_CSV)
        return 1

    headers = {"User-Agent": USER_AGENT}
    today_date = datetime.now().strftime("%Y-%m-%d")

    with requests.Session() as session:
        session.headers.update(headers)

        LOGGER.info("Starting delta scan for new companies...")
        new_leads = delta_scan_new_leads(session, categories, existing_urls)
        LOGGER.info("Delta scan complete: %d new companies found", len(new_leads))

        if not new_leads:
            LOGGER.info("No new companies found. Database is up to date.")
            return 0

        injected = 0
        skipped = 0
        failed = 0

        LOGGER.info("Extracting deep data and injecting new leads...")

        for idx, lead in enumerate(new_leads, start=1):
            LOGGER.info("[%d/%d] Processing: %s", idx, len(new_leads), lead.company_name)

            try:
                data = scrape_profile(session, lead)

                if should_skip_bouncer(lead.category, data["city"], data["phones"]):
                    skipped += 1
                    continue

                opportunity = "Needs Website" if not data["website"] else "Has Website"

                row_data = [
                    today_date,
                    lead.company_name,
                    data["manager"],
                    data["est_year"],
                    lead.category,
                    data["city"],
                    data["phones"],
                    data["email"],
                    data["full_address"],
                    data["website"],
                    opportunity,
                    "Not Contacted",
                    lead.profile_url,
                ]

                worksheet.append_row(row_data)
                injected += 1

            except Exception as exc:
                failed += 1
                LOGGER.warning("Failed processing %s: %s", lead.company_name, exc)

            time.sleep(random.uniform(SCRAPE_SLEEP_MIN_SECONDS, SCRAPE_SLEEP_MAX_SECONDS))

        LOGGER.info("Monthly update complete. injected=%d skipped=%d failed=%d", injected, skipped, failed)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())