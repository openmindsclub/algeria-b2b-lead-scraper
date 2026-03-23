import csv
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import gspread
import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
REQUEST_TIMEOUT_SECONDS = 15

INPUT_LEADS_CSV = "sponsor_leads_urls.csv"

SLEEP_MIN_SECONDS = 2.0
SLEEP_MAX_SECONDS = 4.0

ALGIERS_ONLY_CATEGORIES = {"Catering", "Food_Drink", "Printing", "Take_aways"}


@dataclass(frozen=True)
class Lead:
    category: str
    company_name: str
    profile_url: str


def load_leads(filename: str) -> List[Lead]:
    with open(filename, "r", encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    leads: List[Lead] = []
    for row in rows:
        category = (row.get("Category") or "Unknown").strip()
        name = (row.get("Company Name") or "Unknown").strip()
        url = (row.get("Profile URL") or "").strip()
        if not url:
            continue
        leads.append(Lead(category=category, company_name=name, profile_url=url))

    return leads


def connect_google_sheet() -> gspread.Worksheet:
    """
    Auth options (recommended):
      - Set GOOGLE_APPLICATION_CREDENTIALS to the service account JSON path.
      - Set SHEETS_SPREADSHEET_NAME to your spreadsheet name.
    """
    spreadsheet_name = os.environ.get("SHEETS_SPREADSHEET_NAME", "").strip()
    if not spreadsheet_name:
        raise RuntimeError("Missing env var: SHEETS_SPREADSHEET_NAME")

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path:
        raise RuntimeError("Missing env var: GOOGLE_APPLICATION_CREDENTIALS")

    gc = gspread.service_account(filename=creds_path)
    spreadsheet = gc.open(spreadsheet_name)
    return spreadsheet.sheet1


def get_text_next_to_label(soup: BeautifulSoup, label_keywords: List[str]) -> str:
    """
    Finds text near a label like "Address", "Téléphone", etc.
    Returns a cleaned string or "".
    """
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

    # Simple heuristic: many entries are comma-separated.
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


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    today_date = datetime.now().strftime("%Y-%m-%d")

    try:
        worksheet = connect_google_sheet()
        LOGGER.info("Connected to Google Sheets.")
    except Exception as exc:
        LOGGER.error("Could not connect to Google Sheets: %s", exc)
        return 1

    try:
        leads = load_leads(INPUT_LEADS_CSV)
        LOGGER.info("Loaded %d leads from %s", len(leads), INPUT_LEADS_CSV)
    except FileNotFoundError:
        LOGGER.error("Missing input file %s. Run Phase 2 first.", INPUT_LEADS_CSV)
        return 1

    headers = {"User-Agent": USER_AGENT}
    total_injected = 0
    total_skipped = 0
    total_failed = 0

    with requests.Session() as session:
        session.headers.update(headers)

        for index, lead in enumerate(leads, start=1):
            LOGGER.info("[%d/%d] Inspecting: %s", index, len(leads), lead.company_name)

            try:
                data = scrape_profile(session, lead)

                if should_skip_bouncer(lead.category, data["city"], data["phones"]):
                    LOGGER.info(
                        "Skipped by bouncer: category=%s city=%s phones_present=%s",
                        lead.category,
                        data["city"],
                        bool(data["phones"]),
                    )
                    total_skipped += 1
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
                total_injected += 1
                LOGGER.info("Injected row into Google Sheets.")

            except Exception as exc:
                total_failed += 1
                LOGGER.error("Failed inspecting %s: %s", lead.company_name, exc)

            time.sleep(random.uniform(SLEEP_MIN_SECONDS, SLEEP_MAX_SECONDS))

    LOGGER.info(
        "Phase 3 complete. injected=%d skipped=%d failed=%d",
        total_injected,
        total_skipped,
        total_failed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())