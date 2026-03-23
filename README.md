# Algerian B2B Lead Generation Engine

Open Minds Club — B2BxIT (Python + Google Sheets)

## Summary

A small, phase-based pipeline to discover Algerian business categories, harvest company profile URLs, extract contact/company details, and sync cleaned results into Google Sheets.

## Requirements

- Python 3
- Google Cloud Service Account with access to your target Google Sheet

Install dependencies:

```bash
pip install requests beautifulsoup4 gspread
```

## Configuration (no secrets in repo)

Set environment variables:

- `GOOGLE_APPLICATION_CREDENTIALS` — path to your Service Account JSON key
- `SHEETS_SPREADSHEET_NAME` — your spreadsheet name (e.g., `Algerian Business Leads`)

macOS/Linux:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/service-account.json"
export SHEETS_SPREADSHEET_NAME="Algerian Business Leads"
```

Windows PowerShell:

```powershell
setx GOOGLE_APPLICATION_CREDENTIALS "C:\path\to\service-account.json"
setx SHEETS_SPREADSHEET_NAME "Algerian Business Leads"
```

## Scripts

- `phase1-category.py` → builds `master_categories_list.csv`
- `phase2-harvester.py` → builds `sponsor_leads_urls.csv`
- `phase3-extractor.py` → extracts details from profiles and appends rows to Google Sheets
- `phase4-monthlyUpdater.py` → backs up the sheet to `backups/`, finds new companies, appends only new rows

## Run

```bash
python phase1-category.py
python phase2-harvester.py
python phase3-extractor.py
python phase4-monthlyUpdater.py
```

## Notes

- Requests are intentionally rate-limited. Removing delays can increase the risk of blocking.
- Keep Google credentials out of GitHub (use environment variables and `.gitignore`).
