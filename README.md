# Public Job Listings Scraper (IBM Careers)

Professional, reusable web scraping project focused on extracting **public job listings**
from company career pages and delivering **clean, normalized datasets**.

This project is designed as:
- a realistic **freelance deliverable**
- a **public-safe** repository
- a **reusable template** for similar scraping projects

---

## 🎯 Problem this project solves

Recruiters, analysts, and researchers often need to:
- track job openings over time
- analyze hiring trends by location or role
- consolidate job listings into structured datasets

Manually collecting this information is slow and error-prone.

This scraper automates the process by:
- extracting **publicly available job postings**
- validating and normalizing the data
- exporting a clean dataset ready for analysis (CSV)

---

## 🌐 Data source & legal considerations

### Source
- **IBM Careers** (global)
- Data is retrieved from a **public search endpoint** used by the IBM Careers frontend.

### Access characteristics
- No authentication required
- No private or user-specific data
- Same endpoint accessed by a normal browser session

### Robots & terms
- Scraping is performed with an identifiable User-Agent
- Requests are rate-limited and bounded
- Only publicly accessible endpoints are used
- No attempt is made to bypass access controls

> This project is intended for **educational, analytical, and research purposes** using public data only.

---

## 🏗️ Architecture overview

Extract → Raw JSON → Transform → Clean Schema → CSV Output


### Pipeline stages

1. **Extract**
   - Calls the public search API
   - Supports pagination
   - Retries with exponential backoff
   - Saves immutable raw JSON responses

2. **Transform**
   - Maps raw API records to a canonical schema
   - Validates data with Pydantic
   - Deduplicates records by natural key

3. **Load**
   - Writes a clean CSV dataset
   - One output per execution date

---

## 📁 Project structure

src/
scraper/
main.py # Pipeline entrypoint
config.py # Environment-based configuration
extract.py # HTTP extraction + retries
transform.py # Validation & normalization
mappers.py # API → schema mapping
models.py # Canonical data model (JobPostingV1)
load.py # CSV output

data/
raw/ # Raw JSON responses (per page, per run)
processed/ # Clean CSV outputs

tests/
test_mappers.py
test_dedupe_key.py

logs/


---

## 📦 Output contract

### Raw data
Immutable JSON responses saved per execution and per page:

data/raw/ibm_careers/run_date=YYYY-MM-DD/search_page=001.json


Used for:
- debugging
- reprocessing without re-scraping
- auditability

### Processed data
Clean, deduplicated CSV:

data/processed/ibm_careers/run_date=YYYY-MM-DD/job_postings_v1.csv

Schema: `JobPostingV1`

Key fields:
- `external_id`
- `title`
- `company`
- `job_url`
- `location_*`
- `scraped_at`

---

## ⚙️ Configuration

All runtime behavior is controlled via environment variables.

Example `.env` (local only):

ENV=development

RAW_DATA_PATH=data/raw
PROCESSED_DATA_PATH=data/processed
LOG_PATH=logs

REQUEST_TIMEOUT=30
RETRY_ATTEMPTS=3
USER_AGENT=public-data-scraper/1.0

PAGE_SIZE=30
MAX_PAGES=3


`.env` is **not committed**.  
`.env.example` documents the contract.

---

## ▶️ How to run (WSL / Linux)

```bash
# activate virtual environment
source .venv/bin/activate

# load env vars
set -a
source .env
set +a

# run pipeline
python3 -m src.scraper

At the end of execution, you should see:

raw files saved under data/raw/

a CSV dataset under data/processed/

🧪 Tests

Basic unit tests validate:

job ID extraction

API record → schema mapping

deduplication key stability

Run tests with:

pytest -q

🚀 Possible extensions

This project is intentionally scoped, but easy to extend:

Add support for multiple companies

Store data in SQLite or Postgres

Schedule periodic runs (cron / Airflow)

Add trend analysis or dashboards

Version schema (JobPostingV2) for new fields

📌 Notes

This repository contains no credentials

No scraping of private or authenticated pages

Designed to be safe for public GitHub hosting