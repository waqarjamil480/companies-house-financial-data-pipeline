"""
config.py — Central configuration for the Companies House pipeline.
Edit these values before running. All paths can be absolute or relative.
"""

import os

# ── PostgreSQL connection ────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5433"))
DB_NAME     = os.getenv("DB_NAME",     "companies_house")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── File paths ───────────────────────────────────────────────────────────────
# Root directory where ZIPs and extracted files land
DATA_DIR           = os.getenv("DATA_DIR",    "./data")

# Sub-directories (created automatically by the pipeline)
ACCOUNTS_ZIP_DIR   = os.path.join(DATA_DIR, "accounts_zips")
ACCOUNTS_EXTRACT   = os.path.join(DATA_DIR, "accounts_extracted")
COMPANY_CSV_DIR    = os.path.join(DATA_DIR, "company_csv")

# ── Processing ───────────────────────────────────────────────────────────────
# Number of worker processes for parallel iXBRL parsing
WORKER_COUNT       = int(os.getenv("WORKER_COUNT", "8"))

# Rows flushed to the DB in a single INSERT statement
BATCH_SIZE         = int(os.getenv("BATCH_SIZE", "500"))

# ── Companies House URLs ─────────────────────────────────────────────────────
ACCOUNTS_INDEX_URL = "https://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
COMPANY_CSV_URL    = "https://download.companieshouse.gov.uk/en_output.html"

# The specific monthly accounts file we download for this assessment.
# In production the cron job discovers this dynamically from ACCOUNTS_INDEX_URL.
ACCOUNTS_ZIP_FILENAME = "Accounts_Monthly_Data-February2026.zip"
ACCOUNTS_ZIP_URL = (
    "https://download.companieshouse.gov.uk/"
    + ACCOUNTS_ZIP_FILENAME
)

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_DIR = os.getenv("LOG_DIR", "./logs")
