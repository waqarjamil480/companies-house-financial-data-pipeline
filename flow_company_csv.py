"""
flow_company_csv.py — Flow A: Free Company Data Product pipeline.

Steps:
  1. Download the CSV ZIP files from Companies House.
  2. Extract and read each CSV.
  3. Normalise column names.
  4. Upsert rows into the companies table.
  5. Update pipeline_batches tracking.

The Free Company Data Product is split into multiple ZIP files
(BasicCompanyData-2026-02-01-part1_7.zip … part7_7.zip).
We iterate over all parts.
"""

import csv
import io
import logging
import os
import re
import zipfile
from datetime import date

import requests
from bs4 import BeautifulSoup

import db
from config import BATCH_SIZE, COMPANY_CSV_DIR, COMPANY_CSV_URL

logger = logging.getLogger(__name__)

# ── Column normalisation map ─────────────────────────────────────────────────
# Companies House CSV headers → our DB column names.
# Any column not listed here is silently ignored.
COL_MAP = {
    " CompanyNumber":               "company_number",
    "CompanyNumber":                "company_number",
    "CompanyName":                  "company_name",
    " CompanyName":                 "company_name",
    "CompanyStatus":                "company_status",
    "CompanyCategory":              "company_type",
    "RegAddress.CareOf":            "reg_address_care_of",
    "RegAddress.POBox":             "reg_address_po_box",
    "RegAddress.AddressLine1":      "reg_address_line1",
    "RegAddress.AddressLine2":      "reg_address_line2",
    "RegAddress.PostTown":          "reg_address_post_town",
    "RegAddress.County":            "reg_address_county",
    "RegAddress.Country":           "reg_address_country",
    "RegAddress.PostCode":          "reg_address_postcode",
    "IncorporationDate":            "incorporation_date",
    "DissolutionDate":              "dissolution_date",
    "Accounts.NextDueDate":         "accounts_next_due",
    "Accounts.LastMadeUpDate":      "accounts_last_made_up",
    "Returns.NextDueDate":          "returns_next_due",
    "Returns.LastMadeUpDate":       "returns_last_made_up",
    "ConfirmationStatement.NextDueDate":    "confirmation_next_due",
    "ConfirmationStatement.LastMadeUpDate": "confirmation_last_made",
    "SICCode.SicText_1":            "sic_code_1",
    "SICCode.SicText_2":            "sic_code_2",
    "SICCode.SicText_3":            "sic_code_3",
    "SICCode.SicText_4":            "sic_code_4",
    "CountryOfOrigin":              "country_of_origin",
    "Accounts.AccountCategory":     "accounts_type",
    "LegalForm":                    "legal_form",
}

# Date columns where empty strings should become None
DATE_COLS = {
    "incorporation_date", "dissolution_date",
    "accounts_next_due", "accounts_last_made_up",
    "returns_next_due", "returns_last_made_up",
    "confirmation_next_due", "confirmation_last_made",
}


def _normalise_date(val: str) -> str | None:
    """Convert 'DD/MM/YYYY' → 'YYYY-MM-DD', or return None if blank."""
    if not val or not val.strip():
        return None
    val = val.strip()
    # Try DD/MM/YYYY
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", val)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    # Already YYYY-MM-DD or similar — pass through
    return val


def _normalise_row(raw: dict, batch_label: str) -> dict | None:
    """Map raw CSV columns to DB columns and normalise values."""
    out = {}
    for csv_col, db_col in COL_MAP.items():
        if csv_col in raw:
            val = raw[csv_col]
            if val == "":
                val = None
            if db_col in DATE_COLS and val:
                val = _normalise_date(val)
            out[db_col] = val

    # company_number is mandatory
    cn = out.get("company_number")
    if not cn:
        return None

    # Ensure 8-char zero-padded TEXT
    out["company_number"] = cn.strip().zfill(8)
    out["batch_label"] = batch_label

    return out


# ── Download helpers ─────────────────────────────────────────────────────────

def _discover_csv_zip_urls(index_url: str) -> list[tuple[str, str]]:
    """
    Scrape the Companies House download page and return a list of
    (url, filename) pairs for the Free Company Data CSVs.
    """
    logger.info("Discovering CSV ZIPs from %s", index_url)
    resp = requests.get(index_url, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    base = "https://download.companieshouse.gov.uk/"
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "BasicCompanyData" in href and href.endswith(".zip"):
            filename = href.split("/")[-1]
            full_url = href if href.startswith("http") else base + filename
            urls.append((full_url, filename))

    logger.info("Found %d CSV ZIP(s)", len(urls))
    return urls


def _download_zip(url: str, dest_dir: str, filename: str) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, filename)
    if os.path.exists(dest_path):
        logger.info("Already downloaded: %s — skipping", filename)
        return dest_path
    logger.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return dest_path


def _derive_batch_label(filenames: list[str]) -> str:
    """
    Derive a snapshot label from the first filename.
    'BasicCompanyData-2026-02-01-part1_7.zip' → '2026-02'
    """
    for fn in filenames:
        m = re.search(r"(\d{4}-\d{2})-\d{2}", fn)
        if m:
            return m.group(1)
    return date.today().strftime("%Y-%m")


# ── CSV reader ───────────────────────────────────────────────────────────────

def _stream_csv_from_zip(zip_path: str, batch_label: str):
    """Yield normalised row dicts from all CSV files inside the ZIP."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        for csv_name in csv_names:
            logger.info("Reading %s / %s", zip_path, csv_name)
            with zf.open(csv_name) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace")
                reader = csv.DictReader(text)
                for raw_row in reader:
                    row = _normalise_row(raw_row, batch_label)
                    if row:
                        yield row


# ── Main flow ────────────────────────────────────────────────────────────────

def run(
    index_url: str = COMPANY_CSV_URL,
    local_zip_dir: str | None = None,
) -> None:
    """
    Execute Flow A end-to-end.

    Parameters
    ----------
    index_url     : Companies House page listing the CSV ZIPs.
    local_zip_dir : If set, read ZIPs from this directory instead of downloading.
                    Useful for re-runs without re-downloading.
    """
    zip_dir = local_zip_dir or COMPANY_CSV_DIR

    logger.info("=== Flow A — Company CSV ===")

    if local_zip_dir:
        # Use locally cached ZIPs
        zip_files = [
            (os.path.join(zip_dir, fn), fn)
            for fn in sorted(os.listdir(zip_dir))
            if fn.endswith(".zip") and "BasicCompanyData" in fn
        ]
        batch_label = _derive_batch_label([fn for _, fn in zip_files])
    else:
        # Discover and download
        pairs = _discover_csv_zip_urls(index_url)
        if not pairs:
            raise RuntimeError("No CSV ZIPs found on Companies House website.")
        batch_label = _derive_batch_label([fn for _, fn in pairs])

        zip_files = []
        for url, filename in pairs:
            path = _download_zip(url, zip_dir, filename)
            zip_files.append((path, filename))

    logger.info("Batch label: %s | ZIPs: %d", batch_label, len(zip_files))

    # ── Start tracking ───────────────────────────────────────────────────────
    source_names = ", ".join(fn for _, fn in zip_files)
    with db.get_conn() as conn:
        batch_id = db.start_batch(conn, "company", batch_label, source_names)

    try:
        rows_inserted = 0
        buf: list[dict] = []

        def _flush(force: bool = False):
            nonlocal rows_inserted
            if force or len(buf) >= BATCH_SIZE:
                with db.get_conn() as conn:
                    rows_inserted += db.upsert_companies(conn, buf)
                buf.clear()

        for zip_path, _ in zip_files:
            for row in _stream_csv_from_zip(zip_path, batch_label):
                buf.append(row)
                _flush()

            if rows_inserted % 100_000 == 0 and rows_inserted:
                logger.info("… %d rows upserted so far", rows_inserted)

        _flush(force=True)

        # ── Complete tracking ────────────────────────────────────────────────
        with db.get_conn() as conn:
            db.complete_batch(
                conn, batch_id,
                files_processed=len(zip_files),
                files_failed=0,
                rows_inserted=rows_inserted,
            )

        logger.info(
            "=== Flow A complete | rows upserted: %d ===", rows_inserted
        )

    except Exception as exc:
        logger.exception("Flow A failed: %s", exc)
        with db.get_conn() as conn:
            db.fail_batch(conn, batch_id, str(exc))
        raise
