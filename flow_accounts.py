"""
flow_accounts.py — Flow B: iXBRL accounts pipeline.

Steps:
  1. Download the monthly accounts ZIP from Companies House.
  2. Extract HTML files to a temporary directory.
  3. Parse each file in parallel using test_pipeline.parse_one_file().
  4. Write results (financials, directors, reports) to PostgreSQL.
  5. Delete extracted HTML files (keep the ZIP).
  6. Update the pipeline_batches tracking table.
"""

import logging
import multiprocessing
import os
import shutil
import zipfile
from datetime import date

import requests

import db
from config import (
    ACCOUNTS_EXTRACT,
    ACCOUNTS_ZIP_DIR,
    ACCOUNTS_ZIP_URL,
    ACCOUNTS_ZIP_FILENAME,
    BATCH_SIZE,
    WORKER_COUNT,
)
from test_pipeline import parse_one_file

logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _derive_batch_label(zip_filename: str) -> str:
    """
    'Accounts_Monthly_Data-February2026.zip' → 'Feb-26'
    Falls back to the raw filename if pattern doesn't match.
    """
    MONTH_MAP = {
        "january": "Jan", "february": "Feb", "march": "Mar",
        "april": "Apr", "may": "May", "june": "Jun",
        "july": "Jul", "august": "Aug", "september": "Sep",
        "october": "Oct", "november": "Nov", "december": "Dec",
    }
    name = zip_filename.replace(".zip", "").lower()
    # e.g. "accounts_monthly_data-february2026"
    for month_long, month_short in MONTH_MAP.items():
        if month_long in name:
            # Extract the 4-digit year
            year_part = "".join(c for c in name.split(month_long)[-1] if c.isdigit())
            if len(year_part) >= 4:
                yy = year_part[2:4]
                return f"{month_short}-{yy}"
    return zip_filename


def _is_valid_zip(path: str) -> bool:
    """Return True only if the file exists and is a valid ZIP archive."""
    if not os.path.exists(path):
        return False
    try:
        with zipfile.ZipFile(path, "r") as zf:
            bad = zf.testzip()   # returns None if all files are OK
            return bad is None
    except zipfile.BadZipFile:
        return False


def _download_zip(url: str, dest_dir: str, filename: str) -> str:
    """Stream-download a ZIP, skipping if already present and valid."""
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, filename)

    if _is_valid_zip(dest_path):
        logger.info("ZIP already downloaded and valid: %s — skipping download", dest_path)
        return dest_path

    if os.path.exists(dest_path):
        logger.warning(
            "Existing file is corrupt or incomplete — deleting and re-downloading: %s",
            dest_path,
        )
        os.remove(dest_path)

    logger.info("Downloading %s → %s", url, dest_path)
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):  # 1 MB chunks
                f.write(chunk)

    # Validate what we just downloaded
    if not _is_valid_zip(dest_path):
        os.remove(dest_path)
        raise RuntimeError(f"Downloaded file failed ZIP validation: {dest_path}")

    logger.info("Download complete and verified: %s", dest_path)
    return dest_path


def _extract_zip(zip_path: str, extract_dir: str) -> list[str]:
    """Extract all HTML files from the ZIP, return list of paths."""
    os.makedirs(extract_dir, exist_ok=True)
    logger.info("Extracting %s → %s", zip_path, extract_dir)

    with zipfile.ZipFile(zip_path, "r") as zf:
        html_names = [
            n for n in zf.namelist()
            if n.lower().endswith((".html", ".xhtml", ".xml"))
        ]
        zf.extractall(extract_dir, members=html_names)

    paths = [os.path.join(extract_dir, n) for n in html_names]
    logger.info("Extracted %d HTML files", len(paths))
    return paths


# ── Worker (top-level function so multiprocessing can pickle it) ─────────────

def _parse_worker(filepath: str) -> dict:
    """
    Parse a single iXBRL file.
    Returns a result dict with 'ok': True/False.
    Never raises — errors are captured so the pool keeps running.
    """
    try:
        result = parse_one_file(filepath)
        result["ok"] = True
        return result
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "source_file": os.path.basename(filepath),
            "error": str(exc),
        }


# ── Batch DB write ────────────────────────────────────────────────────────────

def _flush_to_db(conn, financials, directors, reports, metadata) -> int:
    """Write accumulated rows to DB and return total rows inserted."""
    total = 0
    if financials:
        total += db.insert_financials(conn, financials)
    if directors:
        total += db.upsert_directors(conn, directors)
    if reports:
        total += db.upsert_reports(conn, reports)
    if metadata:
        total += db.upsert_metadata(conn, metadata)
    return total


# ── Main flow ────────────────────────────────────────────────────────────────

def run(
    zip_url: str = ACCOUNTS_ZIP_URL,
    zip_filename: str = ACCOUNTS_ZIP_FILENAME,
    skip_download: bool = False,
    workers: int = WORKER_COUNT,
    failed_log_path: str | None = None,
) -> None:
    """
    Execute Flow B end-to-end.

    Parameters
    ----------
    zip_url         : URL of the accounts ZIP to download.
    zip_filename    : Local filename for the ZIP.
    skip_download   : If True, assume the ZIP is already in ACCOUNTS_ZIP_DIR.
    workers         : Number of parallel parser processes.
    failed_log_path : Path to write a list of files that failed to parse.
    """
    batch_label = _derive_batch_label(zip_filename)
    extract_dir = os.path.join(ACCOUNTS_EXTRACT, batch_label.replace("-", "_"))

    logger.info("=== Flow B — Accounts | batch: %s ===", batch_label)

    # ── 1. Start batch tracking ──────────────────────────────────────────────
    with db.get_conn() as conn:
        batch_id = db.start_batch(conn, "accounts", batch_label, zip_filename)
    logger.info("Batch tracking started: id=%d", batch_id)

    try:
        # ── 2. Download ──────────────────────────────────────────────────────
        if skip_download:
            zip_path = os.path.join(ACCOUNTS_ZIP_DIR, zip_filename)
            logger.info("Skipping download — using existing ZIP: %s", zip_path)
        else:
            zip_path = _download_zip(zip_url, ACCOUNTS_ZIP_DIR, zip_filename)

        # ── 3. Extract ───────────────────────────────────────────────────────
        html_paths = _extract_zip(zip_path, extract_dir)
        total_files = len(html_paths)
        logger.info("Total files to process: %d", total_files)

        # ── 4. Parse in parallel + write in batches ──────────────────────────
        files_processed = 0
        files_failed = 0
        rows_inserted = 0
        failed_files = []

        # Accumulation buffers — flushed every BATCH_SIZE results
        buf_financials: list[dict] = []
        buf_directors: list[dict] = []
        buf_reports: list[dict] = []
        buf_metadata: list[dict] = []

        def _maybe_flush(force: bool = False):
            nonlocal rows_inserted
            threshold = BATCH_SIZE
            if force or (
                len(buf_financials) + len(buf_directors) + len(buf_reports) + len(buf_metadata) >= threshold
            ):
                with db.get_conn() as conn:
                    rows_inserted += _flush_to_db(
                        conn, buf_financials, buf_directors, buf_reports, buf_metadata
                    )
                buf_financials.clear()
                buf_directors.clear()
                buf_reports.clear()
                buf_metadata.clear()

        with multiprocessing.Pool(processes=workers) as pool:
            for result in pool.imap_unordered(_parse_worker, html_paths, chunksize=10):
                if not result.get("ok"):
                    files_failed += 1
                    failed_files.append(
                        f"{result.get('source_file', '?')} — {result.get('error', '')}"
                    )
                    logger.warning(
                        "Parse failed: %s — %s",
                        result.get("source_file"),
                        result.get("error"),
                    )
                    continue

                files_processed += 1
                buf_financials.extend(result.get("financials", []))
                buf_directors.extend(result.get("directors", []))
                buf_reports.extend(result.get("text_sections", []))
                buf_metadata.extend(result.get("metadata", []))

                _maybe_flush()

                if files_processed % 5000 == 0:
                    logger.info(
                        "Progress: %d/%d processed, %d failed",
                        files_processed, total_files, files_failed,
                    )

        # Flush any remaining rows
        _maybe_flush(force=True)

        # ── 5. Cleanup: delete extracted HTML files ──────────────────────────
        logger.info("Cleaning up extracted HTML files in %s", extract_dir)
        shutil.rmtree(extract_dir, ignore_errors=True)
        logger.info("Cleanup complete — ZIP retained at %s", zip_path)

        # ── 6. Write failed-files log ────────────────────────────────────────
        if failed_files:
            log_path = failed_log_path or os.path.join(
                "logs", f"failed_{batch_label.replace('-', '_')}.log"
            )
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, "w") as fh:
                fh.write("\n".join(failed_files))
            logger.info(
                "Failed files (%d) logged to: %s", len(failed_files), log_path
            )

        # ── 7. Complete batch tracking ───────────────────────────────────────
        with db.get_conn() as conn:
            db.complete_batch(
                conn, batch_id, files_processed, files_failed, rows_inserted
            )

        logger.info(
            "=== Flow B complete | processed=%d failed=%d rows=%d ===",
            files_processed, files_failed, rows_inserted,
        )

    except Exception as exc:
        logger.exception("Flow B failed: %s", exc)
        with db.get_conn() as conn:
            db.fail_batch(conn, batch_id, str(exc))
        raise
