"""
run_pipeline.py — Main pipeline entry point.

Usage examples:

    # Run both flows (company CSV first, then accounts)
    python run_pipeline.py

    # Run only Flow A (company CSV)
    python run_pipeline.py --flow company

    # Run only Flow B (accounts iXBRL)
    python run_pipeline.py --flow accounts

    # Run accounts but skip download (ZIP already present)
    python run_pipeline.py --flow accounts --skip-download

    # Override worker count
    python run_pipeline.py --workers 16

    # Force reprocess an already-completed batch (deletes old tracking row)
    python run_pipeline.py --force
"""

import argparse
import logging
import os
import sys
from datetime import datetime

import db
import flow_accounts
import flow_company_csv
from config import DATABASE_URL, LOG_DIR, WORKER_COUNT
from setup_db import SCHEMA_SQL


# ── Schema bootstrap ─────────────────────────────────────────────────────────

def _ensure_schema() -> None:
    """
    Create all tables if they don't exist yet.
    Safe to call on every run — uses CREATE TABLE IF NOT EXISTS throughout.
    """
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        logging.getLogger(__name__).info("Schema check complete (tables ready).")
    finally:
        conn.close()


# ── Logging setup ────────────────────────────────────────────────────────────

def _setup_logging(log_dir: str) -> str:
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"pipeline_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return log_file


# ── CLI ──────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(description="Companies House data pipeline")
    p.add_argument(
        "--flow",
        choices=["all", "company", "accounts"],
        default="all",
        help="Which flow to run (default: all — company CSV first, then accounts)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=WORKER_COUNT,
        help=f"Worker processes for parallel iXBRL parsing (default: {WORKER_COUNT})",
    )
    p.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading ZIPs (assume they are already present locally)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Force reprocessing even if a batch is already marked completed",
    )
    p.add_argument(
        "--local-csv-dir",
        default=None,
        help="Load company CSV ZIPs from this local directory instead of downloading",
    )
    return p.parse_args()


# ── Force helper ─────────────────────────────────────────────────────────────

def _reset_batch_if_forced(batch_type: str, batch_label: str) -> None:
    """Delete an existing tracking row so it can be reprocessed."""
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pipeline_batches WHERE batch_type=%s AND batch_label=%s",
                (batch_type, batch_label),
            )
            if cur.rowcount:
                logging.getLogger(__name__).warning(
                    "--force: deleted existing tracking row for %s / %s",
                    batch_type, batch_label,
                )


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    args = _parse_args()
    log_file = _setup_logging(LOG_DIR)
    logger = logging.getLogger(__name__)

    logger.info("Pipeline started | flow=%s | workers=%d", args.flow, args.workers)
    logger.info("Log file: %s", log_file)

    # Initialise DB connection pool
    db.init_pool(minconn=2, maxconn=args.workers + 4)

    # Auto-create tables if this is a first run
    _ensure_schema()

    try:
        run_company = args.flow in ("all", "company")
        run_accounts = args.flow in ("all", "accounts")

        # ── Flow A — Company CSV ─────────────────────────────────────────────
        if run_company:
            logger.info("Starting Flow A — Company CSV")
            try:
                flow_company_csv.run(local_zip_dir=args.local_csv_dir)
            except ValueError as exc:
                if "already completed" in str(exc) and not args.force:
                    logger.warning(
                        "Flow A skipped: %s. Use --force to reprocess.", exc
                    )
                else:
                    raise

        # ── Flow B — Accounts iXBRL ──────────────────────────────────────────
        if run_accounts:
            logger.info("Starting Flow B — Accounts iXBRL")
            failed_log = os.path.join(
                LOG_DIR,
                f"failed_accounts_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log",
            )
            try:
                flow_accounts.run(
                    skip_download=args.skip_download,
                    workers=args.workers,
                    failed_log_path=failed_log,
                )
            except ValueError as exc:
                if "already completed" in str(exc) and not args.force:
                    logger.warning(
                        "Flow B skipped: %s. Use --force to reprocess.", exc
                    )
                else:
                    raise

        logger.info("Pipeline finished successfully.")

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user.")
        sys.exit(1)
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        sys.exit(1)
    finally:
        db.close_pool()


if __name__ == "__main__":
    # Guard required for multiprocessing on Windows / macOS
    main()
