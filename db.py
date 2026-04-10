"""
db.py — Database helpers: connection pool, batch writers, upsert logic.

All write functions accept lists of dicts and handle batching internally.
The caller never touches raw SQL; this module owns all DB interactions.
"""

import logging
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from config import DATABASE_URL, BATCH_SIZE

logger = logging.getLogger(__name__)

# Module-level connection pool — initialised once per process
_pool: ThreadedConnectionPool | None = None


def init_pool(minconn: int = 1, maxconn: int = 20) -> None:
    """Initialise the connection pool. Call once at startup."""
    global _pool
    _pool = ThreadedConnectionPool(minconn, maxconn, DATABASE_URL)
    logger.info("DB connection pool initialised (min=%d, max=%d)", minconn, maxconn)


def close_pool() -> None:
    if _pool:
        _pool.closeall()


@contextmanager
def get_conn():
    """Context manager that borrows a connection from the pool."""
    if _pool is None:
        raise RuntimeError("Call init_pool() before get_conn()")
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


# ── Low-level helpers ────────────────────────────────────────────────────────

def _chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def _execute_values(conn, sql: str, rows: list[tuple]) -> int:
    """Run an INSERT … VALUES … statement and return affected row count."""
    if not rows:
        return 0
    total = 0
    with conn.cursor() as cur:
        for chunk in _chunked(rows, BATCH_SIZE):
            psycopg2.extras.execute_values(cur, sql, chunk, page_size=BATCH_SIZE)
            total += cur.rowcount
    return total


# ── Financials — APPEND ONLY ─────────────────────────────────────────────────

FINANCIALS_INSERT = """
INSERT INTO financials (
    company_number, metric, value, period,
    account_closing_date_last_year, fiscal_period_new,
    is_consolidated, data_scope,
    filing_date, source_file, ch_upload
) VALUES %s
"""


def insert_financials(conn, rows: list[dict]) -> int:
    """Append financial rows. No deduplication — full time series is kept."""
    tuples = [
        (
            r["company_number"],
            r["metric"],
            r.get("value"),
            r.get("period"),
            r.get("account_closing_date_last_year") or None,
            r.get("fiscal_period_new"),
            r.get("is_consolidated"),
            r.get("data_scope"),
            r.get("filing_date") or None,
            r.get("source_file"),
            r.get("ch_upload"),
        )
        for r in rows
    ]
    return _execute_values(conn, FINANCIALS_INSERT, tuples)


# ── Directors — UPSERT ───────────────────────────────────────────────────────

DIRECTORS_UPSERT = """
INSERT INTO directors (
    company_number, director_name,
    filing_date, source_file, ch_upload
) VALUES %s
ON CONFLICT (company_number, director_name)
DO UPDATE SET
    filing_date  = EXCLUDED.filing_date,
    source_file  = EXCLUDED.source_file,
    ch_upload    = EXCLUDED.ch_upload,
    loaded_at    = NOW()
"""


def upsert_directors(conn, rows: list[dict]) -> int:
    # Deduplicate on (company_number, director_name)
    seen: dict[tuple, dict] = {}
    for r in rows:
        key = (r["company_number"], r["director_name"])
        seen[key] = r
    rows = list(seen.values())

    tuples = [
        (
            r["company_number"],
            r["director_name"],
            r.get("filing_date") or None,
            r.get("source_file"),
            r.get("ch_upload"),
        )
        for r in rows
    ]
    return _execute_values(conn, DIRECTORS_UPSERT, tuples)


# ── Reports — UPSERT ─────────────────────────────────────────────────────────

REPORTS_UPSERT = """
INSERT INTO reports (
    company_number, company_name, section, text,
    filing_date, source_file, ch_upload
) VALUES %s
ON CONFLICT (company_number, section)
DO UPDATE SET
    company_name = EXCLUDED.company_name,
    text         = EXCLUDED.text,
    filing_date  = EXCLUDED.filing_date,
    source_file  = EXCLUDED.source_file,
    ch_upload    = EXCLUDED.ch_upload,
    loaded_at    = NOW()
"""


def upsert_reports(conn, rows: list[dict]) -> int:
    # Deduplicate on (company_number, section)
    seen: dict[tuple, dict] = {}
    for r in rows:
        key = (r["company_number"], r["section"])
        seen[key] = r
    rows = list(seen.values())

    tuples = [
        (
            r["company_number"],
            r.get("company_name"),
            r["section"],
            r.get("text"),
            r.get("filing_date") or None,
            r.get("source_file"),
            r.get("ch_upload"),
        )
        for r in rows
    ]
    return _execute_values(conn, REPORTS_UPSERT, tuples)


# ── Companies — UPSERT ───────────────────────────────────────────────────────

COMPANIES_UPSERT = """
INSERT INTO companies (
    company_number, company_name, company_status, company_type,
    reg_address_care_of, reg_address_po_box,
    reg_address_line1, reg_address_line2,
    reg_address_post_town, reg_address_county,
    reg_address_country, reg_address_postcode,
    incorporation_date, dissolution_date,
    accounts_next_due, accounts_last_made_up,
    returns_next_due, returns_last_made_up,
    confirmation_next_due, confirmation_last_made,
    sic_code_1, sic_code_2, sic_code_3, sic_code_4,
    country_of_origin, accounts_type, legal_form,
    batch_label
) VALUES %s
ON CONFLICT (company_number)
DO UPDATE SET
    company_name            = EXCLUDED.company_name,
    company_status          = EXCLUDED.company_status,
    company_type            = EXCLUDED.company_type,
    reg_address_care_of     = EXCLUDED.reg_address_care_of,
    reg_address_po_box      = EXCLUDED.reg_address_po_box,
    reg_address_line1       = EXCLUDED.reg_address_line1,
    reg_address_line2       = EXCLUDED.reg_address_line2,
    reg_address_post_town   = EXCLUDED.reg_address_post_town,
    reg_address_county      = EXCLUDED.reg_address_county,
    reg_address_country     = EXCLUDED.reg_address_country,
    reg_address_postcode    = EXCLUDED.reg_address_postcode,
    incorporation_date      = EXCLUDED.incorporation_date,
    dissolution_date        = EXCLUDED.dissolution_date,
    accounts_next_due       = EXCLUDED.accounts_next_due,
    accounts_last_made_up   = EXCLUDED.accounts_last_made_up,
    returns_next_due        = EXCLUDED.returns_next_due,
    returns_last_made_up    = EXCLUDED.returns_last_made_up,
    confirmation_next_due   = EXCLUDED.confirmation_next_due,
    confirmation_last_made  = EXCLUDED.confirmation_last_made,
    sic_code_1              = EXCLUDED.sic_code_1,
    sic_code_2              = EXCLUDED.sic_code_2,
    sic_code_3              = EXCLUDED.sic_code_3,
    sic_code_4              = EXCLUDED.sic_code_4,
    country_of_origin       = EXCLUDED.country_of_origin,
    accounts_type           = EXCLUDED.accounts_type,
    legal_form              = EXCLUDED.legal_form,
    batch_label             = EXCLUDED.batch_label,
    loaded_at               = NOW()
"""


def upsert_companies(conn, rows: list[dict]) -> int:
    def _date(val):
        """Return val or None — psycopg2 handles date strings natively."""
        return val if val else None

    tuples = [
        (
            r.get("company_number"),
            r.get("company_name"),
            r.get("company_status"),
            r.get("company_type"),
            r.get("reg_address_care_of"),
            r.get("reg_address_po_box"),
            r.get("reg_address_line1"),
            r.get("reg_address_line2"),
            r.get("reg_address_post_town"),
            r.get("reg_address_county"),
            r.get("reg_address_country"),
            r.get("reg_address_postcode"),
            _date(r.get("incorporation_date")),
            _date(r.get("dissolution_date")),
            _date(r.get("accounts_next_due")),
            _date(r.get("accounts_last_made_up")),
            _date(r.get("returns_next_due")),
            _date(r.get("returns_last_made_up")),
            _date(r.get("confirmation_next_due")),
            _date(r.get("confirmation_last_made")),
            r.get("sic_code_1"),
            r.get("sic_code_2"),
            r.get("sic_code_3"),
            r.get("sic_code_4"),
            r.get("country_of_origin"),
            r.get("accounts_type"),
            r.get("legal_form"),
            r.get("batch_label"),
        )
        for r in rows
    ]
    return _execute_values(conn, COMPANIES_UPSERT, tuples)


# ── Metadata — UPSERT ────────────────────────────────────────────────────────

METADATA_UPSERT = """
INSERT INTO metadata (
    company_number, field, value,
    filing_date, source_file, ch_upload
) VALUES %s
ON CONFLICT (company_number, field)
DO UPDATE SET
    value       = EXCLUDED.value,
    filing_date = EXCLUDED.filing_date,
    source_file = EXCLUDED.source_file,
    ch_upload   = EXCLUDED.ch_upload,
    loaded_at   = NOW()
"""


def upsert_metadata(conn, rows: list[dict]) -> int:
    # Deduplicate on (company_number, field) — keep last occurrence.
    # Duplicates within a single batch cause PostgreSQL CardinalityViolation.
    seen: dict[tuple, dict] = {}
    for r in rows:
        key = (r["company_number"], r["field"])
        seen[key] = r  # last writer wins, consistent with ON CONFLICT behaviour
    rows = list(seen.values())

    tuples = [
        (
            r["company_number"],
            r["field"],
            str(r["value"]) if r.get("value") is not None else None,
            r.get("filing_date") or None,
            r.get("source_file"),
            r.get("ch_upload"),
        )
        for r in rows
    ]
    return _execute_values(conn, METADATA_UPSERT, tuples)


# ── Batch tracking ───────────────────────────────────────────────────────────

def start_batch(conn, batch_type: str, batch_label: str, source_file: str) -> int:
    """
    Insert a tracking row and return its id.
    Raises if this (batch_type, batch_label) is already completed — prevents
    accidental reprocessing of a finished batch.
    """
    with conn.cursor() as cur:
        # Check for an existing completed batch
        cur.execute(
            """
            SELECT id, status FROM pipeline_batches
            WHERE batch_type = %s AND batch_label = %s
            """,
            (batch_type, batch_label),
        )
        row = cur.fetchone()
        if row:
            existing_id, status = row
            if status == "completed":
                raise ValueError(
                    f"Batch '{batch_label}' ({batch_type}) is already completed "
                    f"(id={existing_id}). Use --force to reprocess."
                )
            # Return the in-progress id so we can update it
            return existing_id

        cur.execute(
            """
            INSERT INTO pipeline_batches
                (batch_type, batch_label, source_file, started_at, status)
            VALUES (%s, %s, %s, NOW(), 'running')
            RETURNING id
            """,
            (batch_type, batch_label, source_file),
        )
        return cur.fetchone()[0]


def complete_batch(
    conn,
    batch_id: int,
    files_processed: int,
    files_failed: int,
    rows_inserted: int,
) -> None:
    """Mark a batch as completed and set it as the live batch of its type."""
    with conn.cursor() as cur:
        # Pull the batch_type so we can flip is_live correctly
        cur.execute(
            "SELECT batch_type FROM pipeline_batches WHERE id = %s", (batch_id,)
        )
        row = cur.fetchone()
        if not row:
            return
        batch_type = row[0]

        # Demote all previous live batches of this type
        cur.execute(
            "UPDATE pipeline_batches SET is_live = FALSE WHERE batch_type = %s",
            (batch_type,),
        )

        # Mark this batch as completed and live
        cur.execute(
            """
            UPDATE pipeline_batches SET
                status          = 'completed',
                completed_at    = NOW(),
                files_processed = %s,
                files_failed    = %s,
                rows_inserted   = %s,
                is_live         = TRUE
            WHERE id = %s
            """,
            (files_processed, files_failed, rows_inserted, batch_id),
        )


def fail_batch(conn, batch_id: int, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_batches SET
                status = 'failed',
                completed_at = NOW(),
                source_file = COALESCE(source_file, '') || ' | ERROR: ' || %s
            WHERE id = %s
            """,
            (error[:500], batch_id),
        )
