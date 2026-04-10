"""
setup_db.py — Create all PostgreSQL tables for the Companies House pipeline.

Run once before the first pipeline execution:
    python setup_db.py

Safe to re-run: all statements use CREATE TABLE IF NOT EXISTS.
"""

import psycopg2
from config import DATABASE_URL

# ── DDL ──────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
-- ============================================================
-- 1. PIPELINE TRACKING
--    One row per batch (accounts monthly file or company CSV
--    snapshot). Lets a cron job know what has been processed
--    and which batch is currently live in production.
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_batches (
    id              SERIAL PRIMARY KEY,

    -- 'accounts'  → a monthly iXBRL batch (e.g. Feb-26)
    -- 'company'   → a Free Company Data Product CSV snapshot
    batch_type      TEXT        NOT NULL,

    -- Human-readable label: 'Feb-26' for accounts, '2026-02' for CSV snapshots
    batch_label     TEXT        NOT NULL,

    -- Source filename(s) e.g. 'Accounts_Monthly_Data-February2026.zip'
    source_file     TEXT,

    -- Pipeline lifecycle timestamps
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,

    -- 'running' | 'completed' | 'failed'
    status          TEXT        NOT NULL DEFAULT 'running',

    -- Summary counters written at the end of each run
    files_processed INTEGER     DEFAULT 0,
    files_failed    INTEGER     DEFAULT 0,
    rows_inserted   INTEGER     DEFAULT 0,

    -- Whether this is the batch currently shown in production queries
    is_live         BOOLEAN     NOT NULL DEFAULT FALSE,

    loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW(),

    UNIQUE (batch_type, batch_label)
);

-- ============================================================
-- 2. COMPANY METADATA  (from Free Company Data Product CSV)
--    Write strategy: UPSERT — each monthly CSV is a full
--    snapshot of all live companies; we replace the row on
--    conflict so we always hold the latest state.
-- ============================================================
CREATE TABLE IF NOT EXISTS companies (
    -- Core identity
    company_number          TEXT        PRIMARY KEY,   -- 8-digit CRN, TEXT to preserve leading zeros
    company_name            TEXT,
    company_status          TEXT,
    company_type            TEXT,

    -- Registered address
    reg_address_care_of     TEXT,
    reg_address_po_box      TEXT,
    reg_address_line1       TEXT,
    reg_address_line2       TEXT,
    reg_address_post_town   TEXT,
    reg_address_county      TEXT,
    reg_address_country     TEXT,
    reg_address_postcode    TEXT,

    -- Dates
    incorporation_date      DATE,
    dissolution_date        DATE,
    accounts_next_due       DATE,
    accounts_last_made_up   DATE,
    returns_next_due        DATE,
    returns_last_made_up    DATE,
    confirmation_next_due   DATE,
    confirmation_last_made  DATE,

    -- SIC codes (up to 4)
    sic_code_1              TEXT,
    sic_code_2              TEXT,
    sic_code_3              TEXT,
    sic_code_4              TEXT,

    -- Other attributes
    country_of_origin       TEXT,
    accounts_type           TEXT,
    legal_form              TEXT,

    -- Tracking
    batch_label             TEXT,           -- which CSV snapshot this came from
    loaded_at               TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_companies_company_number
    ON companies (company_number);

CREATE INDEX IF NOT EXISTS idx_companies_company_name
    ON companies (company_name);

CREATE INDEX IF NOT EXISTS idx_companies_status
    ON companies (company_status);


-- ============================================================
-- 3. FINANCIALS  (from iXBRL parser)
--    Write strategy: APPEND ONLY — we must preserve the full
--    time series across filing dates to support charts/trends.
--    The unique key includes the period so comparative figures
--    (current year vs. prior year) are stored separately.
-- ============================================================
CREATE TABLE IF NOT EXISTS financials (
    id                              BIGSERIAL   PRIMARY KEY,

    company_number                  TEXT        NOT NULL,
    metric                          TEXT        NOT NULL,   -- e.g. 'revenue', 'cash'
    value                           NUMERIC,                -- raw numeric value

    -- Period fields
    period                          TEXT,                   -- e.g. '2024-01-01 to 2024-12-31'
    account_closing_date_last_year  DATE,                   -- derived closing date
    fiscal_period_new               INTEGER,                -- derived year e.g. 2024

    -- Consolidation / scope
    is_consolidated                 TEXT,                   -- 'yes' | 'no' | 'has_group_but_xbrl_is_parent'
    data_scope                      TEXT,                   -- 'instant' | 'duration' from XBRL

    -- Filing metadata
    filing_date                     DATE,
    source_file                     TEXT,
    ch_upload                       TEXT,                   -- e.g. 'Feb-26'

    loaded_at                       TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- Index for the most common query pattern: all metrics for a company
CREATE INDEX IF NOT EXISTS idx_financials_company_number
    ON financials (company_number);

-- Composite index for time-series queries
CREATE INDEX IF NOT EXISTS idx_financials_company_metric_period
    ON financials (company_number, metric, account_closing_date_last_year);

-- Index to query by batch
CREATE INDEX IF NOT EXISTS idx_financials_ch_upload
    ON financials (ch_upload);


-- ============================================================
-- 4. DIRECTORS  (from iXBRL parser)
--    Write strategy: UPSERT — we only need the latest director
--    list per company, so we replace on conflict.
--    Conflict key: (company_number, director_name) — a person
--    can appear only once per company.
-- ============================================================
CREATE TABLE IF NOT EXISTS directors (
    id              BIGSERIAL   PRIMARY KEY,

    company_number  TEXT        NOT NULL,
    director_name   TEXT        NOT NULL,

    -- Filing metadata
    filing_date     DATE,
    source_file     TEXT,
    ch_upload       TEXT,

    loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW(),

    UNIQUE (company_number, director_name)
);

CREATE INDEX IF NOT EXISTS idx_directors_company_number
    ON directors (company_number);


-- ============================================================
-- 5. REPORTS  (from iXBRL parser — narrative text)
--    Write strategy: UPSERT — replace on conflict so we always
--    hold the latest text for each section per company.
--    TEXT type (not VARCHAR) — sections can exceed 10 000 chars.
--    Conflict key: (company_number, section)
-- ============================================================
CREATE TABLE IF NOT EXISTS reports (
    id              BIGSERIAL   PRIMARY KEY,

    company_number  TEXT        NOT NULL,
    company_name    TEXT,
    section         TEXT        NOT NULL,   -- e.g. 'strategic_report'
    text            TEXT,                   -- unlimited length

    -- Filing metadata
    filing_date     DATE,
    source_file     TEXT,
    ch_upload       TEXT,

    loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW(),

    UNIQUE (company_number, section)
);

CREATE INDEX IF NOT EXISTS idx_reports_company_number
    ON reports (company_number);

CREATE INDEX IF NOT EXISTS idx_reports_section
    ON reports (section);


-- ============================================================
-- 6. METADATA  (from iXBRL parser — filing-level facts)
--    Flat key-value pairs: company name, address, auditor,
--    period dates, dormant flag, consolidation status, etc.
--    Write strategy: UPSERT — we only need the latest value
--    for each (company_number, field) pair.
--    Conflict key: (company_number, field)
-- ============================================================
CREATE TABLE IF NOT EXISTS metadata (
    id              BIGSERIAL   PRIMARY KEY,

    company_number  TEXT        NOT NULL,
    field           TEXT        NOT NULL,   -- e.g. 'company_name', 'auditor_name'
    value           TEXT,                   -- always TEXT — values are heterogeneous

    -- Filing metadata
    filing_date     DATE,
    source_file     TEXT,
    ch_upload       TEXT,

    loaded_at       TIMESTAMP   NOT NULL DEFAULT NOW(),

    UNIQUE (company_number, field)
);

CREATE INDEX IF NOT EXISTS idx_metadata_company_number
    ON metadata (company_number);

CREATE INDEX IF NOT EXISTS idx_metadata_field
    ON metadata (field);
"""


def setup_database():
    print("Connecting to PostgreSQL …")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    print("Creating tables …")
    cur.execute(SCHEMA_SQL)

    # Confirm what was created
    cur.execute("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename;
    """)
    tables = [row[0] for row in cur.fetchall()]
    print(f"\nTables in database: {', '.join(tables)}")

    cur.close()
    conn.close()
    print("\nSetup complete. All tables are ready.")


if __name__ == "__main__":
    setup_database()
