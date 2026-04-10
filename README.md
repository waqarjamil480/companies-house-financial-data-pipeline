# Companies House Financial Data Pipeline

A production-grade Python pipeline that downloads, parses, and stores UK company accounts (iXBRL) and company metadata (Free Company Data Product CSV) into PostgreSQL.

---

## Quick Start

```bash
# 1. Clone and install dependencies
pip install -r requirements.txt

# 2. Configure credentials
cp config.py config.py          # edit DB_* variables or set environment variables

# 3. Create the database (once)
python setup_db.py

# 4. Run the full pipeline (Flow A then Flow B)
python run_pipeline.py
```

### Environment variable overrides

All config values can be overridden without editing `config.py`:

```bash
export DB_HOST=my-postgres-host
export DB_PASSWORD=secret
export WORKER_COUNT=16
python run_pipeline.py
```

---

## Repository Layout

```
ch_pipeline/
├── config.py           # DB credentials, paths, worker count, URLs
├── setup_db.py         # Creates all PostgreSQL tables (run once)
├── run_pipeline.py     # Main entry point — CLI for both flows
├── flow_accounts.py    # Flow B: iXBRL download → parse → write
├── flow_company_csv.py # Flow A: CSV download → normalise → upsert
├── db.py               # All database helpers: pool, writers, batch tracking
├── test_pipeline.py    # Parser (provided — treat as black box)
├── requirements.txt
├── README.md
└── logs/               # Per-run log files + failed-file lists
```

---

## Schema Design

### Design philosophy

The schema models four distinct entities with very different write behaviours. Getting those right was the central design decision.

### `pipeline_batches` — Batch tracking

```sql
CREATE TABLE pipeline_batches (
    id              SERIAL PRIMARY KEY,
    batch_type      TEXT NOT NULL,          -- 'accounts' | 'company'
    batch_label     TEXT NOT NULL,          -- 'Feb-26' | '2026-02'
    source_file     TEXT,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    status          TEXT DEFAULT 'running', -- 'running' | 'completed' | 'failed'
    files_processed INTEGER DEFAULT 0,
    files_failed    INTEGER DEFAULT 0,
    rows_inserted   INTEGER DEFAULT 0,
    is_live         BOOLEAN DEFAULT FALSE,
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (batch_type, batch_label)
);
```

**Why:** Any production pipeline needs an audit trail. This table answers "what is live right now?" in a single query and prevents a cron job from accidentally reprocessing a completed batch. The `is_live` flag is toggled atomically — when a new batch completes, all previous batches of the same type are demoted first.

**Live batch query:**
```sql
SELECT * FROM pipeline_batches WHERE is_live = TRUE ORDER BY batch_type;
```

---

### `companies` — Company metadata (from Free Company Data Product CSV)

Primary key: `company_number TEXT` (8-digit CRN, zero-padded).

**Write strategy: UPSERT** — the CSV is a full monthly snapshot of all live companies. Each load replaces any existing row for a company, so we always hold the most recent state.

**Why TEXT for company_number:** The CRN `01174621` would silently become `1174621` as an integer — losing the leading zero and breaking all joins with the accounts tables. `TEXT` with a `ZFILL(8)` on ingestion is the only safe approach.

**Conflict key:** `company_number` (PRIMARY KEY) — one row per company.

---

### `financials` — XBRL metrics (from iXBRL parser)

```sql
CREATE TABLE financials (
    id                              BIGSERIAL PRIMARY KEY,
    company_number                  TEXT NOT NULL,
    metric                          TEXT NOT NULL,
    value                           NUMERIC,
    period                          TEXT,
    account_closing_date_last_year  DATE,
    fiscal_period_new               INTEGER,
    is_consolidated                 TEXT,
    data_scope                      TEXT,    -- 'single_entity' | 'parent_only'
    filing_date                     DATE,
    source_file                     TEXT,
    ch_upload                       TEXT,
    loaded_at                       TIMESTAMP DEFAULT NOW()
);
```

**Write strategy: APPEND ONLY** — this table is a time series. A company filing in February 2026 will include both the 2025 figures and the 2024 comparatives. We must preserve both. Overwriting would destroy the historical trend that drives the charts. There is intentionally no unique constraint on `(company_number, metric, period)` — the same metric for the same period appearing in two different monthly batches is a valid occurrence (a restatement) and both rows should be retained.

**Indexes:**
- `(company_number)` — most queries filter by company
- `(company_number, metric, account_closing_date_last_year)` — time-series queries
- `(ch_upload)` — batch-level queries ("how many rows did Feb-26 produce?")

---

### `directors` — Named officers

**Write strategy: UPSERT** — we only need the current director list per company. If a director leaves and is removed in the next filing, the next upsert removes them by replacing the full set. The conflict key is `(company_number, director_name)`.

**Note:** The parser returns names exactly as tagged in the iXBRL. We do not deduplicate by fuzzy name matching — that would be a separate enrichment step outside this pipeline's scope.

---

### `reports` — Narrative text sections

**Write strategy: UPSERT** — we only need the latest version of each narrative section. The conflict key is `(company_number, section)`.

**TEXT not VARCHAR:** Section text regularly exceeds 10 000 characters. `VARCHAR(n)` would silently truncate or raise an error. PostgreSQL's `TEXT` type has no length limit and stores content efficiently.

Sections observed in sample data: `strategic_report`, `directors_report`, `related_party_disclosures`, `related_party_disclosures_xbrl`, `ultimate_controlling_party`.

---

### `metadata` — Filing-level key-value facts

**Write strategy: UPSERT** — flat key-value store for filing metadata: company name, address fields, auditor, period start/end, balance sheet date, dormant flag, consolidation status, and more. The conflict key is `(company_number, field)` — one value per field per company, always the latest.

Fields observed in sample data: `company_name`, `company_number_filed`, `period_start`, `period_end`, `balance_sheet_date`, `address_line_1`, `city`, `postcode`, `auditor_name`, `is_dormant`, `is_consolidated`, `principal_activities`, `parent_entity`, `subsidiary_name`.

The `value` column is always TEXT — these fields are heterogeneous (dates, names, booleans as strings) and a key-value model is simpler than 20+ nullable columns on the financials table.

---

## Architecture: Data Flow

### Flow A — Company CSV

```
Companies House website
        │  (scrape index page for ZIP URLs)
        ▼
flow_company_csv.py
        │  download BasicCompanyData-YYYY-MM-DD-partN_7.zip (×7 files)
        │  extract CSV from each ZIP (in-memory, no temp files)
        │  normalise column names + date formats
        │  batch upsert → companies table (500 rows/batch)
        │  update pipeline_batches (is_live = TRUE)
        ▼
PostgreSQL: companies
```

### Flow B — Accounts iXBRL

```
Companies House website
        │  download Accounts_Monthly_Data-February2026.zip (~1.8 GB)
        ▼
flow_accounts.py
        │  extract HTML files → ./data/accounts_extracted/Feb_26/
        │  multiprocessing.Pool (8 workers, imap_unordered)
        │       └─ each worker calls test_pipeline.parse_one_file(path)
        │  accumulate results in buffers
        │  flush to DB every 500 rows (insert_financials / upsert_directors / upsert_reports)
        │  delete extracted HTML directory (keep ZIP)
        │  log any parse failures to logs/failed_*.log
        │  update pipeline_batches (is_live = TRUE)
        ▼
PostgreSQL: financials (append), directors (upsert), reports (upsert)
```

---

## Trade-offs and Design Decisions

### Batch size: 500 rows

`psycopg2.extras.execute_values` with 500-row chunks balances two concerns: very small batches waste round-trips to PostgreSQL; very large batches hold locks longer and increase memory pressure in the worker pool. 500 rows was chosen as a pragmatic starting point — for a production system, this should be tuned against observed network latency.

### `imap_unordered` vs `imap`

Using `imap_unordered` allows the main process to start writing DB rows while workers are still parsing, rather than waiting for the entire batch to finish. This significantly reduces peak memory usage for a 300k-file batch.

### Chunksize=10 in `imap_unordered`

Each task (parsing one HTML file) takes roughly 50–200ms. A chunksize of 10 reduces IPC overhead without starving workers. For faster I/O environments, increasing chunksize to 50 would improve throughput.

### No ORM

SQLAlchemy or similar would add abstraction with little benefit here. The write patterns are narrow and well-defined. Raw `psycopg2` with `execute_values` is faster and simpler to reason about.

### CSV parsing: in-memory via `io.TextIOWrapper`

Rather than extracting CSV files to disk, we wrap the ZipFile member in a `TextIOWrapper` and stream rows directly to the upsert buffer. This avoids writing ~500 MB of CSV to disk.

### Multi-part CSV handling

Companies House splits the Free Company Data into 7 ZIP files. We discover these by scraping the index page (so new part counts are handled automatically) and process them sequentially. The upsert strategy means order doesn't matter — each part is idempotent.

### Error handling

Individual file parse failures are logged and counted but do not abort the batch. The pipeline continues and records the total failure count in `pipeline_batches.files_failed`. This matches real-world behaviour where ~1–3% of iXBRL filings are malformed.

---

## Idempotency

The pipeline is safe to re-run:

- `start_batch()` checks for an existing `completed` tracking row and raises before any processing begins. Use `--force` to override.
- Financials use APPEND ONLY — re-running a batch would produce duplicate rows. The tracking check prevents this.
- Directors and Reports use UPSERT — re-running is always safe (idempotent by definition).
- Companies uses UPSERT — also always safe.

---

## Verification: Example Sanity Queries

### 1. What batches are live?
```sql
SELECT batch_type, batch_label, files_processed, files_failed, rows_inserted, completed_at
FROM pipeline_batches
WHERE is_live = TRUE;
```

### 2. How many companies loaded?
```sql
SELECT COUNT(*), COUNT(DISTINCT company_status) FROM companies;
```

### 3. Revenue time series for a specific company
```sql
SELECT fiscal_period_new, value, ch_upload, filing_date
FROM financials
WHERE company_number = '01174621'
  AND metric = 'revenue'
ORDER BY fiscal_period_new;
```

### 4. Companies with no matching financials (data quality check)
```sql
SELECT COUNT(*) FROM companies c
WHERE NOT EXISTS (
    SELECT 1 FROM financials f WHERE f.company_number = c.company_number
);
```

### 5. How many financials rows per batch?
```sql
SELECT ch_upload, COUNT(*) AS rows
FROM financials
GROUP BY ch_upload
ORDER BY ch_upload;
```

### 6. Most common metrics in the Feb-26 batch
```sql
SELECT metric, COUNT(*) AS cnt
FROM financials
WHERE ch_upload = 'Feb-26'
GROUP BY metric
ORDER BY cnt DESC
LIMIT 20;
```

### 7. Failed files in a batch
```sql
SELECT batch_label, files_failed, source_file
FROM pipeline_batches
WHERE batch_type = 'accounts'
ORDER BY completed_at DESC;
```

### 8. Strategic reports with > 5000 chars
```sql
SELECT company_number, company_name, LENGTH(text) AS chars
FROM reports
WHERE section = 'strategic_report' AND LENGTH(text) > 5000
ORDER BY chars DESC
LIMIT 10;
```

---

## Monthly Cron Design

The following describes how to extend this pipeline to run automatically every month.

### Detecting new batches

```python
# In a cron script: detect_new_batches.py
import requests
from bs4 import BeautifulSoup

def discover_available_batches(index_url):
    """Scrape the Companies House page and return all available ZIP filenames."""
    resp = requests.get(index_url)
    soup = BeautifulSoup(resp.text, "html.parser")
    return [
        a["href"].split("/")[-1]
        for a in soup.find_all("a", href=True)
        if "Accounts_Monthly_Data" in a["href"] and a["href"].endswith(".zip")
    ]
```

### Comparing against the tracking table

```python
def find_unprocessed(available, db_conn):
    """Return filenames not yet in pipeline_batches as 'completed'."""
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT batch_label FROM pipeline_batches "
            "WHERE batch_type='accounts' AND status='completed'"
        )
        done = {row[0] for row in cur.fetchall()}

    return [
        fn for fn in available
        if derive_batch_label(fn) not in done
    ]
```

### Cron logic

```python
unprocessed = find_unprocessed(available, conn)
unprocessed.sort()  # chronological — oldest first (upsert tables always end up with latest)

for filename in unprocessed:
    flow_accounts.run(zip_filename=filename)

# Refresh company CSV monthly (full snapshot — always replace)
flow_company_csv.run()
```

### Company CSV refresh

The Free Company Data Product is a complete snapshot — not incremental. Each monthly load replaces all rows via UPSERT. The `is_live` flag in `pipeline_batches` always points to the most recently completed CSV batch, making it queryable which snapshot is active.

### Deployment

A simple `cron` entry to run on the 2nd of each month (accounts are typically published on the 1st):

```cron
0 3 2 * * /usr/bin/python /opt/ch_pipeline/run_pipeline.py >> /var/log/ch_pipeline_cron.log 2>&1
```

---

## Setup Instructions

### Prerequisites

- Python 3.11+
- PostgreSQL 14+ (local or remote)
- ~5 GB disk space for ZIPs and temporary extraction

### Steps

```bash
# Install Python dependencies
pip install -r requirements.txt

# Set database credentials (or edit config.py directly)
export DB_NAME=companies_house
export DB_USER=postgres
export DB_PASSWORD=yourpassword

# Create the database (if it doesn't exist)
createdb companies_house

# Create all tables
python setup_db.py

# Run the full pipeline
python run_pipeline.py

# Or run flows independently
python run_pipeline.py --flow company      # CSV only
python run_pipeline.py --flow accounts    # iXBRL only

# Skip download if ZIPs are already local
python run_pipeline.py --flow accounts --skip-download

# Use more workers on a beefy machine
python run_pipeline.py --workers 16
```

### Testing locally with sample files

The repository includes sample iXBRL files in `sample_html/`. Test the parser before running against the full batch:

```bash
python test_pipeline.py sample_html/
```

This writes `test_results.xlsx` and `test_results_reports.json` to `sample_html/`.

---

## Assumptions

1. **company_number from filename is authoritative.** The `CCCCCCCC` segment of the filename is used as the CRN, not the value tagged in the XBRL (which can be missing or malformed in micro-entity accounts). This matches the spec's note that iXBRL metadata is unreliable.

2. **Financials append for every monthly batch.** If the same company files in both Jan-26 and Feb-26, we store both sets of rows. Deduplication is a downstream concern (analytics queries can filter by `ch_upload` or `filing_date`).

3. **Director removal is implicit.** We upsert the current filing's director list. If a director disappears in a later filing, they remain in the table until a subsequent filing for that company upserts without them — at which point they are not removed (only updated rows change). A full delete-then-insert strategy would be safer; this is noted as a future improvement.

4. **CSV date format is DD/MM/YYYY.** Companies House uses this format. The normaliser converts to `YYYY-MM-DD` for PostgreSQL `DATE` columns.

5. **Multi-part CSV order is irrelevant.** Since the write strategy is UPSERT by `company_number`, processing parts in any order yields the same result.
