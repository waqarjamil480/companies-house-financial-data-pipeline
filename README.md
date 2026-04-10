# UK Companies House Data Pipeline

## 📌 Overview

This project builds a scalable **data engineering pipeline** to process UK Companies House data from two sources:

1. **Accounts Data (iXBRL HTML files)** – Financials, Directors, Reports
2. **Company Data (CSV)** – Official company metadata

The pipeline extracts, transforms, and loads this data into a **PostgreSQL database** using parallel processing.

---

## 🏗️ Architecture

### 🔹 Flow A – Company Data (CSV)

* Download Companies House CSV
* Extract ZIP files
* Load into `companies` table
* Uses **UPSERT** (latest snapshot replaces old data)

### 🔹 Flow B – Accounts Data (iXBRL)

* Download monthly accounts ZIP
* Extract ~300k+ HTML files
* Process using `test_pipeline.py`
* Store output into:

  * `financials` (APPEND ONLY)
  * `directors` (UPSERT)
  * `reports` (UPSERT)
* Delete extracted files after processing

---

## 🗄️ Database Schema

### 1. companies

* Stores master company data (from CSV)
* **Strategy:** UPSERT

### 2. financials

* Stores financial metrics over time
* **Strategy:** APPEND ONLY

### 3. directors

* Stores latest directors per company
* **Strategy:** UPSERT

### 4. reports

* Stores narrative sections
* **Strategy:** UPSERT

### 5. pipeline_batches

* Tracks processed batches

---

## 🔑 Key Design Decisions

### ✔ Company Number

* Stored as `TEXT`
* Preserves leading zeros

### ✔ Write Strategies

| Table      | Strategy | Reason             |
| ---------- | -------- | ------------------ |
| financials | APPEND   | Time-series data   |
| directors  | UPSERT   | Only latest needed |
| reports    | UPSERT   | Only latest needed |
| companies  | UPSERT   | Full snapshot      |

### ✔ Indexing

* Index on `company_number` in all tables

### ✔ Tracking

* Batch tracking ensures:

  * No duplicate processing
  * Easy identification of latest data

---

## ⚙️ Pipeline Features

### 🔹 Parallel Processing

* Uses `multiprocessing.Pool`
* Configurable workers (default: 8)

### 🔹 Batch Inserts

* Inserts in chunks (e.g., 500 rows)

### 🔹 Idempotency

* Safe to re-run
* No duplicate data

### 🔹 Error Handling

* Failed files logged
* Pipeline continues execution

### 🔹 Cleanup

* HTML files deleted after processing
* ZIP files retained

---

## 📂 Project Structure

```
companies_house_financial/
│── data/
│── logs/
│── errors/
│── config.py
│── db.py
│── setup_db.py
│── run_pipeline.py
│── flow_company_csv.py
│── flow_accounts.py
│── test_pipeline.py
│── README.md
```

---

## ▶️ How to Run

### 1. Run Pipeline

```bash
python run_pipeline.py
```

---

## ✅ Verification Queries

### Check companies loaded

```sql
SELECT COUNT(*) FROM companies;
```

### Check financials

```sql
SELECT company_number, COUNT(*)
FROM financials
GROUP BY company_number
LIMIT 10;
```

### Latest batch

```sql
SELECT * FROM pipeline_batches
ORDER BY created_at DESC
LIMIT 1;
```

---

## 🔄 Batch Tracking

Tracks:

* batch_name (Feb-26)
* status (completed/failed)
* processed files
* failed files

Ensures:

* No reprocessing
* Always know latest data

---

## 🚀 Future Improvements

* Cron job/orchestration for automation
* Security checks
* Auto-detect new batches (It will do this but currently used hard-coded line we only need to pass variable)
* Cloud storage (S3)
* Data quality checks

---

## 🧠 Trade-offs

| Decision          | Reason                          |
| ----------------- | ------------------------------- |
| Multiprocessing   | Faster processing of 300k files |
| Batch inserts     | Avoid DB overload               |
| UPSERT strategy   | Keeps latest clean data         |
| Append financials | Required for analytics          |

---

## 📊 Conclusion

This pipeline is:

* Scalable
* Fault-tolerant
* Production-ready

Designed to handle large-scale financial data ingestion efficiently.
