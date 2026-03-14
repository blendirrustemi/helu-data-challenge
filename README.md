## Helu Data Engineering Assignment Solution

This repository contains my solution for the Helu data engineering technical assignment.  

---

### How to run the solution

- **Python version**: 3.10+

### Setup
- **Create and activate a virtual environment**:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

- **Install dependencies**:
```bash
python -m pip install -r requirements.txt
```

- **Start the subscription API Docker** from the project root:

```bash
docker compose up --build
```

Verify it is running:

```bash
curl http://localhost:5050/health
```

- **Run the tests:**

```bash
python -m pytest -v
````

- **Run the pipeline** from the project root:

```bash
python -m pipeline.main
```

By default the pipeline reads from `http://localhost:5050`.  
You can override this via the `SUBSCRIPTION_API_URL` environment variable, for example:

```bash
SUBSCRIPTION_API_URL="http://localhost:5051" python -m pipeline.main
```

The pipeline is **idempotent**: running it multiple times overwrites the same output files and DuckDB tables with the latest data.

---

### Outputs and how to query them

The pipeline writes the following outputs to the `output/` directory:

- `silver_subscription_events.parquet`: cleaned, normalized subscription events (Silver layer)
- `gold_monthly_report.parquet`: final CFO report (Gold layer)
- `warehouse.duckdb`: DuckDB database containing:
  - `silver_subscription_events`
  - `gold_monthly_report`

**Query examples (DuckDB CLI or Python):**

```sql
SELECT *
FROM gold_monthly_report
WHERE country = 'DE'
ORDER BY report_month, platform, subscription_type;
```

```sql
SELECT report_month,
       platform,
       SUM(mrr_eur) AS total_mrr_eur
FROM gold_monthly_report
GROUP BY report_month, platform
ORDER BY report_month, platform;
```

You can also work directly with the Parquet files in Python:

```python
import pandas as pd

gold = pd.read_parquet("output/gold_monthly_report.parquet")
print(gold.head())
```

---

### Design overview

#### Storage and queryability

- **Parquet files** (`silver_subscription_events.parquet`, `gold_monthly_report.parquet`)
  - Columnar, compressed, and easy to read from Python and many BI tools.
- **DuckDB database** (`warehouse.duckdb`)
  - Provides a simple, file-based analytical database that can be queried with SQL.
  - Tables are created using `CREATE OR REPLACE`, so re-running the pipeline refreshes the data without manual cleanup.

For this assignment, DuckDB + Parquet was a good fit because it keeps the solution lightweight, local, and easy to inspect without requiring external infrastructure.

In a **production** environment, I would likely keep Parquet as the analytical storage format and move the pipeline to an S3 based architecture. A simple version could use Lambda or Batch for ingestion/transformation, Step Functions for orchestration, and S3 as the Bronze/Silver/Gold storage layer.

#### Silver / Gold modeling

- **Silver layer (`silver_subscription_events`)**
  - Canonical schema defined in `pipeline/schemas.py` - `SILVER_COLUMNS`.
  - Source-specific column names are mapped via `APFEL_COLUMN_MAP` and `FENSTER_COLUMN_MAP`.
  - Timestamps are parsed to UTC-aware `datetime`.
  - Additional normalization is handled in `pipeline/quality.py`:
    - Event types are mapped onto a common set: `acquisition`, `renewal`, `cancellation`.
    - Country values are standardized to ISO2 codes.
    - Currencies are normalized (e.g. `EURO` to `EUR`).
    - Renewal period is inferred from explicit fields or subscription type where possible.
    - Rows missing required fields or with invalid event types are dropped.
    - Duplicate events are removed (based on `platform` + `event_id`).

- **Gold layer (`gold_monthly_report`)**
  - Schema defined in `pipeline/schemas.py` - `GOLD_COLUMNS`.
  - Built in `pipeline/report.py` from the cleaned Silver data and the exchange-rate table.
  - Provides the required CFO metrics per month, platform, subscription type, and country:
    - `acquisitions`
    - `renewals`
    - `cancellations`
    - `mrr_eur`

---

### MRR and event logic

#### Event counts

- Event counts are computed in `build_event_counts`:
  - `report_month` is derived from the event timestamp.
  - Events are grouped by:
    - `report_month`
    - `platform`
    - `subscription_type`
    - `country`
    - `event_type`
  - A pivot produces:
    - `acquisitions`
    - `renewals`
    - `cancellations`

#### MRR calculation

MRR is computed in `build_mrr_report` with the following logic:

- For each `report_month`, consider all events **up to the end of that month**.
- For each subscription (grouped by `platform`, `customer_id`, `subscription_type`):
  - Take the **latest event on or before month-end**.
  - If the latest event is a `cancellation`, that subscription does **not** contribute to MRR for that month.
- Normalize to a **monthly amount**:
  - If `renewal_period` is monthly: use the amount as it is.
  - If `renewal_period` is yearly: divide by 12.
  - Otherwise: treat as 0.
- Convert to **EUR**:
  - Exchange rates are normalized and grouped by `report_month` and `currency`.
  - EUR amounts use a rate of 1.0.
  - Subscriptions without a matching FX rate are excluded from MRR, and the number of excluded rows is logged.
- Aggregate by `report_month`, `platform`, `subscription_type`, `country` to produce `mrr_eur`.

`build_gold_report` then joins the event counts and MRR into the final Gold table, filling missing counts and MRR with zeros.

---

### Idempotency

- Parquet outputs are written to fixed paths under `output/` and overwritten on each run.
- DuckDB tables are created using `CREATE OR REPLACE TABLE`, so each run fully refreshes the `silver_subscription_events` and `gold_monthly_report` tables.

This makes the pipeline **idempotent**: re-running with the same source data will always produce the same results, without duplicate rows.

---

### Data quality considerations and trade-offs

During profiling and standardization, I found several source-data inconsistencies that needed to be handled before building the final report.

#### Issues identified

- Mixed event-type casing and formats across sources  
  Examples included `SUBSCRIPTION_STARTED`, `subscription_started`, `NEW`, `new`, and `cancel`.

- Mixed country representations  
  The raw data contained ISO2 codes (`GB`, `DE`), ISO3 codes (`GBR`, `USA`), full names (`United States`), blank values, and invalid placeholders such as `INVALID` and `XX`.

- Mixed currency representations  
  Examples included `EUR`, `eur`, `Euro`, `USD`, and `GBP`.

- Source-specific timestamp formats  
  Apfel provided timezone-aware ISO timestamps (for example with `Z`), while Fenster used timestamps such as `2025-02-01 13:38:59`.

- Duplicate event records  
  Some events appeared more than once and needed deduplication.

- Missing FX coverage for some currencies  
  The provided FX table only included monthly USD to EUR rates. EUR could be handled as `1.0`, but currencies such as GBP could not be converted with the provided data.

#### Rules applied

- Rows with missing required fields (such as `event_id`, `event_type`, `amount`, `country`) are dropped.
- Event types are normalized before mapping; values outside the expected lifecycle mapping are treated as invalid and removed.
- Country values are normalized to ISO2 format. Values that cannot be reliably normalized are treated as missing and dropped.
- Blank country values are explicitly converted to missing values before validation.
- Currency values are normalized through a controlled mapping. Values outside the supported mapping are treated as invalid and dropped.
- Duplicate events are removed using (`platform`, `event_id`).
- For MRR conversion, EUR is treated as `1.0`. Active subscription snapshots with missing FX rates are excluded from MRR and logged rather than silently converted to `0.0`.

These rules are intentionally strict. The goal is to ensure that the final CFO report is built only from consistent and explainable records. The number of dropped or excluded rows per rule is logged for observability.

---
