# CSV -> PostgreSQL Pipeline
A production-style batch ingestion pipeline that loads sales transaction data from CSV files into a PostgreSQL database with schema validation, data quality enforcement, error handling, structured logging, and idempotent upserts.

---
# Problem Statement
Flat files like CSVs are common outputs from business systmes, vendor exports, and legacy tools.They are useful for transport but inadequate for serving to multiple consumers, they offer no concurrent access, no query capability, no data integrity guarantees and no audit trail. This pipeline bridges that gap by ingesting CSV data into a relational database in a reliable, repeatable, and observable way.

---

# Architecture
![project architecture](docs/architecture.drawio.svg)

---
### Pipeline stages:
`ingest.py` reads the file from disk, handles encoding detection, and returns a raw DataFrame. It has no opinion about what the data should contain.

`validate.py` (structural) checks that required columns are present and the file is non-empty. Failure here halts the pipeline immediately - there is nothing to transform.

`tranform.py` cast types, normalized strings, parses mixed date formats explicitly, fills null statuses, and derives `total_sale = quantity * unit_price`

`validate.py` (business rules) checks each row after transformation. Rows that violate rules are separated into a dead letter table rather than silently drooped or allowed to corrupt the clean dataset.

`load.py` upserts clean rows into `sales_transactions` and inserts rejected rows into `rejected_rows` for investigation and reprocessing.

---

## Tech Stack

| Tool          |  Role  | Why           |
|--------------|----------|-----------------|
|Python 3.11+ | Pipeline language | Industry standard for data engineering|
|pandas |  DataFrame operations | Efficient tabular data manipulation|
|SQLAlchemy | Database abstraction | Connection pooling, engine management, database-agnostic interface|
|psycopg2| PostgreSQL driver | Required by SQLAlchemy for PostgreSQL connections|
| PostgreSQL 15 | Target database | ACID compliance, concurrent access, rich SQL, production standard |
| Docker | Database environment | Portable, reproducible, no local install required |
|python-dotenv| Credentials management | Keeps secrets out of source code|
|uv |Python environment manager | Fast, modern dependency resolution|

---

## Project Structure
``` text
csv_pipeline/
├── .venv                      # virtual environment
├── data/
│   └── raw/
│       └── sales_data.csv     # Source data
├── docs/
│   ├── architecture.drawio    # Editable architecture diagram
|   ├── architecture.drawio.svg 
│   └── DOCUMENTATION.md       # Full design decision log
├── logs/                      # Pipeline run logs (auto-created)
├── src/
│   ├── ingest.py              # Stage 1 — file reading
│   ├── validate.py            # Stage 2 & 4 — structural + business rule validation
│   ├── transform.py           # Stage 3 — cleaning, casting, enrichment
│   └── load.py                # Stage 5 — PostgreSQL upsert
├── .env                       # Database credentials (never committed)
├── .gitignore
├── docker-compose.yml         # PostgreSQL container definition
├── .python-version
├── README.md
├── test_connection.py         # Test database connectivity
├── uv.lock
├── pyproject.toml
└── main.py                    # Pipeline orchestrator
```

---

## Setup
`Prerequisites`: Docker desktop, Python 3.11+, uv

1. `Clone the repostory`

```bash 
git clone <repo-url>
cd csv_pipeline
```

2. `Install dependencies`

``` bash
uv install
```
3. `Configure credentials`:  copy the example env file and fill in your values

```bash
cp .env.example .env
```

`.env` contents:

```text
DB_USER=pipeline_user
DB_PASSWORD=pipeline_pass
DB_HOST=localhost
DB_PORT=5432
DB_NAME=pipeline_db
```

4. `Start PostgreSQL

```bash
docker compose up -d
```

5. `Verify database connection`

```bash
uv run test_connection.py
```
Expected ouput: `('PostgreSQL 15.x ...',)`

6. `Run the pipeline`
```bash
uv run main.py
```

`Verifying results`
connect to the database and query results
```bash 
docker exec -it csv_pipeline_db psql -U pipeline_user -d pipeline_db
```

```sql
-- Clean rows loaded
SELECT * FROM sales_transactions;

-- Rejected rows with reasons
SELECT transaction_id, rejection_reason, rejected_at 
FROM rejected_rows;

-- Rejection rate
SELECT 
    COUNT(*) FILTER (WHERE rejection_reason IS NULL) AS clean,
    COUNT(*) FILTER (WHERE rejection_reason IS NOT NULL) AS rejected,
    ROUND(
        COUNT(*) FILTER (WHERE rejection_reason IS NOT NULL) * 100.0 / COUNT(*), 
        1
    ) AS rejection_pct
FROM (
    SELECT NULL::text AS rejection_reason FROM sales_transactions
    UNION ALL
    SELECT rejection_reason FROM rejected_rows
) combined;
```

`Idempotency test`: Run the pipeline twice without changing the source file. Row counts in `sales_transactions` should be identical after both runs. This confirms the upsert is working correctly

---
## Observability
Every pipeline run produces a structured log file in `logs/` named with a run ID:

```text
logs/pipeline_a3f2b1c4.log
```

Every log line includes the run ID, timestamp, severity, modulea dn message:

```text 
2024-01-15 09:23:01 | run=a3f2b1c4 | INFO  | src.ingest    | Reading file: data/raw/sales.csv
2024-01-15 09:23:01 | run=a3f2b1c4 | INFO  | src.validate  | Structural validation passed
2024-01-15 09:23:01 | run=a3f2b1c4 | INFO  | src.transform | Transformation complete — shape: (10, 9)
2024-01-15 09:23:01 | run=a3f2b1c4 | WARN  | src.validate  | Rejected rows: T006 — missing status
2024-01-15 09:23:02 | run=a3f2b1c4 | INFO  | main          | Clean loaded: 9 | Rejected: 1 | Rate: 10.0%
```

The run ID ties every log line from a single execution together. When debugging a failure across multiple scheduled runs, grep for the run ID to isolate exactly one execution's logs.

---
## Key Design Decisions
See [documentation](docs/documentation.md) for the full reasoning behind each decision.

`Upsert over append or replace` — keeps the database in sync with the source without duplicating data or destroying existing records

`Dead letter table over silent drops` — rejected rows are preserved and queryable, not lost.

`Explicit date format parsing over pd.to_datetime` — prevents silent misinterpretation of ambiguous formats like 01/02/2024

`Two exception types in ingestion` — allows the orchestrator to distinguish recoverable failures (retry) from unrecoverable ones (escalate)

`Per-stage module boundaries` — each file owns exactly one concern, making stages independently testable and replaceable


## What I Would Do Differently in Production
`Orchestration` — replace main.py as the runner with Apache Airflow. Each pipeline stage becomes a DAG task with retries, backfill capability, and dependency management.

`Schema migrations` — replace CREATE TABLE IF NOT EXISTS with Alembic. Schema changes would be versioned, auditable, and reversible across all environments.

`Data validation` — replace the custom validation framework with Great Expectations or Soda for richer checks, built-in alerting, and data quality dashboards.

`Testing` — add unit tests for each module using pytest with a test fixture database, and integration tests that run the full pipeline against known input/output pairs.

`Secrets management` — replace .env files with AWS Secrets Manager or HashiCorp Vault.