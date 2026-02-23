# TECHNICAL IMPLEMENTATION - CSV -> POSTGRES PIPELINE
This document records the engineering decisions made during the design and implementation of this pipeline, the reasoning behind each decision, the tradeoofs considered, and what would change at production scale. It is intended for engineers reading this codebase who want to understand not just what the code does, but why it is written the way it is.

---

## Table of Contents
* [Architecture Decisions](#architecture-decisions)
* [Module Boundaries](#module-boundaries)
* [Ingestion Design](#ingestion-design)
* [Validation Design](#validation-design)
* [Transformation Design](#transformation-design)
* [Load Design](#load-design)
* [Observability Design](#observability-design)
* [Scale Considerations](#scale-considerations)
* [Known Limitations](#known-limitations)

---

## Architecture Decisions
### `Why PostgreSQL over a flat file store`
CSVs are adequate for transport but inadequate for serving data to multiple consumers simultaneously. The decision to load into PostgreSQL was driven by four requirements.

#### `Concurrent access.`
A CSV file cannot be safely read and written by multiple processes at the same time. PostgreSQL handles hundreds of concurrent connections with row-level locking. The moment more than one systems needs to query this data, flat files break down.

#### `Query efficiency at scale.`
Pandas requires loading the entire file into memory before filtering. PostgreSQL executes `WHERE` clauses against indexes, touching only the rows it needs. At `50GB` the difference between these approaches is the difference between a working pipeline and an out-of-memory crash.

#### `Data integrity guarantees.`
PostgreSQL enforces column types, NOT NULL constraints, and primary key uniqueness at write time. A CSV ccepts any value in any column silently. Bad data is caught at the storage layer, not discovered later by an analyst.

#### `Audit trail.`
The `loaded_at` column on `sales_transactions` is set automatically by PostgreSQL on very insert. Every row carries a permanent record of when it entered the system. This is free infrastructure for data lineage.

### Why batch ingestion over streaming
The source data is a file delivered periodically. There is no event stream to consume. Batch is the correct pattern here, process the complete file when it arrives, load all valid rows, store rejected rows for investigation.
Streaming would introduce significant operational complexity (Kafka cluster, consumer groups, offset management, exactly-once semantics) with no benefit, because the data is not time-sensitve at the row level.

`The decision framework: ask how stale the data can be before it causes a business problem. For periodic file-based data, batch is almost always the right answer`

---

## Module Boundaries
Each source file own exactly one concern. This  is enforced deliberately and documented here because it is the most important structural decision decision in the codebase

| Module       | Owns                 |        Does Not Own|
|--------------|-----------------------|--------------------|
|`ingest.py`| Reading bytes from disks, encoding detection| Column validation, value checks, any transformations|
|`validate.py`| Structural checks, business rule checks | Reading files, modifying data|
|`transform.py` |Type casting, string normalization, date parsing, derived columns | Checking whether the data is valid, reading files|
|`load.py` | Writing to PostgreSQL, data creation | Any data modification, validation|
|`main.py` | Orchestration, error routing, logging setup, pipeline summary | Any data processing logic| 
|

#### `Why this matters.`
When a bug appears in production, you know immediately which file to look in based on what went wrong. When a business rule changes, you change exactly one function in exactly one file. When you want to test date parsing in isolation, you import `parse_data` from `transform.py` and test it directly without touching a database.

Violating these boundaries, for example,putting validation logic in `ingest.py` makes stages untestable in isolation and creates invisible coupling between concerns that should be independent.

#### `The column normalization decision.`
`A deliberate edge case`: column name normalization (stripping whitespace, lowercasing) happens in `transform.py` rather than `ingest.py`. This created a sequencing problem because strutural validation runs before transformation and needs to check column names.

`The resolution` : `validate_structure` normalizes column names internally for comparison only, without mutationg the dataframe it received. The DataFrame it received. The DataFrame that enters `validae_structure` is identical to the one that exits it. Normalize-for-comparison is a read operation, not a write operation, and therefore belongs in validae rather than transform.

---
## Ingestion Design
#### `Two exception types for two operationally different failures`
`ingest.py` raises a `FileNotFoundError` when the file does not exist, and `RuntimeError` when the file exists but cannot be read. These are different exceptions because they require different responses from the orchestrator

``` python
except FileNotFoundError:
    # File hasn't arrived yet - check upstream delivery, consider retrying
    retry_or_alert()

except RuntimeError:
    # File arrived but iscorrupted or uses an unsupported encoding
    # Retrying will not help - escalate immediately
    escalate()
```
---
If both failures raised `ValueError`, the orchestrator would need to parse error message strings to distinguish them, which is fragile and breaks when messages changes. Exception types are the correct channel for communicating the nature of the failure.

#### `Encoding fallback strategy.`
`UTF-8` is attempted first because it is the correct modern standard. If it fails, `latin-1` is used as a fallback. Latin-1 is chosen specifically because it never raises a `UnicodeDecodeError`, every possible byte value is a valid latin-1 character. It is the universal safe fallback for single-byte encodings.

--- 
## Validation Design
#### `Two validation passes at different pipeline positions`
`Structural validation` runs immediately after ingestion, before any transformation. It checks shape and schema - are the required columns present? is the file non-empty? Failure here halts the entire pipeline because if the structure is wrong, every downstream operation is operating on undefined data. There is no partial failure - it is all or nothing.
`Business rule validation` runs after transformation. It checks individual rows - are quantities positive? are dates parseable? are status values from the allowed set? Failure here does not halt the pipeline. Rows that fail are separated into a dead letter table and the clean rows proceed to load. 

`The distinction between two validation types is important: structural failures are pipeline failures. Business rule failures are data quality issues. They require different responses.`

### DEAD LETTER TABLE OVER SILENT DROPPING
Rejected rows are written to a `rejected_rows` table in PostgreSQL rather than being logged and discarded. This is the most important data quality design decision in the pipeline.

Silent data loss is the most dangerous failure mode in a data pipeline because it produces no error, the pipeline succeeds, the load completes, but your data is wrong. If you drop 10% of rows silently, analysts querying the database see correct-looking data that is actually incomplete.
Storing rejected rows in a queryable table means:

```sql
-- Understand what is failing and why 
SELECT rejection_reason, COUNT(*)
FROM rejected_rows
GROUP BY rejection_reason
ORDER BY COUNT(*) DESC;

-- Reprocess after fixing source data
SELECT * FROM rejected_rows WHERE rejected_at > "2024-01-15";
```
---
This pattern is called a `dead letter queue` in distributed systems. The concept is identical - messages (rows) that cannot be processed are routed to a separate destination rather than discarded, preserving them for investigtion and preprocessing.

#### `Collecting all violations per row post-transformation`
Business rule validation collects all violations per row rather than stopping at the first one. A row rejected for `quantity must be positve` might also have `unparseable transaction_date`. Reporting both means the enginer fixing the source data fixes all problems at once rather than discovering them once at a time across multiple pipeline runs.

`Structural validation, by contrast, halts immediately on first failure. If columns are missing, checking row counts is meaningless. The principle: fail fast on structural issues, collect everything on data quality issues`

---
## Transformation Design
#### Explicit date format parsing over `pd.to_datetime`
The source data contains three date formats in the same column: `2024-01-15`, `15/01/2024`, `Jan 15 2024`.

`pd.to_datetime(df["transaction_date"], infer_datetime_format=True)` would handle this, but with a dangerous behavior: it infers the format from the first value it successfully parses, then applies that assumption to every subsequent value. For ambiguous dates like `01/02/2024`, this produces silently wrong results - the pipeline succeeds, the data loads, and the error only surfaces when an analyst notices that January and February revenue figures look wrong.

The production-safe approach is explicit format iteration

```python
DATE_FORMATS = ["%Y-%m-%d", "%d/%m/%Y", "%b %d %Y"]

for fmt in DATE_FORMATS:
    try:
        return datetime.strptime(date_str, fmt)
    except ValueError:
        continue
return None  # explicit None — never silently wrong
```

---
This is slower than vectorized `pd.to_datetime` but its behavior is fully predictable. You know exactly which formats are supported. You know exactly what happens when an unsupported format appears - it returns `None`, gets flagged by business rule validation, and lands in the rejected rows table. There is no silent failures.

The performance tradeoff is acceptable because ingestion pipelines are not latency-sensitive - they run in batch, often overnight. Correctness outweighs speed.

#### `Derived column at transformation time`
`total_sale = quantity * unit_price` is computed during transformation rather than left for analysts to compute in queries.

The rule: if 90% of queries against this table will need a calculation, compute it once at load time. Every analyst writing a revenue query should not need to know that total sale is quantity times unit price - that is a business fact that belongs in the data, not in every query that touches it

`NUMERIC(10,2)` with `.round(2)` prevents floating point artifacts. Financial values stored as `float64` produces results like `59.9900000000000000002`. `NUMERIC(10,2)` in PostgreSQL stores exact decimal values.

## Load Design
#### `Upsert over append or replace`
These strategies exist for handling reruns of the same data:

`Append` inserts all rows unconditionally. IF the pipeline runs twice on the same file, every row is duplicated. Revenue aggregations double. This almost never correct for operational data.

`Replace` truncates the table and reloads from scratch. This works but has two problems at scale: it is slow (drop and recreate millions of rows), and it is dangerous (the window between truncate and reload completion is a period where the table is empty or incomplete - any query during that window returns wrong results).

`Upsert` inserts new rows and updates existing rows based on the primary key. If the pipeline runs 100 times on the same file, the result is identical to running it once.This property - that running an operation multiple times produces the same result as running it once - it is called `idempotency`.It is a critical property for any pipeline that runs on a schedule.

PostgreSQL implements upsert with `INSERT ... ON CONFLICT (primary_key) DO UPDATE SET`. This is a single atomic statement - it does not require a separate SELECT to check existence and a conditional INSERT or UPDATE. One round trip, one operation, atomic.

#### Why not `df.to_sql()`
pandas `DataFrame.to_sql` is a convenience method that supports `if_exists="append"` and `if_exists="replace"` but has no upsert capability. Using it for production loads means giving up idempotency. Writing the SQL explicitly means you know exactly what statement is executing aginst your database.

#### `Table creation in application code`
`CREATE TABLE IF NOT EXISTS` is used here for simplicity. In production this would be replaced with Alembic migrations. The limitation of `CREATE TABLE IF NOT EXISTS` is that it only answers "does the table exist?" - it cannot apply schema changes to an existing table. Adding a column, changing a constraint, adding an index on an existing table all require `ALTER TABLE` statements that `CREATE TABLE IF NOT EXISTS` will never execute because the table exists.

Migration tools like Alembic version schema changes the same way git versions application code. Each change is a numbered file with an upgrade and downgrade function. The tool tracks which migrations have been applied and runs only the pending ones. Every environment stays in sync.

---

## Obervability Design
#### `Run ID on every log line`
Every pipeline execution generates a UUID-based run ID on startup. This ID appears in every log line from that execution and in the filename of the log file

Without a run ID, debugging a scheduled pipeline is extremely difficult. If a pipeline runs nightly and fails on Tuesday, the logs from Monday, Tuesday and Wednesday are interleaved in the same output. With a run ID, you grep run ID and see exactly that execution's complete story from start to finish

```
2024-01-15 09:23:01 | run=a3f2b1c4| INFO | src.transform | 
Transformation complete - shape: (10, 9)
```
Every line contains: timestamp, run ID, severity level, module name, message. This format is grep-friendly, parseable by log aggregation tools (Datadog, CloudWatch, ELK) and human-readable. The module name tells you immediately which stage produced the line.

#### `Pipeline summary block`
The final log block reports: run ID, duration, input row count, clean rows loaded, rejected rows and rejection rate. This is the operational signal for the pipeline. A data engineer reviewing pipeline health does not read individual log lines - they look at the summary.

Rejection rate is the key metric. Sustained 0% rejection means the source data is consistently clean. A sudden spike to 20% means something changed at the source - a field was removed, a format changed, a business process produced unexpected values. Catching this in the pipeline summary is catching it before it silently corrupts downstream analytics.

---

## Scale Considerations
#### `Compute: parallelism`
The current pipeline is single-threaded and single-process. At scale, the transformation stage can be parallelized using Python's `multiprocessing` module or Dask DataFrames. The load stage can be parallelized with batch inserts and connection pooling.

#### `Orchestration: Airflow`
`main.py` as the runner works for a single pipeline run by a single developer. In production, pipelines needs scheduling, retry logic, dependency management, backfill capability, and alerting. Each stage of this pipeline would become an Airflow task in a DAG:

```
ingest_task >> validate_structure_task >> transform_task >> validate_rows_task >> load_task
```
Each task is independently retriable. If the load task fails, only the load task reruns - not ingestion and transformation again.

#### `Validation: Great Expectations`
The custom validation framework built here is educational. In production, Great Expectations or Soda provides richer checks (statistical distributions, referential integrity, trend detention), built-in documentation generation (data dictionaries, validation reports), and integration with alerting systems.

---
## Known Limitations
`No file delivery detection`. The pipeline assumes the file is present when it runs. In production you would add a file watcher, check for a manifest file confirming delivery completeness, or listen for an S3 event notification.

`No checksum validation`. There is no verification that the file received is identical to the file sent. A partial delivery or network corruption eould produce a valid-looking but incomplete file. Checksums (MD5 or SHA256) provided alongside the file would catch this.

`No schema versioning`. `CREATE TABLE IF NOT EXISTS` cannot apply schema changes to existing tables. This is acceptable for a single-environment project and must be replaced with Alembic before multiple environments or schema evolution are needed.

`Single file only`. The pipeline processed one file per run. Production batch ingestion often involves processing all files in a directory or an S3 prefix, with tracking of which files have been processed to avois reprocessing.

`No unit tests`. Each module should have a corresponding test file that exercises it against known inputs with known expected outputs. The `parse_date` function, `_check_row` function, and upsert behavior are all independently testable without a live database.