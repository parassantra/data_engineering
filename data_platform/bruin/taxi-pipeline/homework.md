# Module 5 Homework: Data Platforms with Bruin

## Setup

### 1. Install Bruin CLI

```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

After installation, restarted the terminal and verified with `bruin version`.

### 2. Initialize the Zoomcamp Template

```bash
bruin init zoomcamp taxi-pipeline
cd taxi-pipeline
```

This generated the project skeleton with TODO-laden template files:

```
taxi-pipeline/
├── .bruin.yml
├── .gitignore
└── pipeline/
    ├── pipeline.yml
    └── assets/
        ├── ingestion/
        │   ├── trips.py
        │   ├── requirements.txt
        │   ├── payment_lookup.asset.yml
        │   └── payment_lookup.csv
        ├── staging/
        │   └── trips.sql
        └── reports/
            └── trips_report.sql
```

### 3. Configure `.bruin.yml`

Set up the DuckDB connection in the project root:

```yaml
default_environment: default

environments:
  default:
    connections:
      duckdb:
        - name: duckdb-default
          path: duckdb.db
```

### 4. Configure `pipeline.yml`

Cleaned up the template comments and set the pipeline config:

```yaml
name: nyc_taxi
schedule: daily
start_date: "2022-01-01"
default_connections:
  duckdb: duckdb-default
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow"]
```

### 5. Build the Ingestion Layer

**`trips.py`** — Python asset using Bruin's materialization approach (Approach 1). The `materialize()` function fetches Parquet files from the NYC TLC endpoint, tags rows with `taxi_type` and `extracted_at`, and returns a DataFrame. Bruin handles writing it to DuckDB.

**`payment_lookup.asset.yml`** — Seed asset that loads `payment_lookup.csv` (7 payment type mappings) into DuckDB.

**`requirements.txt`** — Pinned dependencies: `pandas`, `requests`, `pyarrow`, `python-dateutil`.

### 6. Build the Staging Layer

**`staging/trips.sql`** — Joins raw trips with payment lookup, filters by date range using `{{ start_datetime }}` / `{{ end_datetime }}`, and deduplicates using `QUALIFY ROW_NUMBER()` over a composite key.

### 7. Build the Reports Layer

**`reports/trips_report.sql`** — Aggregates staging data by date, taxi type, and payment type. Produces `trip_count`, `total_fare`, and `avg_fare`.

### 8. Validate and Run

```bash
bruin validate ./taxi-pipeline/pipeline/pipeline.yml
bruin run ./taxi-pipeline/pipeline/pipeline.yml --start-date 2022-01-01 --end-date 2022-02-01
```

For a full refresh (drops and recreates all tables from scratch):

```bash
bruin run ./pipeline/pipeline.yml --full-refresh
```

To verify results by querying the tables directly:

```bash
bruin query --connection duckdb-default --query "SELECT COUNT(*) FROM ingestion.trips"
```

---

## Roadblocks and Solutions

### Roadblock 1: `uv` Installation Permission Error

**Error:** When running `bruin validate`, the `uv` package manager installer tried to write to `~/.bash_profile` and `~/.config/fish/conf.d/` but got `Permission denied`. This cascaded into Bruin failing to register the `duckdb-default` connection.

**Fix:** Ran `chmod u+w ~/.bash_profile` to grant write permission, then re-ran the command. Alternatively, `export PATH="$HOME/.bruin:$PATH"` works since the `uv` binary was already installed to `~/.bruin/`.

### Roadblock 2: DuckDB File Lock Conflict

**Error:** Both `ingestion.trips` and `ingestion.payment_lookup` ran in parallel. DuckDB only allows one writer at a time, so `payment_lookup` failed with:

> Could not set lock on file "duckdb.db": Conflicting lock is held in python3.11

**Fix:** Added `depends: ingestion.payment_lookup` to the `trips.py` YAML header. This serializes execution: the tiny CSV seed loads first (~1s), then the big Parquet download starts (~38s). No more lock conflict.

### Roadblock 3: `time_interval` Strategy Fails on First Run

**Error:** The `time_interval` materialization strategy tries to `DELETE FROM staging.trips` before inserting, but on a fresh DuckDB database the table doesn't exist yet:

> Catalog Error: Table with name trips does not exist!

**Fix:** Changed both `staging/trips.sql` and `reports/trips_report.sql` from `strategy: time_interval` to `strategy: create+replace`. This creates the table if it doesn't exist and replaces its contents on each run. Works on first run without needing `--full-refresh`.

**Tradeoff:** `create+replace` does a full refresh each run (no incremental benefit). For a learning pipeline this is fine. In production, you'd use `time_interval` with `--full-refresh` on the initial run.

### Roadblock 4: Column Name Mismatch (Parquet vs DuckDB)

**Error:** The template used generic column names like `pickup_datetime`, `PULocationID`, but:

1. The actual NYC yellow taxi Parquet columns use `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`.
2. Bruin uses dlt under the hood, which normalizes all column names to **snake_case lowercase** when loading into DuckDB. So `PULocationID` became `pu_location_id`.

> Binder Error: Table "t" does not have a column named "pickup_datetime"
> Candidate bindings: "tpep_pickup_datetime"

**Fix:** Queried the actual DuckDB schema to get the real column names:

```
tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id,
do_location_id, fare_amount, payment_type, taxi_type, ...
```

Updated all SQL references across `staging/trips.sql` and `reports/trips_report.sql` to match.

### Roadblock 5: `--full-refresh` Without Date Range Tries to Download All Months

**Error:** Running `bruin run --full-refresh` without `--start-date` / `--end-date` caused Bruin to use the pipeline's `start_date` (2022-01-01) through yesterday (2026-02-27). The `materialize()` function then tried to download Parquet files for every month in that 4+ year range. NYC TLC data is only available up to November 2025, so it failed on December 2025:

> 403 Client Error: Forbidden for url: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-12.parquet

**Fix:** Always specify the date range when running the pipeline:

```bash
bruin run ./taxi-pipeline/pipeline/pipeline.yml --full-refresh --start-date 2022-01-01 --end-date 2022-02-01
```

For development, use 1-3 months to keep downloads fast. Only expand the range once the pipeline is fully tested.

---

## Homework Answers

### Question 1. Bruin Pipeline Structure

> In a Bruin project, what are the required files/directories?

**Answer: `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`**

The README states the required parts of a Bruin project are:
- `.bruin.yml` in the root directory (connection and environment config)
- `pipeline.yml` in the `pipeline/` directory (pipeline name, schedule, variables)
- `assets/` folder next to `pipeline.yml` (containing Python, SQL, and YAML asset files)

This matches the project structure generated by `bruin init zoomcamp`.

---

### Question 2. Materialization Strategies

> Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

**Answer: `time_interval` - incremental based on a time column**

The `time_interval` strategy is specifically designed for time-based incremental loads. It works by:
1. Deleting existing rows where the `incremental_key` falls within the run's date range
2. Inserting the new rows from the query for that range

This is exactly "deleting and inserting data for that time period." The other options don't fit: `append` never deletes, `replace` rebuilds the entire table, and `view` doesn't store data at all.

---

### Question 3. Pipeline Variables

> How do you override the `taxi_types` array variable to only process yellow taxis?

**Answer: `bruin run --var 'taxi_types=["yellow"]'`**

Since `taxi_types` is defined as an `array` type in `pipeline.yml`, the override must pass a valid JSON array string. The `--var` flag accepts `key=value` pairs where the value follows the variable's schema. The README explicitly shows this pattern:

```bash
bruin run ./pipeline/pipeline.yml --var 'taxi_types=["yellow"]'
```

---

### Question 4. Running with Dependencies

> You've modified `ingestion/trips.py` and want to run it plus all downstream assets. Which command should you use?

**Answer: `bruin run ingestion/trips.py --downstream`**

The `--downstream` flag tells Bruin to run the specified asset and then all assets that depend on it (directly or transitively). In our pipeline, running `trips.py --downstream` would execute:

1. `ingestion.trips` (the specified asset)
2. `staging.trips` (depends on `ingestion.trips`)
3. `reports.trips_report` (depends on `staging.trips`)

---

### Question 5. Quality Checks

> You want to ensure `pickup_datetime` never has NULL values. Which quality check should you add?

**Answer: `name: not_null`**

The `not_null` check validates that a column contains no NULL values. In our pipeline, we used it on `tpep_pickup_datetime` in `staging/trips.sql`:

```yaml
columns:
  - name: tpep_pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null
```

The other options don't fit: `unique` checks for duplicates, `positive` checks for positive numbers, and `accepted_values` checks against a whitelist.

---

### Question 6. Lineage and Dependencies

> Which Bruin command should you use to visualize the dependency graph?

**Answer: `bruin lineage`**

The `bruin lineage <path>` command shows the upstream (what an asset depends on) and downstream (what depends on the asset) relationships. For example:

```bash
bruin lineage ./pipeline/assets/ingestion/trips.py
```

This would show that `ingestion.trips` depends on `ingestion.payment_lookup` (upstream) and that `staging.trips` depends on it (downstream).

---

### Question 7. First-Time Run

> What flag should you use to ensure tables are created from scratch on a new DuckDB database?

**Answer: `--full-refresh`**

The `--full-refresh` flag tells Bruin to drop and recreate tables instead of using incremental strategies. This is necessary on first runs because incremental strategies like `time_interval` attempt to `DELETE FROM` a table that doesn't exist yet. The README recommends:

```bash
bruin run ./pipeline/pipeline.yml --full-refresh --start-date 2022-01-01 --end-date 2022-02-01
```

After the initial run creates the tables, subsequent runs can use incremental strategies without this flag.
