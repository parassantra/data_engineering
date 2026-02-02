# Module 2 — Kestra Backfill & BigQuery Validation

This file contains the solution and notes for Module 2 Homework (workflow orchestration + ETL using Kestra) using the NYC TLC taxi datasets. The goal was to backfill and validate data for 2021 (available months: 2021-01 → 2021-07) for both Green and Yellow taxi datasets.

## Assignment Summary
- Extend existing Kestra flows to include data for 2021 (January through July).
- Run Kestra backfill for both `green` and `yellow` taxi inputs using the scheduled flow.
- Validate ingestion results with BigQuery queries against consolidated tables.

## Datasets

- Green taxi releases: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download
- Wget prefix (green example): https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/
- (Yellow taxi has equivalent structure)

Note: Only months 2021-01 through 2021-07 are available in the dataset.

## How I Performed the Backfill (Kestra UI)
Repeat the following for both `green` and `yellow`:

1. Open the Kestra UI and go to `Flows`.
2. Open the scheduled taxi ingestion flow (the flow already handling 2019/2020).
3. Click `Triggers` → find the `Schedule` trigger → Click `Backfill`.
4. Set the time window:
	 - Start: `2021-01-01`
	 - End: `2021-07-31`
5. Set the flow input `taxi`/`service` to `green` (first run) then `yellow` (second run).
6. Start the backfill and wait for all executions to complete. Kestra will run the flow for each scheduled interval.

## Validation (BigQuery)
I validated row counts by querying consolidated main tables in BigQuery:
- `de-2026-workshop.demo_dataset.green_tripdata`
- `de-2026-workshop.demo_dataset.yellow_tripdata`

Example queries used (replace project/dataset as needed):

Yellow 2020 total rows:
```
SELECT COUNT(*) AS row_count
FROM `de-2026-workshop.demo_dataset.yellow_tripdata`
WHERE tpep_pickup_datetime >= TIMESTAMP("2020-01-01 00:00:00")
	AND tpep_pickup_datetime <  TIMESTAMP("2021-01-01 00:00:00");
```

Green 2020 total rows:
```
SELECT COUNT(*) AS row_count
FROM `de-2026-workshop.demo_dataset.green_tripdata`
WHERE lpep_pickup_datetime >= TIMESTAMP("2020-01-01 00:00:00")
	AND lpep_pickup_datetime <  TIMESTAMP("2021-01-01 00:00:00");
```

Yellow March 2021 rows:
```
SELECT COUNT(*) AS row_count
FROM `de-2026-workshop.demo_dataset.yellow_tripdata`
WHERE tpep_pickup_datetime >= TIMESTAMP("2021-03-01 00:00:00")
	AND tpep_pickup_datetime <  TIMESTAMP("2021-04-01 00:00:00");
```

## Quiz Answers / Validation Results
- Uncompressed file size for `yellow_tripdata_2020-12.csv`: (selected based on Kestra execution output — choose the closest option in your UI)
- Rendered `file` value for `taxi=green, year=2020, month=04`: `green_tripdata_2020-04.csv`
- Yellow 2020 total rows: `24,648,499`
- Green 2020 total rows: `1,734,051`
- Yellow March 2021 rows: `1,925,152`
- Schedule trigger timezone configuration: `America/New_York` (handles DST correctly)