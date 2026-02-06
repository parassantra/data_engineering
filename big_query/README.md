# DE Zoomcamp 2026 — Module 3 Homework (BigQuery + GCS)

My solution for **Module 3 Homework: Data Warehousing & BigQuery** using **NYC Yellow Taxi** data for **January 2024 – June 2024** (6 parquet files). 

---

## Tech stack

- Google Cloud Storage (GCS)
- BigQuery (BQ)
- Python script (optional): `load_yellow_taxi_data.py`

---

## Dataset & resources

- Source data: NYC TLC Yellow Taxi trip records (parquet), **Jan 2024 – Jun 2024**
- Files expected in bucket:  
  `yellow_tripdata_2024-01.parquet` … `yellow_tripdata_2024-06.parquet`

---

## My environment values (update if you fork)

- **Project:** `de-2026-workshop`
- **BigQuery dataset:** `de_zoomcamp` *(location: `us-central1`)*
- **GCS bucket:** `demo-bucket`

> Important: BigQuery jobs must run in the **same location as the dataset**. My dataset is `us-central1`, so I run queries in `us-central1` (BigQuery UI → Query settings → Processing location).

---

# 1) Setup & Authentication

You can authenticate either with a **Service Account** or **Google SDK (`gcloud`)**.  
I used **gcloud login**.

### 1.1 gcloud auth

```bash
gcloud auth login
gcloud config set project de-2026-workshop
gcloud auth application-default login
```

---

# 2) Load data into GCS (Jan–Jun 2024)

The homework provides:
- Python script: `load_yellow_taxi_data.py`
- Notebook: `DLT_upload_to_GCP.ipynb`

## Option A — Python script (recommended)

1) Edit bucket name in `load_yellow_taxi_data.py`:

```py
BUCKET_NAME = "demo-bucket"
```

2) If using **gcloud auth**, comment out service account lines and use ADC client:

```py
# CREDENTIALS_FILE = "gcs.json"
# client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

client = storage.Client()
```

(The script notes this is expected if using Google SDK auth.)

3) Run:

```bash
python load_yellow_taxi_data.py
```

## Verify all 6 files exist (required)

Homework says: make sure all 6 files show in your bucket before beginning BigQuery.

```bash
gsutil ls -l gs://demo-bucket/yellow_tripdata_2024-*.parquet
```

---

# 3) BigQuery Setup

Homework requirements:
1) Create an **external table** from Yellow Taxi trip records (PARQUET required).  
2) Create a **regular/materialized** table (no partition, no clustering).

## 3.1 Create external table (PARQUET)

> Note: Use the PARQUET option when creating an external table.

**Steps to run in BigQuery:**

1. Go to [BigQuery Console](https://console.cloud.google.com/bigquery)
2. Select your project (`de-2026-workshop`) from the dropdown
3. Click **+ Compose new query** (or use the query editor)
4. Set the processing location:
   - Click **More** → **Query settings**
   - Under **Processing location**, select `us-central1` (must match your dataset location)
   - Click **Save**
5. Paste the SQL query below and click **Run**

```sql
CREATE OR REPLACE EXTERNAL TABLE `de-2026-workshop.de_zoomcamp.yellow_taxi_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://demo-bucket/yellow_tripdata_2024-*.parquet']
);
```

Sanity check:

```sql
SELECT * FROM `de-2026-workshop.de_zoomcamp.yellow_taxi_ext` LIMIT 10;
```

## 3.2 Create regular/materialized table (no partition/cluster)

```sql
CREATE OR REPLACE TABLE `de-2026-workshop.de_zoomcamp.yellow_taxi` AS
SELECT *
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi_ext`;
```

Sanity check:

```sql
SELECT COUNT(*) AS cnt
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi`;
```

---

# 4) Homework questions + SQL + Answers

## Question 1. Counting records

**Query**

```sql
SELECT COUNT(*) AS cnt
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi`;
```

**Answer**
- ✅ **20,332,093**

---

## Question 2. Data read estimation (External vs Materialized)

**Query (External table)**

```sql
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi_ext`;
```

**Query (Materialized table)**

```sql
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi`;
```

**Answer option (matches homework choices)**
- ✅ **0 MB for the External Table and 155.12 MB for the Materialized Table**

**Reasoning**
- BigQuery's *estimate* for external tables can show **0 MB** because external data lives in GCS and the planner often can't (or doesn't) compute a reliable "bytes processed" estimate before execution.
- The materialized table is stored in BigQuery storage, so the estimate is clearer and typically non-zero.

---

## Question 3. Understanding columnar storage

**Query (1 column)**

```sql
SELECT PULocationID
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi`;
```
→ This query will process **155.12 MB** when run.

**Query (2 columns)**

```sql
SELECT PULocationID, DOLocationID
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi`;
```
→ This query will process **310.24 MB** when run (double, because 2 columns).

**Answer**
- ✅ BigQuery is columnar; selecting more columns requires scanning more column data, increasing bytes processed.

---

## Question 4. Counting zero fare trips

**Query**

```sql
SELECT COUNT(*) AS zero_fare
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi`
WHERE fare_amount = 0;
```

**My result**
- ✅ **8,333**  
*(This is one of the provided answer options.)*

---

## Question 5. Partitioning and clustering strategy

Goal: optimize for queries that:
- filter on `tpep_dropoff_datetime`
- order by `VendorID`

**Create optimized table**

```sql
CREATE OR REPLACE TABLE `de-2026-workshop.de_zoomcamp.yellow_taxi_part_clust`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi`;
```

**Answer**
- ✅ Partition by `tpep_dropoff_datetime` and cluster on `VendorID`

---

## Question 6. Partition benefits (estimate bytes processed)

Date range: **2024-03-01 to 2024-03-15 (inclusive)**

**Query (non-partitioned)**

```sql
SELECT DISTINCT VendorID
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime <  '2024-03-16';
```

**Query (partitioned+clustered)**

```sql
SELECT DISTINCT VendorID
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi_part_clust`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime <  '2024-03-16';
```

**Answer option (most closely matches homework choices)**
- ✅ **310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**

**Reasoning**
- Partition pruning: only partitions that match the date range are scanned.
- Clustering further improves read efficiency by co-locating similar `VendorID` values.

---

## Question 7. External table storage

**Answer**
- ✅ **GCP Bucket**

---

## Question 8. Clustering best practices

**Answer**
- ✅ **False**

**Reasoning**
- Clustering helps only when it matches access patterns; otherwise it adds complexity without benefit.

---

## Question 9. Understanding table scans (No points)

**Query**

```sql
SELECT COUNT(*)
FROM `de-2026-workshop.de_zoomcamp.yellow_taxi`;
```

**Observation**
- Estimated bytes may show **0** in some cases.

**Reasoning (why 0 can happen)**
- BigQuery may answer `COUNT(*)` from table metadata/statistics without scanning data blocks.
- If query results are cached, the UI can also report 0 bytes processed.

To test caching effects:
- BigQuery UI → Query settings → disable **Use cached results**
- Re-run the query.

---

# 5) Submission

Homework requires a public repo link; SQL/shell can be included directly in README.

Submission form: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw3

---

## Notes / gotchas I hit

- **Dataset location matters**: `de_zoomcamp` is in `us-central1`, so queries must run in `us-central1`.
- BigQuery UI external table creation can be confusing for URI patterns; using SQL DDL is reliable.
- zsh wildcard expansion: quote patterns in terminal:
  ```bash
  gsutil ls -l 'gs://demo-bucket/yellow_tripdata_2024-*.parquet'
  ```
