# Module 6 Homework — Batch Processing with PySpark
**DataTalksClub Data Engineering Zoomcamp 2026**

---

## Setup

**Versions used:**
- PySpark: `4.1.1`
- Java: `17` or `21` (Java 11 not supported in Spark 4.x)
- Python: `3.10+`

**Install dependencies (using uv):**
```bash
uv add pyspark==4.1.1
```

Or to sync the existing workspace:
```bash
uv sync
```

**Download data:**
```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

---

## Question 1: Spark Version

**Steps:**
1. Create a local SparkSession
2. Print `spark.version`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("homework_2026") \
    .getOrCreate()

print(spark.version)
```

**Output:**
```
4.1.1
```

**Answer: `4.1.1`**

---

## Question 2: Yellow November 2025 — Average Parquet File Size

**Steps:**
1. Read the parquet file into a DataFrame
2. Repartition to 4 partitions
3. Write to disk as parquet
4. Check the average size of the 4 output part files

```python
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
print(f"Total rows: {df.count()}")

df.repartition(4).write.parquet(
    "output/yellow_tripdata_2025-11_repartitioned",
    mode="overwrite"
)
```

```python
import os

output_dir = "output/yellow_tripdata_2025-11_repartitioned"
part_files = [f for f in os.listdir(output_dir) if f.startswith("part-")]

for f in sorted(part_files):
    size_mb = os.path.getsize(os.path.join(output_dir, f)) / (1024 * 1024)
    print(f"{f}: {size_mb:.1f} MB")

avg_mb = sum(os.path.getsize(os.path.join(output_dir, f)) for f in part_files) / len(part_files) / (1024 * 1024)
print(f"\nAverage size: {avg_mb:.1f} MB")
```

**Output:**
```
part-00000-....snappy.parquet: 24.4 MB
part-00001-....snappy.parquet: 24.4 MB
part-00002-....snappy.parquet: 24.4 MB
part-00003-....snappy.parquet: 24.4 MB

Average size: 24.4 MB
```

**Answer: `25MB`** (24.4 MB is closest to 25MB)

---

## Question 3: Count Trips on November 15

**Steps:**
1. Filter rows where `tpep_pickup_datetime` date equals `2025-11-15`
2. Count the matching rows

```python
from pyspark.sql.functions import col, to_date

count_nov15 = df.filter(to_date(col("tpep_pickup_datetime")) == "2025-11-15").count()
print(f"Trips on Nov 15: {count_nov15}")
```

**Output:**
```
Trips on Nov 15: 162604
```

**Answer: `162,604`**

---

## Question 4: Longest Trip Duration in Hours

**Steps:**
1. Compute duration = `dropoff - pickup` in seconds using `unix_timestamp`
2. Divide by 3600 to convert to hours
3. Find the maximum

```python
from pyspark.sql.functions import col, unix_timestamp, max as spark_max

longest_trip_hours = df.select(
    ((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600).alias("duration_hours")
).select(spark_max("duration_hours")).collect()[0][0]

print(f"Longest trip: {longest_trip_hours:.1f} hours")
```

**Output:**
```
Longest trip: 90.6 hours
```

**Answer: `90.6`**

---

## Question 5: Spark UI Port

**Knowledge question — no code needed.**

Spark's web UI runs on port **4040** by default.

Verify while SparkSession is active by opening: `http://localhost:4040`

**Answer: `4040`**

---

## Question 6: Least Frequent Pickup Location Zone

**Steps:**
1. Read `taxi_zone_lookup.csv` with headers
2. Left join taxi data with zone lookup on `PULocationID = LocationID`
3. Group by `Zone`, count trips per zone
4. Sort ascending and take the top 1 (fewest trips)

```python
zones_df = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")

joined_df = df.join(zones_df, df.PULocationID == zones_df.LocationID, "left")

least_frequent = (
    joined_df
    .groupBy("Zone")
    .count()
    .orderBy("count")
    .limit(1)
)

least_frequent.show(truncate=False)
```

**Output:**
```
+---------------------------------------------+-----+
|Zone                                         |count|
+---------------------------------------------+-----+
|Governor's Island/Ellis Island/Liberty Island|1    |
+---------------------------------------------+-----+
```

**Answer: `Governor's Island/Ellis Island/Liberty Island`**

---

## Summary of Answers

| Question | Answer |
|----------|--------|
| Q1 — Spark version | `4.1.1` |
| Q2 — Average parquet file size | `25MB` |
| Q3 — Trips on Nov 15 | `162,604` |
| Q4 — Longest trip in hours | `90.6` |
| Q5 — Spark UI port | `4040` |
| Q6 — Least frequent pickup zone | `Governor's Island/Ellis Island/Liberty Island` |
