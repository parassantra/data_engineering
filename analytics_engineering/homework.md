# Module 4 Homework: Analytics Engineering

## Question 1. dbt Lineage and Execution

**Given a dbt project with the following structure:**

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

**If you run `dbt run --select int_trips_unioned`, what models will be built?**

**Answer: `int_trips_unioned` only**

`dbt run --select <model>` without any graph operators builds **only** the explicitly named model. It does not automatically include upstream parents or downstream children.

To include related models, you need the `+` graph operator:

| Command | What gets built |
|---|---|
| `--select int_trips_unioned` | Only `int_trips_unioned` |
| `--select +int_trips_unioned` | Upstream deps + `int_trips_unioned` |
| `--select int_trips_unioned+` | `int_trips_unioned` + downstream dependents |
| `--select +int_trips_unioned+` | Upstream + model + downstream |

**Verification:**

```
$ dbt run --select int_trips_unioned --target prod

1 of 1 START sql table model prod.int_trips_unioned .... [RUN]
1 of 1 OK created sql table model prod.int_trips_unioned [OK]
Done. PASS=1 TOTAL=1
```

Only 1 model was built — confirming the answer.

---

## Question 2. dbt Tests

**You've configured a generic test like this in your `schema.yml`:**

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

**Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data. What happens when you run `dbt test --select fct_trips`?**

**Answer: dbt will fail the test, returning a non-zero exit code**

The `accepted_values` test queries the **actual data in the warehouse** — it doesn't matter that the model SQL hasn't changed. Under the hood, the compiled SQL looks like:

```sql
select count(*) as failures
from (
    select payment_type
    from prod.fct_trips
    where payment_type not in (1, 2, 3, 4, 5)
) validation_errors
```

When value `6` appears, `failures > 0`, and dbt reports an **ERROR** with a non-zero exit code. Key points:

- dbt tests run against **data**, not model code — new data triggers failures even if nothing in the project changed.
- dbt never auto-updates configuration or silently ignores failures.
- There is no built-in "warning" behavior for `accepted_values` unless you explicitly configure `severity: warn`.

**Verification:**

We added the `accepted_values` test on `payment_type` to `fct_trips` and confirmed all current values are within `[1, 2, 3, 4, 5]` (PASS). Then, to simulate the scenario, we temporarily narrowed `service_type` accepted values from `['Green', 'Yellow']` to just `['Green']`:

```
$ dbt test --select fct_trips --target prod

1 of 10 FAIL 1 accepted_values_fct_trips_service_type__Green ... [FAIL 1]
  Got 1 result, configured to fail if != 0

Done. PASS=8 ERROR=1 TOTAL=10
```

The test failed with exit code 1 — confirming the answer.

---

## Question 3. Counting Records in `fct_monthly_zone_revenue`

**What is the count of records in the `fct_monthly_zone_revenue` model?**

**Answer: 12,184**

```sql
select count(*) as record_count
from {{ ref('fct_monthly_zone_revenue') }}
```

```
| record_count |
| ------------ |
|        12184 |
```

---

## Question 4. Best Performing Zone for Green Taxis (2020)

**Using the `fct_monthly_zone_revenue` table, find the pickup zone with the highest total revenue (`revenue_monthly_total_amount`) for Green taxi trips in 2020.**

**Answer: East Harlem North**

```sql
select
    pickup_zone,
    sum(revenue_monthly_total_amount) as total_revenue
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and revenue_month >= '2020-01-01'
  and revenue_month < '2021-01-01'
group by pickup_zone
order by total_revenue desc
```

```
| pickup_zone          | total_revenue |
| -------------------- | ------------- |
| East Harlem North    |  1,817,256.45 |
| East Harlem South    |  1,653,017.11 |
| Central Harlem       |  1,097,208.92 |
| Washington Heights S |    880,202.00 |
| Morningside Heights  |    764,388.64 |
```

East Harlem North had the highest Green taxi revenue in 2020 at **$1,817,256.45**.

---

## Question 5. Green Taxi Trip Counts (October 2019)

**What is the total number of trips (`total_monthly_trips`) for Green taxis in October 2019?**

**Answer: 384,624**

```sql
select sum(total_monthly_trips) as total_trips
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green'
  and revenue_month = '2019-10-01'
```

```
| total_trips |
| ----------- |
|      384624 |
```

---

## Question 6. Build a Staging Model for FHV Data

**Create a staging model `stg_fhv_tripdata` for the For-Hire Vehicle (FHV) trip data for 2019. Filter out records where `dispatching_base_num IS NULL` and rename fields to match naming conventions. What is the count of records?**

**Answer: 43,244,693**

### Steps taken

**1. Loaded FHV 2019 data:**

Downloaded all 12 months of FHV 2019 data from [DataTalksClub/nyc-tlc-data](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv), converted CSV.gz files to Parquet, and loaded them into `prod.fhv_tripdata` in DuckDB (43,244,696 raw rows).

**2. Added source definition** in `models/staging/sources.yml`:

```yaml
- name: fhv_tripdata
  description: Raw For-Hire Vehicle (FHV) trip records
  loaded_at_field: pickup_datetime
```

**3. Created `models/staging/stg_fhv_tripdata.sql`:**

```sql
with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(affiliated_base_number as string) as affiliated_base_num,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(sr_flag as integer) as shared_ride_flag
    from source
    where dispatching_base_num is not null
)

select * from renamed
```

**4. Result:**

```
| record_count |
| ------------ |
|     43244693 |
```

The raw table had 43,244,696 rows. After filtering out 3 records where `dispatching_base_num IS NULL`, the staging model contains **43,244,693** records.
