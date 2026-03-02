# Homework: Build Your Own dlt Pipeline

Based on the [dlt Workshop homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/workshops/dlt/dlt_homework.md) from the Data Engineering Zoomcamp 2026.

## Step 1: Create a New Project

Created a `homework` folder inside the existing `workshop` project:

```bash
mkdir -p workshop/homework
cd workshop/homework
```

## Step 2: Set Up the dlt MCP Server

In Cursor, go to **Settings → Tools & MCP → New MCP Server** and add the following configuration:

```json
{
  "mcpServers": {
    "dlt": {
      "command": "uv",
      "args": [
        "run",
        "--with",
        "dlt[duckdb]",
        "--with",
        "dlt-mcp[search]",
        "python",
        "-m",
        "dlt_mcp"
      ]
    }
  }
}
```

This enables the dlt MCP server, giving the AI access to dlt documentation, code examples, and pipeline metadata. Once configured, the following MCP tools become available:

| Tool | Description |
|------|-------------|
| `list_pipelines` | List all available dlt pipelines |
| `list_tables` | List all tables in a pipeline |
| `execute_sql_query` | Run SQL queries against a pipeline's destination |
| `search_docs` | Search dlt documentation |
| `search_code` | Search dlt code examples |
| `display_schema` | Display the schema of a pipeline |

## Step 3: Install dlt

```bash
pip install "dlt[workspace]"
```

This installs dlt along with workspace extras (marimo, ibis, DuckDB, etc.).
You can verify the installation with:

```bash
dlt --version
```

## Step 4: Build the Pipeline

Created `taxi_pipeline.py` with a dlt REST API source for the NYC taxi trip API.

**API details discovered:**

| Property | Value |
|----------|-------|
| Base URL | `https://us-central1-dlthub-analytics.cloudfunctions.net/` |
| Endpoint | `data_engineering_zoomcamp_api` |
| Format | JSON array at root level (no wrapper object) |
| Page Size | 1,000 records per page |
| Total Pages | 10 (10,000 records) |
| Pagination | Page-number based (`?page=N`), starting at page 1, stop on empty page |
| Authentication | None required |

**Key configuration choices:**

- **Paginator**: `page_number` with `base_page=1`, `page_param="page"`, `total_path=None`, `stop_after_empty_page=True`
- **Write disposition**: `replace` (full reload each run)
- **Destination**: DuckDB (local)

## Step 5: Run the Pipeline

```bash
cd workshop/homework
uv run python3 taxi_pipeline.py
```

Pipeline successfully loaded 10,000 rides into DuckDB.

## Step 6: Answers

Answers were obtained by prompting the AI agent in Cursor, which used the dlt MCP `execute_sql_query` tool behind the scenes to query the `taxi_pipeline` destination.

### Question 1: What is the start date and end date of the dataset?

**Answer: 2009-06-01 to 2009-07-01**

Prompt:

> What is the start date and end date of the dataset in the taxi_pipeline?

MCP tool called: `execute_sql_query`

```sql
SELECT MIN(trip_pickup_date_time) AS min_pickup,
       MAX(trip_pickup_date_time) AS max_pickup
FROM rides
```

Agent response:

> The earliest pickup is **2009-06-01** and the latest pickup is **2009-06-30**, so the dataset spans from **2009-06-01 to 2009-07-01**.

---

### Question 2: What proportion of trips are paid with credit card?

**Answer: 26.66%**

Prompt:

> What proportion of trips are paid with credit card in the taxi_pipeline?

MCP tool called: `execute_sql_query`

```sql
SELECT payment_type,
       COUNT(*) AS cnt,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM rides
GROUP BY payment_type
ORDER BY cnt DESC
```

Agent response:

> | payment_type | count | percentage |
> |---|---|---|
> | CASH | 7,235 | 72.35% |
> | Credit | 2,666 | 26.66% |
> | Cash | 97 | 0.97% |
> | No Charge | 1 | 0.01% |
> | Dispute | 1 | 0.01% |
>
> Credit card trips account for **26.66%** of all trips.

---

### Question 3: What is the total amount of money generated in tips?

**Answer: $6,063.41**

Prompt:

> What is the total amount of money generated in tips in the taxi_pipeline?

MCP tool called: `execute_sql_query`

```sql
SELECT ROUND(SUM(tip_amt), 2) AS total_tips
FROM rides
```

Agent response:

> The total tip amount is **$6,063.41**.
