"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

depends:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append

columns:
  - name: tpep_pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: tpep_dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"

@bruin"""

import os
import json
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

def materialize():
    start_date = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d")
    end_date = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d")
    taxi_types = json.loads(os.environ.get("BRUIN_VARS", "{}")).get("taxi_types", ["yellow"])

    frames = []
    current = start_date
    while current < end_date:
        year_month = current.strftime("%Y-%m")
        for taxi_type in taxi_types:
            url = f"{BASE_URL}{taxi_type}_tripdata_{year_month}.parquet"
            response = requests.get(url)
            response.raise_for_status()
            df = pd.read_parquet(BytesIO(response.content))
            df["taxi_type"] = taxi_type
            frames.append(df)

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    if not frames:
        return pd.DataFrame()

    final_df = pd.concat(frames, ignore_index=True)
    final_df["extracted_at"] = datetime.utcnow()

    return final_df
