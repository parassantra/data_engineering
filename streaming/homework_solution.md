# Homework 7: Streaming with Kafka and PyFlink - Solutions

## Setup

```bash
cd streaming/
docker compose build
docker compose up -d
```

Services:
- Redpanda (Kafka-compatible broker) on `localhost:9092`
- Flink Job Manager at http://localhost:8081
- Flink Task Manager
- PostgreSQL on `localhost:5432` (user: `postgres`, password: `postgres`)

---

## Question 1. Redpanda version

```bash
docker compose exec redpanda rpk version
```

**Answer: v25.3.9**

---

## Question 2. Sending data to Redpanda

Create the topic:
```bash
docker compose exec redpanda rpk topic create green-trips
```

Producer code (`src/green_trip_producer.ipynb`):

```python
import pandas as pd
from time import time
from kafka import KafkaProducer
from models_green import green_trip_from_row, green_trip_serializer

columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]

df = pd.read_parquet('green_tripdata_2025-10.parquet', columns=columns)

server = 'localhost:9092'
topic_name = 'green-trips'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=green_trip_serializer
)

t0 = time()

for idx, row in df.iterrows():
    trip = green_trip_from_row(row)
    producer.send(topic_name, value=trip)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

**Answer: 60 seconds**

---

## Question 3. Consumer - trip distance

Consumer code (`src/green_trip_consumer.py`):

```python
from kafka import KafkaConsumer
from models_green import green_trip_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-counter',
    value_deserializer=green_trip_deserializer,
    consumer_timeout_ms=5000
)

count_total = 0
count_distance_gt_5 = 0

for message in consumer:
    trip = message.value
    count_total += 1
    if trip.trip_distance > 5.0:
        count_distance_gt_5 += 1

print(f"Total trips processed: {count_total}")
print(f"Trips with trip_distance > 5.0: {count_distance_gt_5}")
```

**Answer: 8506**

---

## Question 4. Tumbling window - pickup location

Create PostgreSQL table:
```sql
CREATE TABLE IF NOT EXISTS trips_per_location (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
```

Submit Flink job (`src/job/question4_tumbling_window.py`):
```bash
docker compose exec jobmanager flink run -py /opt/src/job/question4_tumbling_window.py
```

Query results:
```sql
SELECT PULocationID, num_trips
FROM trips_per_location
ORDER BY num_trips DESC
LIMIT 3;
```

**Answer: 74**

---

## Question 5. Session window - longest streak

Create PostgreSQL table:
```sql
CREATE TABLE IF NOT EXISTS sessions_per_location (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);
```

Submit Flink job (`src/job/question5_session_window.py`):
```bash
docker compose exec jobmanager flink run -py /opt/src/job/question5_session_window.py
```

Query results:
```sql
SELECT PULocationID, num_trips, window_start, window_end
FROM sessions_per_location
ORDER BY num_trips DESC
LIMIT 1;
```

**Answer: 51**

---

## Question 6. Tumbling window - largest tip

Create PostgreSQL table:
```sql
CREATE TABLE IF NOT EXISTS tips_per_hour (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_tips DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end)
);
```

Submit Flink job (`src/job/question6_tumbling_window.py`):
```bash
docker compose exec jobmanager flink run -py /opt/src/job/question6_tumbling_window.py
```

Query results:
```sql
SELECT window_start, window_end, total_tips
FROM tips_per_hour
ORDER BY total_tips DESC
LIMIT 1;
```

**Answer: 2025-10-16 18:00:00**
