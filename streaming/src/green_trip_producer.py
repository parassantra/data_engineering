import pandas as pd
from time import time
from kafka import KafkaProducer
from models_green import GreenTrip, green_trip_from_row, green_trip_serializer

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

# Only read the columns we need
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

df = pd.read_parquet(url, columns=columns)

print(f"Loaded {len(df)} rows")
print(df.head())


# Convert datetime columns to strings in the format 'YYYY-MM-DD HH:MM:SS'
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

print("After conversion:")
print(df.head())
print(df.dtypes)

server = 'localhost:9092'
topic_name = 'green-trips'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=green_trip_serializer
)

print(f"Producer connected to {server}")

t0 = time()

count = 0
for _, row in df.iterrows():
    trip = green_trip_from_row(row)
    producer.send(topic_name, value=trip)
    count += 1
    
    # Print progress every 1000 rows
    if count % 1000 == 0:
        print(f"Sent {count} rows...")

# Important: flush to ensure all messages are sent
producer.flush()

t1 = time()
elapsed = t1 - t0

print(f"\n✅ Sent {count} trips to '{topic_name}' topic")
print(f"⏱️  Took {elapsed:.2f} seconds")