from kafka import KafkaConsumer
from models_green import green_trip_deserializer
import json

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-counter',
    value_deserializer=green_trip_deserializer,
    consumer_timeout_ms=5000  # Exit after 5 seconds of no messages
)

print(f"Consumer connected to {server}")
print(f"Reading from topic: {topic_name}")
print(f"Setting auto_offset_reset to 'earliest'\n")

count_total = 0
count_distance_gt_5 = 0

for message in consumer:
    trip = message.value
    
    count_total += 1
    
    if trip.trip_distance > 5.0:
        count_distance_gt_5 += 1
    
    # Print progress every 1000 rows
    if count_total % 1000 == 0:
        print(f"Processed {count_total} trips, {count_distance_gt_5} have trip_distance > 5.0")

print(f"\n✅ Total trips processed: {count_total}")
print(f"✅ Trips with trip_distance > 5.0: {count_distance_gt_5}")
