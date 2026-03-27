"""
Q3 — Consumer: Counts trips with distance > 5.0 km
Run from your host machine (not inside Docker):
    python consumer.py
"""

import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',         # read from the very beginning of the topic
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=15000             # stop after 15s of silence (topic exhausted)
)

print("Reading from green-trips topic...")

count_over_5 = 0
total = 0

for message in consumer:
    trip = message.value
    total += 1
    if trip.get('trip_distance', 0) > 5.0:
        count_over_5 += 1

    if total % 5000 == 0:
        print(f"  ...processed {total} messages so far")

print(f"\nTotal trips read       : {total}")
print(f"Trips with distance >5 : {count_over_5}")
