"""
Q2 — Producer: Sends green taxi data to Redpanda (Kafka)
Run from your host machine (not inside Docker):
    python producer.py
Make sure the parquet file is in the same folder.
"""

import pandas as pd
import json
from kafka import KafkaProducer
from time import time

# ---- Load and filter only the columns we need ----
print("Loading parquet file...")
df = pd.read_parquet('green_tripdata_2025-10.parquet', columns=[
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
])

print(f"Loaded {len(df)} rows")

# ---- Convert datetime columns to strings ----
# JSON cannot serialize datetime objects — must be strings first
df['lpep_pickup_datetime']  = df['lpep_pickup_datetime'].astype(str)
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

# ---- Fill NaN values (JSON can't serialize NaN either) ----
df = df.fillna(0)

# ---- Connect to Redpanda on localhost:9092 ----
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---- Send all rows and time it ----
t0 = time()

for _, row in df.iterrows():
    producer.send('green-trips', value=row.to_dict())

producer.flush()

t1 = time()
print(f'Sent {len(df)} messages in {(t1 - t0):.2f} seconds')
