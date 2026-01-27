import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://myuser:mypass@localhost:5432/mydb"
engine = create_engine(DB_URL)

# Step 1: Create table manually with SQL
create_table_query = """
CREATE TABLE IF NOT EXISTS yellow_taxi_data (
    "VendorID" BIGINT,
    lpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
    lpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
    passenger_count BIGINT,
    trip_distance FLOAT(53),
    "RatecodeID" BIGINT,
    store_and_fwd_flag TEXT,
    "PULocationID" BIGINT,
    "DOLocationID" BIGINT,
    payment_type BIGINT,
    fare_amount FLOAT(53),
    extra FLOAT(53),
    mta_tax FLOAT(53),
    tip_amount FLOAT(53),
    tolls_amount FLOAT(53),
    improvement_surcharge FLOAT(53),
    total_amount FLOAT(53),
    congestion_surcharge FLOAT(53)
)
"""

with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()
    print("✅ Table created!")

# Step 2: Load CSV and insert data
df = pd.read_parquet("/workspaces/data-engineering/homework_1_docker_and_terraform/green_tripdata_2025-11.parquet")
df.to_sql(
    name='yellow_taxi_data',
    con=engine,
    if_exists='append',     # append to existing table
    index=False
)

print("✅ Data loaded!")