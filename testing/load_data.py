import pandas as pd
from sqlalchemy import create_engine

# Database connection string
# Format: postgresql://username:password@host:port/database
DB_URL = "postgresql://myuser:mypass@localhost:5432/mydb"

# Create engine (the bridge to database)
engine = create_engine(DB_URL)

# Read CSV with pandas
df = pd.read_parquet("/workspaces/data-engineering/homework_1_docker_and_terraform/green_tripdata_2025-11.parquet")

pf = pd.read_csv("/workspaces/data-engineering/homework_1_docker_and_terraform/taxi_zone_lookup.csv")

# Push to database (creates table automatically!)
df.to_sql(
    name='nyc_taxi',           # table name
    con=engine,            # connection
    if_exists='replace',   # replace if exists
    index=False            # don't save pandas index
)

pf.to_sql(
    name='taxi_lookup',           # table name
    con=engine,            # connection
    if_exists='replace',   # replace if exists
    index=False            # don't save pandas index
)

print("✅ Data loaded successfully!")