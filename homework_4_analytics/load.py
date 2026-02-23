import duckdb
import urllib.request
import os

DB_PATH = '/workspaces/data-engineering/homework_4_analytics/newdbt/newdbt_prj/newdbt_prj.duckdb'
DATA_PATH = '/workspaces/data-engineering/homework_4_analytics/newdbt/newdbt_prj/data'

con = duckdb.connect(DB_PATH)

# ── Load Green tripdata ──────────────────────────────────────────
green_files = f'{DATA_PATH}/green/*.parquet'
con.execute(f"CREATE TABLE IF NOT EXISTS green_tripdata AS SELECT * FROM read_parquet('{green_files}')")
count = con.execute("SELECT COUNT(*) FROM green_tripdata").fetchone()[0]
print(f"green_tripdata: {count:,} rows")

# ── Load Yellow tripdata ─────────────────────────────────────────
yellow_files = f'{DATA_PATH}/yellow/*.parquet'
con.execute(f"CREATE TABLE IF NOT EXISTS yellow_tripdata AS SELECT * FROM read_parquet('{yellow_files}')")
count = con.execute("SELECT COUNT(*) FROM yellow_tripdata").fetchone()[0]
print(f"yellow_tripdata: {count:,} rows")

# ── Download FHV 2019 ────────────────────────────────────────────
fhv_dir = f'{DATA_PATH}/fhv'
os.makedirs(fhv_dir, exist_ok=True)

base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv'
months = ['01','02','03','04','05','06','07','08','09','10','11','12']

for month in months:
    filename = f'fhv_tripdata_2019-{month}.csv.gz'
    dest = f'{fhv_dir}/{filename}'
    if os.path.exists(dest):
        print(f"  {filename} already exists, skipping...")
        continue
    print(f"  Downloading {filename}...")
    urllib.request.urlretrieve(f'{base_url}/{filename}', dest)
    print(f"  Done.")

# ── Load FHV into DuckDB ─────────────────────────────────────────
print("Loading FHV data into DuckDB...")
fhv_files = f'{fhv_dir}/*.csv.gz'
con.execute(f"CREATE TABLE IF NOT EXISTS fhv_tripdata AS SELECT * FROM read_csv_auto('{fhv_files}')")
count = con.execute("SELECT COUNT(*) FROM fhv_tripdata").fetchone()[0]
print(f"fhv_tripdata: {count:,} rows")

print("\nAll tables loaded successfully!")
con.execute("SHOW ALL TABLES").fetchdf().to_string()
tables = con.execute("SHOW ALL TABLES").fetchdf()
print(tables[['name', 'schema']])