import duckdb
import os

DB_PATH = '/workspaces/data-engineering/homework_4_analytics/newdbt/newdbt_prj/newdbt_prj.duckdb'
FHV_DIR = '/workspaces/data-engineering/homework_4_analytics/newdbt/newdbt_prj/data/fhv'

con = duckdb.connect(DB_PATH)

# Set memory limit to avoid OOM kills
con.execute("SET memory_limit='1GB'")
con.execute("SET temp_directory='/tmp/duckdb_temp'")
os.makedirs('/tmp/duckdb_temp', exist_ok=True)

# Drop table if partial load exists
con.execute("DROP TABLE IF EXISTS fhv_tripdata")

months = ['01','02','03','04','05','06','07','08','09','10','11','12']
table_created = False

for month in months:
    filepath = f'{FHV_DIR}/fhv_tripdata_2019-{month}.csv.gz'
    if not os.path.exists(filepath):
        print(f"  MISSING: {filepath}")
        continue

    print(f"  Loading fhv_tripdata_2019-{month}...", end=' ', flush=True)

    if not table_created:
        con.execute(f"CREATE TABLE fhv_tripdata AS SELECT * FROM read_csv_auto('{filepath}')")
        table_created = True
    else:
        con.execute(f"INSERT INTO fhv_tripdata SELECT * FROM read_csv_auto('{filepath}')")

    count = con.execute("SELECT COUNT(*) FROM fhv_tripdata").fetchone()[0]
    print(f"cumulative count: {count:,}")

final_count = con.execute("SELECT COUNT(*) FROM fhv_tripdata").fetchone()[0]
print(f"\nDone! fhv_tripdata total: {final_count:,} rows")