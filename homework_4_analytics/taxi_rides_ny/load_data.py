#!/usr/bin/env python3
"""
Load NYC Taxi Data into BigQuery - Robust Version
Uses pyarrow directly (no pandas↔BQ schema conversion) to avoid
type-mismatch errors on columns like ehail_fee, airport_fee, SR_Flag.

Run from your Codespace terminal:
    python3 load_taxi_data.py
"""

import io
import os
import urllib.request

# ─────────────────────────────────────────────
PROJECT_ID = "dataproject-484804"
DATASET_ID = "raw_data"
LOCATION   = "US"
CHUNK_SIZE = 200_000      # rows per upload chunk
# ─────────────────────────────────────────────

def install_if_missing():
    try:
        import google.cloud.bigquery  # noqa
        import pyarrow                # noqa
        print("✅ Dependencies already installed")
    except ImportError:
        print("📦 Installing dependencies...")
        os.system("pip install google-cloud-bigquery pyarrow --quiet")
        print("✅ Installed")


def create_dataset(client):
    from google.cloud import bigquery
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset.location = LOCATION
    client.create_dataset(dataset, exists_ok=True)
    print(f"✅ Dataset '{DATASET_ID}' ready")


def drop_table_if_exists(client, table_name):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    client.delete_table(table_ref, not_found_ok=True)
    print(f"🗑️  Dropped {table_name}")


def download_file(url, filename):
    if os.path.exists(filename):
        size_mb = os.path.getsize(filename) / 1024 / 1024
        print(f"⏭️  {filename} already exists ({size_mb:.0f} MB), skipping")
        return
    print(f"⬇️  Downloading {filename}...")
    urllib.request.urlretrieve(url, filename)
    size_mb = os.path.getsize(filename) / 1024 / 1024
    print(f"   ✅ Done ({size_mb:.0f} MB)")


def cast_table_safe(arrow_table):
    """
    Cast every column to a BigQuery-safe type using only pyarrow
    (no pandas involved, so no conversion errors).

    Rules:
      - int8/16/32/64, uint*  → float64  (avoids int↔float conflicts)
      - large_string          → string
      - large_binary          → binary
      - timestamp(ns)         → timestamp(us)   BQ only supports microseconds
      - Everything else kept as-is
    """
    import pyarrow as pa

    new_cols   = []
    new_fields = []

    for i, field in enumerate(arrow_table.schema):
        col = arrow_table.column(i)
        t   = field.type

        if pa.types.is_integer(t) or pa.types.is_unsigned_integer(t):
            new_type = pa.float64()
        elif t == pa.large_string():
            new_type = pa.string()
        elif t == pa.large_binary():
            new_type = pa.binary()
        elif pa.types.is_timestamp(t) and t.unit == "ns":
            new_type = pa.timestamp("us", tz=t.tz)
        else:
            new_cols.append(col)
            new_fields.append(field)
            continue

        new_cols.append(col.cast(new_type, safe=False))
        new_fields.append(field.with_type(new_type))

    new_schema = pa.schema(new_fields)
    return pa.table(
        {f.name: c for f, c in zip(new_fields, new_cols)},
        schema=new_schema,
    )


def chunk_to_parquet_bytes(arrow_table):
    """Serialise an Arrow table to an in-memory parquet buffer."""
    import pyarrow.parquet as pq
    buf = io.BytesIO()
    pq.write_table(arrow_table, buf)
    buf.seek(0)
    return buf


def load_parquet_chunked(client, filepath, table_name, first_file=False):
    """
    Stream a parquet file → cast types → upload chunks as parquet bytes.
    Never materialises the whole file in memory at once.
    """
    import pyarrow.parquet as pq
    from google.cloud import bigquery

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    size_mb   = os.path.getsize(filepath) / 1024 / 1024
    print(f"\n📤 {filepath} ({size_mb:.0f} MB)  →  {table_ref}")

    pf         = pq.ParquetFile(filepath)
    total_rows = pf.metadata.num_rows
    uploaded   = 0
    chunk_num  = 0

    for batch in pf.iter_batches(batch_size=CHUNK_SIZE):
        import pyarrow as pa
        arrow_tbl = pa.Table.from_batches([batch])
        arrow_tbl = cast_table_safe(arrow_tbl)
        buf       = chunk_to_parquet_bytes(arrow_tbl)

        if first_file and chunk_num == 0:
            disposition = "WRITE_TRUNCATE"
        else:
            disposition = "WRITE_APPEND"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=disposition,
            autodetect=True,
        )
        job = client.load_table_from_file(buf, table_ref, job_config=job_config)
        job.result()

        uploaded  += len(batch)
        chunk_num += 1
        pct = uploaded / total_rows * 100 if total_rows else 0
        print(f"   chunk {chunk_num:3d} — {uploaded:>9,} / {total_rows:,} rows ({pct:.0f}%)")

        del arrow_tbl, buf, batch   # free memory

    rows = client.get_table(table_ref).num_rows
    print(f"   ✅ {rows:,} total rows in {table_name}")


def load_csv(client, filepath, table_name):
    from google.cloud import bigquery
    table_ref  = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    print(f"📤 Loading {filepath}  →  {table_ref}")
    with open(filepath, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()
    rows = client.get_table(table_ref).num_rows
    print(f"   ✅ {rows:,} rows in {table_name}")


def load_table(client, files, table_name):
    loaded = 0
    for i, filename in enumerate(files):
        if not os.path.exists(filename):
            print(f"   ⚠️  {filename} not found, skipping")
            continue
        load_parquet_chunked(client, filename, table_name, first_file=(i == 0))
        loaded += 1
    print(f"\n✅ {table_name}: {loaded} file(s) loaded")


def verify(client):
    print("\n📊 Final row counts:")
    tables = ["green_tripdata", "yellow_tripdata", "fhv_tripdata", "taxi_zone_lookup"]
    for t in tables:
        try:
            tbl = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{t}")
            print(f"   {t:30s}: {tbl.num_rows:>14,} rows")
        except Exception:
            print(f"   {t:30s}: ❌ not found")


def main():
    install_if_missing()

    from google.cloud import bigquery

    print(f"\n🔧 Project: {PROJECT_ID}   Dataset: {DATASET_ID}")
    client = bigquery.Client(project=PROJECT_ID)

    # 1. Dataset
    print("\n── Step 1: Create dataset ─────────────────────────────────")
    create_dataset(client)

    # 2. File lists
    BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    green_files  = [f"green_tripdata_{y}-{m:02d}.parquet"
                    for y in (2019, 2020) for m in range(1, 13)]
    yellow_files = [f"yellow_tripdata_{y}-{m:02d}.parquet"
                    for y in (2019, 2020) for m in range(1, 13)]
    fhv_files    = [f"fhv_tripdata_2019-{m:02d}.parquet"
                    for m in range(1, 13)]

    # 3. Download
    print("\n── Step 2: Download source files ──────────────────────────")
    for filename in green_files + yellow_files + fhv_files:
        download_file(f"{BASE}/{filename}", filename)
    download_file(
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv",
        "taxi_zone_lookup.csv",
    )

    # 4. Drop existing tables (clean schema)
    print("\n── Step 3: Drop existing tables ───────────────────────────")
    for t in ("green_tripdata", "yellow_tripdata", "fhv_tripdata"):
        drop_table_if_exists(client, t)

    # 5. Load
    print("\n── Step 4: Load ───────────────────────────────────────────")

    print("\n--- Green Taxi (2019-2020) ---")
    load_table(client, green_files, "green_tripdata")

    print("\n--- Yellow Taxi (2019-2020) ---")
    load_table(client, yellow_files, "yellow_tripdata")

    print("\n--- FHV Taxi (2019) ---")
    load_table(client, fhv_files, "fhv_tripdata")

    print("\n--- Taxi Zone Lookup ---")
    load_csv(client, "taxi_zone_lookup.csv", "taxi_zone_lookup")

    # 6. Verify
    print("\n── Step 5: Verify ─────────────────────────────────────────")
    verify(client)

    print("\n🎉 Done!  Now run:  dbt build --target dev")


if __name__ == "__main__":
    main()