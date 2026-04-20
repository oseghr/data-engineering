from prefect import flow, task
import os, subprocess
import pandas as pd
from google.cloud import bigquery

PROJECT  = os.environ["GCP_PROJECT"]
DATASET  = "breastfeeding_analytics"
TABLE    = "raw_bf_rates"
LOCAL    = "data/raw_breastfeeding.csv"

@task(name="download-unicef-data", retries=2)
def download():
    from ingestion.download_data import download
    return download()

@task(name="upload-to-gcs")
def upload_gcs(local_path: str):
    from ingestion.upload_to_gcs import upload
    upload()

@task(name="load-to-bigquery")
def load_bq():
    df = pd.read_csv(LOCAL)

    # Uppercase all columns to avoid BigQuery case-insensitive conflicts
    # then clean special characters
    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
        .str.replace(r'[^A-Z0-9_]', '_', regex=True)
        .str.replace(r'_+', '_', regex=True)
        .str.strip('_')
    )

    # Now drop any duplicates that emerge after uppercasing
    df = df.loc[:, ~df.columns.duplicated()]

    print(f"Columns ({len(df.columns)}): {list(df.columns)}")

    client = bigquery.Client(project=PROJECT)
    table_ref = f"{PROJECT}.{DATASET}.{TABLE}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_ref}")

@task(name="run-dbt-transformations")
def run_dbt():
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "dbt_transform", "--profiles-dir", "dbt_transform"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)

@flow(name="breastfeeding-pipeline", log_prints=True)
def main_pipeline():
    path = download()
    upload_gcs(path)
    load_bq()
    run_dbt()

if __name__ == "__main__":
    main_pipeline()
