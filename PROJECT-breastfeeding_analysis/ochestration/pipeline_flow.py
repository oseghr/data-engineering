from prefect import flow, task
import subprocess, os
from google.cloud import bigquery

PROJECT   = os.environ["GCP_PROJECT"]
DATASET   = "breastfeeding_analytics"
TABLE     = "raw_bf_rates"
GCS_URI   = f"gs://{os.environ['GCS_BUCKET']}/raw/breastfeeding/raw_breastfeeding.csv"

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
    client = bigquery.Client(project=PROJECT)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_uri(GCS_URI, f"{PROJECT}.{DATASET}.{TABLE}", job_config=job_config)
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into BigQuery")

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
    main_pipeline.serve(name="daily-run", cron="0 6 * * *")  # 6am UTC daily