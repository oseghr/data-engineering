from google.cloud import storage
import os, pathlib

BUCKET = os.environ["GCS_BUCKET"]
LOCAL  = pathlib.Path("data/raw_breastfeeding.csv")

def upload():
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob   = bucket.blob(f"raw/breastfeeding/{LOCAL.name}")
    blob.upload_from_filename(str(LOCAL))
    print(f"Uploaded to gs://{BUCKET}/raw/breastfeeding/{LOCAL.name}")

if __name__ == "__main__":
    upload()