# import io
# import os
# import requests
# import pandas as pd
# from google.cloud import storage

# """
# Pre-reqs: 
# 1. `pip install pandas pyarrow google-cloud-storage`
# 2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
# 3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
# """

# # services = ['fhv','green','yellow']
# init_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page/'
# # switch out the bucketname
# BUCKET = os.environ.get("GCP_GCS_BUCKET", "dataproject-484804-demo-bucket")


# def upload_to_gcs(bucket, object_name, local_file):
#     """
#     Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
#     """
#     # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
#     # # (Ref: https://github.com/googleapis/python-storage/issues/74)
#     # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
#     # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

#     client = storage.Client()
#     bucket = client.bucket(bucket)
#     blob = bucket.blob(object_name)
#     blob.upload_from_filename(local_file)


# def web_to_gcs(year, service):
#     for i in range(12):
        
#         # sets the month part of the file_name string
#         month = '0'+str(i+1)
#         month = month[-2:]

#         # csv file_name
#         file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

#         # download it using requests via a pandas df
#         request_url = f"{init_url}{service}/{file_name}"
#         r = requests.get(request_url)
#         open(file_name, 'wb').write(r.content)
#         print(f"Local: {file_name}")

#         # read it back into a parquet file
#         df = pd.read_csv(file_name, compression='gzip')
#         file_name = file_name.replace('.csv.gz', '.parquet')
#         df.to_parquet(file_name, engine='pyarrow')
#         print(f"Parquet: {file_name}")

#         # upload it to gcs 
#         upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
#         print(f"GCS: {service}/{file_name}")


# # web_to_gcs('2019', 'green')
# # web_to_gcs('2020', 'green')
# # web_to_gcs('2019', 'yellow')
# web_to_gcs('2024', 'yellow')


import os
import requests
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
from google.cloud import storage

# Use official NYC TLC data source
init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
BUCKET = "dataproject-484804_hw3_bucket"


def upload_to_gcs(bucket_name, object_name, local_file):
    """Upload file to GCS"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        month = f"{i+1:02d}"  # Cleaner way to format month

        # NYC TLC provides parquet files directly now
        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        request_url = f"{init_url}{file_name}"
        
        print(f"Downloading: {request_url}")
        r = requests.get(request_url)
        
        # Check if download succeeded
        if r.status_code != 200:
            print(f"✗ SKIP {file_name}: HTTP {r.status_code} (file doesn't exist)")
            continue
        
        # Verify it's actually a parquet file (magic bytes: 'PAR1')
        if not r.content[:4] == b'PAR1':
            print(f"✗ SKIP {file_name}: Not a valid parquet file")
            continue
        
        # Save locally
        with open(file_name, 'wb') as f:
            f.write(r.content)
        print(f"✓ Local: {file_name}")
        
        # Upload to GCS
        try:
            upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
            print(f"✓ GCS: gs://{BUCKET}/{service}/{file_name}")
        except Exception as e:
            print(f"✗ Upload failed: {e}")
            continue
        finally:
            # Clean up local file
            if os.path.exists(file_name):
                os.remove(file_name)


# Run for years that actually exist

# web_to_gcs('2022', 'yellow')
web_to_gcs('2024', 'yellow')
# 2024 might not have all months available yet

# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')