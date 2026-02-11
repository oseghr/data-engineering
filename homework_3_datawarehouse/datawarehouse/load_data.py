# import os
# import sys
# import urllib.request
# from concurrent.futures import ThreadPoolExecutor
# from google.cloud import storage
# from google.api_core.exceptions import NotFound, Forbidden
# import time


# # Change this to your bucket name
# BUCKET_NAME = "dataproject-484804-demo-bucket"

# # If you authenticated through the GCP SDK you can comment out these two lines
# CREDENTIALS_FILE = "gcs.json"
# client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# # If commented initialize client with the following
# # client = storage.Client(project='zoomcamp-mod3-datawarehouse')


# BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
# MONTHS = [f"{i:02d}" for i in range(1, 7)]
# DOWNLOAD_DIR = "."

# CHUNK_SIZE = 8 * 1024 * 1024

# os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# bucket = client.bucket(BUCKET_NAME)


# def download_file(month):
#     url = f"{BASE_URL}{month}.parquet"
#     file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

#     try:
#         print(f"Downloading {url}...")
#         urllib.request.urlretrieve(url, file_path)
#         print(f"Downloaded: {file_path}")
#         return file_path
#     except Exception as e:
#         print(f"Failed to download {url}: {e}")
#         return None


# def create_bucket(bucket_name):
#     try:
#         # Get bucket details
#         bucket = client.get_bucket(bucket_name)

#         # Check if the bucket belongs to the current project
#         project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
#         if bucket_name in project_bucket_ids:
#             print(
#                 f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
#             )
#         else:
#             print(
#                 f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
#             )
#             sys.exit(1)

#     except NotFound:
#         # If the bucket doesn't exist, create it
#         bucket = client.create_bucket(bucket_name)
#         print(f"Created bucket '{bucket_name}'")
#     except Forbidden:
#         # If the request is forbidden, it means the bucket exists but you don't have access to see details
#         print(
#             f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
#         )
#         sys.exit(1)


# def verify_gcs_upload(blob_name):
#     return storage.Blob(bucket=bucket, name=blob_name).exists(client)


# def upload_to_gcs(file_path, max_retries=3):
#     blob_name = os.path.basename(file_path)
#     blob = bucket.blob(blob_name)
#     blob.chunk_size = CHUNK_SIZE

#     create_bucket(BUCKET_NAME)

#     for attempt in range(max_retries):
#         try:
#             print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
#             blob.upload_from_filename(file_path)
#             print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

#             if verify_gcs_upload(blob_name):
#                 print(f"Verification successful for {blob_name}")
#                 return
#             else:
#                 print(f"Verification failed for {blob_name}, retrying...")
#         except Exception as e:
#             print(f"Failed to upload {file_path} to GCS: {e}")

#         time.sleep(5)

#     print(f"Giving up on {file_path} after {max_retries} attempts.")


# if __name__ == "__main__":
#     create_bucket(BUCKET_NAME)

#     with ThreadPoolExecutor(max_workers=4) as executor:
#         file_paths = list(executor.map(download_file, MONTHS))

#     with ThreadPoolExecutor(max_workers=4) as executor:
#         executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

#     print("All files processed and verified.")


import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time


# Configuration
BUCKET_NAME = "20260210_oseghr_bucket"
CREDENTIALS_FILE = "gcs.json"
DOWNLOAD_DIR = "./downloads"
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
MAX_WORKERS = 4
MAX_RETRIES = 3

# Data configuration
YEAR = "2024"
MONTHS = [f"{i:02d}" for i in range(1, 7)]  # January to June
TAXI_TYPES = ["yellow", "green"]  # Add more if needed: "fhv", "fhvhv"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Initialize client
client = storage.Client()  # Automatically uses GOOGLE_APPLICATION_CREDENTIALS env var
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def create_bucket_if_needed(bucket_name):
    """Create bucket if it doesn't exist, verify access if it does"""
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"✓ Bucket '{bucket_name}' exists and is accessible")
        return bucket
    except NotFound:
        try:
            bucket = client.create_bucket(bucket_name)
            print(f"✓ Created new bucket '{bucket_name}'")
            return bucket
        except Exception as e:
            print(f"✗ Failed to create bucket: {e}")
            sys.exit(1)
    except Forbidden:
        print(f"✗ Bucket '{bucket_name}' exists but you don't have access")
        sys.exit(1)


def download_file(task):
    """Download a single parquet file with progress tracking"""
    taxi_type, month = task
    filename = f"{taxi_type}_tripdata_{YEAR}-{month}.parquet"
    url = f"{BASE_URL}{filename}"
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    # Skip if already downloaded
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path) / (1024 * 1024)
        print(f"⏭️  Skipping {filename} (already exists, {file_size:.1f} MB)")
        return file_path, taxi_type, month

    def show_progress(block_num, block_size, total_size):
        """Display download progress"""
        if total_size > 0:
            downloaded = block_num * block_size
            percent = min(100, (downloaded / total_size) * 100)
            mb_downloaded = downloaded / (1024 * 1024)
            mb_total = total_size / (1024 * 1024)
            print(f"\r  📥 {filename}: {percent:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end="", flush=True)

    try:
        print(f"\n📥 Downloading {filename}...")
        urllib.request.urlretrieve(url, file_path, reporthook=show_progress)
        file_size = os.path.getsize(file_path) / (1024 * 1024)
        print(f"\n✓ Downloaded {filename} ({file_size:.1f} MB)")
        return file_path, taxi_type, month
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"\n⚠️  File not found: {filename} (might not be available yet)")
        else:
            print(f"\n✗ HTTP error downloading {filename}: {e}")
        return None, taxi_type, month
    except Exception as e:
        print(f"\n✗ Failed to download {filename}: {e}")
        return None, taxi_type, month


def upload_to_gcs(upload_task):
    """Upload file to GCS with retry logic and verification"""
    file_path, taxi_type, month, bucket = upload_task
    
    if file_path is None:
        return False, taxi_type, month
    
    if not os.path.exists(file_path):
        print(f"✗ File not found for upload: {file_path}")
        return False, taxi_type, month
    
    filename = os.path.basename(file_path)
    # Organize in GCS by taxi type: yellow/yellow_tripdata_2024-01.parquet
    blob_name = f"{taxi_type}/{filename}"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(MAX_RETRIES):
        try:
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            print(f"📤 Uploading {filename} to gs://{BUCKET_NAME}/{blob_name} (attempt {attempt + 1}/{MAX_RETRIES}, {file_size:.1f} MB)...")
            
            blob.upload_from_filename(file_path)
            
            # Verify upload
            blob.reload()
            if blob.exists():
                print(f"✓ Uploaded and verified: gs://{BUCKET_NAME}/{blob_name}")
                # Clean up local file after successful upload
                os.remove(file_path)
                return True, taxi_type, month
            else:
                print(f"⚠️  Upload verification failed for {filename}")
                
        except Exception as e:
            print(f"✗ Upload attempt {attempt + 1} failed for {filename}: {e}")
        
        if attempt < MAX_RETRIES - 1:
            wait_time = 5 * (attempt + 1)  # Exponential backoff
            print(f"  Waiting {wait_time}s before retry...")
            time.sleep(wait_time)

    print(f"✗ Gave up on {filename} after {MAX_RETRIES} attempts")
    return False, taxi_type, month


def print_summary(download_results, upload_results):
    """Print summary of operations"""
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    # Download summary
    total_downloads = len(download_results)
    successful_downloads = sum(1 for path, _, _ in download_results if path is not None)
    print(f"\n📥 Downloads: {successful_downloads}/{total_downloads} successful")
    
    failed_downloads = [(t, m) for path, t, m in download_results if path is None]
    if failed_downloads:
        print("   Failed downloads:")
        for taxi_type, month in failed_downloads:
            print(f"     - {taxi_type}_tripdata_{YEAR}-{month}.parquet")
    
    # Upload summary
    total_uploads = len(upload_results)
    successful_uploads = sum(1 for success, _, _ in upload_results if success)
    print(f"\n📤 Uploads: {successful_uploads}/{total_uploads} successful")
    
    failed_uploads = [(t, m) for success, t, m in upload_results if not success]
    if failed_uploads:
        print("   Failed uploads:")
        for taxi_type, month in failed_uploads:
            print(f"     - {taxi_type}_tripdata_{YEAR}-{month}.parquet")
    
    print("\n" + "="*60)


def main():
    """Main execution function"""
    print("="*60)
    print("NYC TAXI DATA LOADER - January to June 2024")
    print("="*60)
    print(f"Taxi types: {', '.join(TAXI_TYPES)}")
    print(f"Months: January to June ({YEAR})")
    print(f"Target bucket: gs://{BUCKET_NAME}")
    print("="*60 + "\n")
    
    # Step 1: Create/verify bucket
    print("🔧 Setting up GCS bucket...")
    bucket = create_bucket_if_needed(BUCKET_NAME)
    print()
    
    # Step 2: Generate download tasks (all combinations of taxi_type x month)
    download_tasks = [
        (taxi_type, month) 
        for taxi_type in TAXI_TYPES 
        for month in MONTHS
    ]
    
    print(f"📋 Queued {len(download_tasks)} files for download\n")
    
    # Step 3: Download files concurrently
    print("="*60)
    print("DOWNLOADING FILES")
    print("="*60 + "\n")
    
    download_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {executor.submit(download_file, task): task for task in download_tasks}
        
        for future in as_completed(future_to_task):
            result = future.result()
            download_results.append(result)
    
    # Filter successful downloads
    successful_downloads = [
        (file_path, taxi_type, month) 
        for file_path, taxi_type, month in download_results 
        if file_path is not None
    ]
    
    print(f"\n✓ Download phase complete: {len(successful_downloads)}/{len(download_tasks)} files ready for upload\n")
    
    # Step 4: Upload files concurrently
    if successful_downloads:
        print("="*60)
        print("UPLOADING FILES TO GCS")
        print("="*60 + "\n")
        
        upload_tasks = [
            (file_path, taxi_type, month, bucket) 
            for file_path, taxi_type, month in successful_downloads
        ]
        
        upload_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_task = {executor.submit(upload_to_gcs, task): task for task in upload_tasks}
            
            for future in as_completed(future_to_task):
                result = future.result()
                upload_results.append(result)
        
        # Step 5: Print summary
        print_summary(download_results, upload_results)
        
        # Check if we should clean up download directory
        remaining_files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.parquet')]
        if not remaining_files:
            try:
                os.rmdir(DOWNLOAD_DIR)
                print(f"\n🧹 Cleaned up download directory")
            except:
                pass
    else:
        print("\n⚠️  No files were successfully downloaded. Nothing to upload.")
        print_summary(download_results, [])
    
    print("\n✅ Process complete!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Fatal error: {e}")
        sys.exit(1)
