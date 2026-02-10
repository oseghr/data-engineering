-- ============================================
-- SETUP
-- ============================================
First, make sure you have:
✅ Data loaded in GCS: gs://dezoomcamp_hw3_2025/yellow/'*'.parquet 
    -- by running the webload.py script
✅ Dataset created in BigQuery: 
    bq mk --dataset --location=US dataproject_hw3_dataset --> by running the command on terminal



-- Create external and regular tables
CREATE OR REPLACE EXTERNAL TABLE `dataproject_hw3_dataset.yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dataproject-484804_hw3_2025/yellow/*.parquet']
); -- external table

CREATE OR REPLACE TABLE `dataproject_hw3_dataset.yellow_tripdata_2024`
AS
SELECT * FROM `dataproject_hw3_dataset.yellow_tripdata_2024`; -- regular/materialized table

-- ============================================
-- QUESTION 1: Count records
-- ============================================
SELECT COUNT(*) as total_records
FROM `dezoomcamp_hw3_2025.yellow_tripdata`;

-- ============================================
-- QUESTION 2: Data read estimation
-- Count distinct PULocationID (Check estimates in UI before running)
-- ============================================
SELECT COUNT(DISTINCT PULocationID)
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024`; -- external table

SELECT COUNT(DISTINCT PULocationID)
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material`; -- materialized table

-- ============================================
-- QUESTION 3: Columnar storage
-- Count distinct PULocationID (Check estimates in UI before running)
-- ============================================
-- Single column (check estimate)
SELECT PULocationID
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material`;

-- Two columns (check estimate)
SELECT PULocationID, DOLocationID
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material`;


-- ============================================
-- QUESTION 4: Zero fare trips
-- Count records (Fare Amount = 0)
-- ============================================
SELECT COUNT(fare_amount)
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024`
WHERE fare_amount = 0; -- external table

SELECT COUNT(fare_amount)
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material` -- materialized table
WHERE fare_amount = 0;

-- ============================================
-- QUESTION 5: Create partitioned/clustered table
-- Create partitioned and clustered table
-- ============================================
CREATE OR REPLACE TABLE `dataproject_hw3_dataset.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024`;

-- ============================================
-- QUESTION 6: Partition benefits
-- (Check estimates in UI before running)
-- ============================================
-- Non-partitioned
SELECT DISTINCT VendorID
FROM `dezoomcamp_hw3_2025.yellow_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Partitioned
SELECT DISTINCT VendorID
FROM `dezoomcamp_hw3_2025.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- ============================================
-- QUESTION 9: Table scans
-- Total count (Check estimate in UI before running)
-- ============================================
SELECT COUNT(*)
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material`; -- materialized table