-- ============================================
-- SETUP
-- ============================================





-- Create external table
CREATE OR REPLACE EXTERNAL TABLE `dataproject_hw3_dataset.yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dataproject-484804-demo-bucket/yellow/*.parquet']
);

-- Create regular table
CREATE OR REPLACE TABLE `dataproject_hw3_dataset.yellow_tripdata_2024`
AS
SELECT * FROM `dataproject_hw3_dataset.yellow_tripdata_2024`;

-- ============================================
-- QUESTION 1: Count records
-- ============================================
SELECT COUNT(*) as total_records
FROM `dezoomcamp_hw3_2025.yellow_tripdata`;

-- ============================================
-- QUESTION 2: Data read estimation
-- (Check estimates in UI before running)
-- ============================================
SELECT COUNT(DISTINCT PULocationID)
FROM `dezoomcamp_hw3_2025.yellow_tripdata_external`;

SELECT COUNT(DISTINCT PULocationID)
FROM `dezoomcamp_hw3_2025.yellow_tripdata`;

-- ============================================
-- QUESTION 3: Columnar storage
-- (Check estimates in UI before running)
-- ============================================
SELECT PULocationID
FROM `dezoomcamp_hw3_2025.yellow_tripdata`;

SELECT PULocationID, DOLocationID
FROM `dezoomcamp_hw3_2025.yellow_tripdata`;

-- ============================================
-- QUESTION 4: Zero fare trips
-- ============================================
SELECT COUNT(*) as zero_fare_trips
FROM `dezoomcamp_hw3_2025.yellow_tripdata`
WHERE fare_amount = 0;

-- ============================================
-- QUESTION 5: Create partitioned/clustered table
-- ============================================
CREATE OR REPLACE TABLE `dezoomcamp_hw3_2025.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `dezoomcamp_hw3_2025.yellow_tripdata`;

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
-- (Check estimate in UI before running)
-- ============================================
SELECT COUNT(*)
FROM `dezoomcamp_hw3_2025.yellow_tripdata`;