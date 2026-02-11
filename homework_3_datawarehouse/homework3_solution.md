# Module 3 Solution: Data Warehousing & BigQuery

In this homework we'll practice working with BigQuery and Google Cloud Storage.

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework.

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.

## Data

For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 (not the entire year of data).

Parquet Files are available from the New York City Taxi Data found here:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Loading the data

You can use the following scripts to load the data into your GCS bucket:

- Python script: [load_yellow_taxi_data.py](./load_yellow_taxi_data.py)
- Jupyter notebook with DLT: [DLT_upload_to_GCP.ipynb](./DLT_upload_to_GCP.ipynb)

You will need to generate a Service Account with GCS Admin privileges or be authenticated with the Google SDK, and update the bucket name in the script.

If you are using orchestration tools such as Kestra, Mage, Airflow, or Prefect, do not load the data into BigQuery using the orchestrator.

Make sure that all 6 files show in your GCS bucket before beginning.

Note: You will need to use the PARQUET option when creating an external table.


## BigQuery Setup

Create an external table using the Yellow Taxi Trip Records. 

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). 



## Question 1. Counting records

What is count of records for the 2024 Yellow Taxi Data?

## Answer
```sql
SELECT COUNT(*) FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024`;
```
```bash
+--------------+
| total_count  | 
|--------------+
| 20332093     | 
+--------------+
```
- SOLUTION: 20,332,093



## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

## Answer
```sql
SELECT COUNT(DISTINCT PULocationID)
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024`; -- external table

SELECT COUNT(DISTINCT PULocationID)
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material`;  -- materialized table
```
```bash
+--------------+
|total_distinct| 
|--------------+
| 262          | 
+--------------+
```

- SOLUTION: 0 MB for the External Table and 155.12 MB for the Materialized Table



## Question 3. Understanding columnar storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?

## Answer
```sql
-- Single column (check estimate)
SELECT PULocationID
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material`; -- 155.12MB

-- Two columns (check estimate)
SELECT PULocationID, DOLocationID
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material`; -- 310.24MB
```

- SOLUTION: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.




## Question 4. Counting zero fare trips

How many records have a fare_amount of 0?

## Answer
```sql
-- Count records (fare_amount = 0)
SELECT COUNT(fare_amount)
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material`
WHERE fare_amount = 0;
```

- SOLUTION: 8,333



## Question 5. Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

## Answer
```sql
-- Create partitioned and clustered table
CREATE OR REPLACE TABLE `dataproject_hw3_dataset.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024`;
```

- SOLUTION: Partition by tpep_dropoff_datetime and Cluster on VendorID



## Question 6. Partition benefits

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)


Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? 


Choose the answer which most closely matches.
 
## Answer
```sql
-- Query on NON-PARTITIONED table (check estimate)
SELECT DISTINCT VendorID
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Query on PARTITIONED table (check estimate)
SELECT DISTINCT VendorID
FROM `dataproject_hw3_dataset.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

- SOLUTION: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table



## Question 7. External table storage

Where is the data stored in the External Table you created?

## Answer
- SOLUTION: GCP Bucket (your data is in gs://dataproject_hw3_dataset/yellow/)
    - Explanation:
        - External tables don't store data in BigQuery
        - They query data directly from Google Cloud Storage
        - Regular tables store data in BigQuery's internal storage



## Question 8. Clustering best practices

It is best practice in Big Query to always cluster your data:

## Answer
- SOLUTION: False
    - Explanation:
        - Clustering is NOT always beneficial
        - Only helpful when:
            - Table > 1 GB
            - You frequently filter/sort by specific columns
            - Query patterns are predictable
        - Small tables or random query patterns = clustering adds overhead with no benefit



## Question 9. Understanding table scans

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

## Answer
```sql
SELECT COUNT(*)
FROM `dataproject-484804.dataproject_hw3_dataset.yellow_tripdata_2024_material`;
```

- SOLUTION  0MB (BigQuery stores metadata about row counts)

    - Why this estimate?
        - COUNT(*) doesn't need to read actual data
        - BigQuery stores metadata about row counts
        - Should show 0 MB because it uses table metadata, not the actual rows
        - If it shows bytes, BigQuery might be reading column data unnecessarily
