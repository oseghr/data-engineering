# import pyspark
# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .master("local[*]") \
#     .appName('test') \
#     .getOrCreate()

# print(f"Spark version: {spark.version}")

# df = spark.range(10)
# df.show()

# spark.stop()

import os

# Point PySpark to Java 11 specifically — leave system Java alone
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = "/usr/lib/jvm/java-11-openjdk-amd64/bin:" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Q1: Create Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("module6_homework") \
    .getOrCreate()

print("Q1 - Spark version:", spark.version)

# Q2: Read, repartition, save to parquet
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
df.repartition(4).write.mode("overwrite").parquet("output/yellow_nov_2025")

# Check file sizes
import os
files = [f for f in os.listdir("output/yellow_nov_2025") if f.endswith(".parquet")]
sizes = [os.path.getsize(f"output/yellow_nov_2025/{f}") / (1024*1024) for f in files]
print("Q2 - File sizes (MB):", sizes)
print("Q2 - Average size (MB):", sum(sizes)/len(sizes))

# Q3: Trips on Nov 15
nov15 = df.filter(
    (F.to_date(F.col("tpep_pickup_datetime")) == "2025-11-15")
)
print("Q3 - Trip count on Nov 15:", nov15.count())

# Q4: Longest trip in hours
df_with_duration = df.withColumn(
    "duration_hours",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
)
print("Q4 - Longest trip (hours):", df_with_duration.agg(F.max("duration_hours")).collect()[0][0])

# Q5: UI Port - answered directly (no code needed)
print("Q5 - Spark UI runs on port: 4040")

# Q6: Least frequent pickup zone
zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")
df.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

result = spark.sql("""
    SELECT z.Zone, COUNT(*) as cnt
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY cnt ASC
    LIMIT 1
""")
result.show()
print("Q6 - Least frequent pickup zone:", result.collect()[0][0])