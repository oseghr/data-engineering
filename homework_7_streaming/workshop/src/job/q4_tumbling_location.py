"""
Q4 — PyFlink: 5-minute tumbling window, trip count per PULocationID
Submit with:
    docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_location.py

After 1-2 mins, query results in Postgres:
    SELECT "PULocationID", num_trips
    FROM trip_counts_by_location
    ORDER BY num_trips DESC
    LIMIT 3;
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# --- Environment setup ---
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)   # MUST be 1 — topic has 1 partition; higher parallelism stalls watermarks

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# --- Source table: reads from Redpanda ---
# Note: inside Docker, Redpanda is reached via "redpanda:29092" (internal listener)
# Note: event_timestamp is derived from the string column using TO_TIMESTAMP
t_env.execute_sql("""
CREATE TABLE green_trips (
    lpep_pickup_datetime  STRING,
    lpep_dropoff_datetime STRING,
    PULocationID          INT,
    DOLocationID          INT,
    passenger_count       DOUBLE,
    trip_distance         DOUBLE,
    tip_amount            DOUBLE,
    total_amount          DOUBLE,
    event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'green-trips',
    'properties.bootstrap.servers'  = 'redpanda:29092',
    'properties.group.id'           = 'flink-q4-group',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true'
)
""")

# --- Sink table: writes to PostgreSQL ---
t_env.execute_sql("""
CREATE TABLE trip_counts_by_location (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    PULocationID  INT,
    num_trips     BIGINT
) WITH (
    'connector'   = 'jdbc',
    'url'         = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name'  = 'trip_counts_by_location',
    'username'    = 'postgres',
    'password'    = 'postgres',
    'driver'      = 'org.postgresql.Driver'
)
""")

# --- Job: count trips per location per 5-minute window ---
t_env.execute_sql("""
INSERT INTO trip_counts_by_location
SELECT
    TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(event_timestamp, INTERVAL '5' MINUTE)   AS window_end,
    PULocationID,
    COUNT(*) AS num_trips
FROM green_trips
GROUP BY
    TUMBLE(event_timestamp, INTERVAL '5' MINUTE),
    PULocationID
""")
