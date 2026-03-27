"""
Q6 — PyFlink: 1-hour tumbling window, total tip_amount across all locations
Submit with:
    docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q6_hourly_tips.py

After 1-2 mins, query results in Postgres:
    SELECT window_start, ROUND(total_tip::numeric, 2) AS total_tip
    FROM hourly_tips
    ORDER BY total_tip DESC
    LIMIT 3;
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

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
    'properties.group.id'           = 'flink-q6-group',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true'
)
""")

t_env.execute_sql("""
CREATE TABLE hourly_tips (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    total_tip     DOUBLE
) WITH (
    'connector'   = 'jdbc',
    'url'         = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name'  = 'hourly_tips',
    'username'    = 'postgres',
    'password'    = 'postgres',
    'driver'      = 'org.postgresql.Driver'
)
""")

t_env.execute_sql("""
INSERT INTO hourly_tips
SELECT
    TUMBLE_START(event_timestamp, INTERVAL '1' HOUR) AS window_start,
    TUMBLE_END(event_timestamp, INTERVAL '1' HOUR)   AS window_end,
    SUM(tip_amount) AS total_tip
FROM green_trips
GROUP BY TUMBLE(event_timestamp, INTERVAL '1' HOUR)
""")
