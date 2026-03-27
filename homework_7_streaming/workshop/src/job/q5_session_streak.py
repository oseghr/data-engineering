"""
Q5 — PyFlink: Session window (5-min gap), longest trip streak per PULocationID
Submit with:
    docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q5_session_streak.py

After 1-2 mins, query results in Postgres:
    SELECT "PULocationID", num_trips,
           window_end - window_start AS session_duration
    FROM session_trips
    ORDER BY num_trips DESC
    LIMIT 5;
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
    'properties.group.id'           = 'flink-q5-group',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true'
)
""")

t_env.execute_sql("""
CREATE TABLE session_trips (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    PULocationID  INT,
    num_trips     BIGINT
) WITH (
    'connector'   = 'jdbc',
    'url'         = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name'  = 'session_trips',
    'username'    = 'postgres',
    'password'    = 'postgres',
    'driver'      = 'org.postgresql.Driver'
)
""")

# Session window: groups events where the gap between consecutive events
# for the same PULocationID is less than 5 minutes.
# When a 5-minute gap occurs, the window closes and emits.
t_env.execute_sql("""
INSERT INTO session_trips
SELECT
    SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
    SESSION_END(event_timestamp, INTERVAL '5' MINUTE)   AS window_end,
    PULocationID,
    COUNT(*) AS num_trips
FROM green_trips
GROUP BY
    SESSION(event_timestamp, INTERVAL '5' MINUTE),
    PULocationID
""")
