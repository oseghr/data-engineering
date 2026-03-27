# Module 7 Streaming Homework — Setup & Execution Guide

## Folder Structure

```
streaming-homework/
├── Dockerfile                  # Custom Flink image with all JARs
├── docker-compose.yml          # All services wired together
├── init-db.sql                 # Auto-creates Postgres tables on startup
├── requirements.txt            # Python deps for producer/consumer
├── producer.py                 # Q2 — sends data to Redpanda
├── consumer.py                 # Q3 — counts trips > 5km
├── green_tripdata_2025-10.parquet  # <-- YOU MUST PLACE THIS HERE
└── src/
    └── job/
        ├── q4_tumbling_location.py
        ├── q5_session_streak.py
        └── q6_hourly_tips.py
```

---

## Step 0 — Prerequisites

- Docker Desktop installed and running
- Python 3.9+ installed on your host machine
- Download the parquet file and place it in this folder:
  https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green

---

## Step 1 — Install Python dependencies (host machine)

```bash
pip install -r requirements.txt
```

---

## Step 2 — Build and start all services

```bash
# Clean start (removes old volumes too)
docker compose down -v

# Build the custom Flink image (downloads JARs — takes 3-5 mins first time)
docker compose build

# Start everything in background
docker compose up -d
```

Wait ~30 seconds for all services to be healthy, then verify:

```bash
docker compose ps
# All services should show "running" or "healthy"
```

---

## Q1 — Check Redpanda version

```bash
docker exec -it workshop-redpanda-1 rpk version
```

---

## Q2 — Create topic and send data

```bash
# Create the topic
docker exec -it workshop-redpanda-1 rpk topic create green-trips

# Send all rows (timed) — run from your host machine
python producer.py
```

---

## Q3 — Count trips with distance > 5 km

```bash
python consumer.py
```

---

## Q4 — Tumbling window: trips per pickup location

```bash
# Submit the Flink job
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4_tumbling_location.py

# Wait 1-2 minutes, watch job at http://localhost:8081
# Then query results:
docker exec -it workshop-postgres-1 psql -U postgres -c \
  'SELECT "PULocationID", num_trips FROM trip_counts_by_location ORDER BY num_trips DESC LIMIT 3;'
```

Cancel the job from http://localhost:8081 once results appear.

---

## Q5 — Session window: longest trip streak

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q5_session_streak.py

# Query results:
docker exec -it workshop-postgres-1 psql -U postgres -c \
  'SELECT "PULocationID", num_trips FROM session_trips ORDER BY num_trips DESC LIMIT 5;'
```

---

## Q6 — Hourly tumbling window: largest tip total

```bash
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q6_hourly_tips.py

# Query results:
docker exec -it workshop-postgres-1 psql -U postgres -c \
  'SELECT window_start, ROUND(total_tip::numeric, 2) FROM hourly_tips ORDER BY total_tip DESC LIMIT 3;'
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `docker compose build` fails downloading JARs | Check internet connection; Maven Central must be reachable |
| Flink job hangs, no results in Postgres | Make sure parallelism=1 in the job file |
| Duplicate data in Postgres | Delete and recreate topic: `rpk topic delete green-trips && rpk topic create green-trips` |
| `kafka.errors.NoBrokersAvailable` in producer | Redpanda not ready yet — wait 30s and retry |
| `Connection refused` on port 9092 | Run `docker compose ps` to confirm redpanda is healthy |
| Job fails with ClassNotFoundException for JDBC/Kafka | Dockerfile JAR download failed during build — rebuild with `docker compose build --no-cache` |

---

## Resetting everything cleanly

```bash
docker compose down -v          # stops containers AND deletes volumes
docker compose build --no-cache # full rebuild
docker compose up -d
docker exec -it workshop-redpanda-1 rpk topic create green-trips
python producer.py
```
