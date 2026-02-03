# Kestra + Docker Setup Guide for Your Configuration

## Your Current Setup

```
Docker Compose Services:
├── kestra (port 8080, 8081)
│   └── Has Docker socket mounted ✅
├── kestra_postgres (internal only)
│   └── Kestra's metadata DB
├── pgdatabase (port 5432)
│   └── Your NY Taxi data DB ✅
└── pgadmin (port 8085)
    └── Database admin UI
```

## The Problem You Had (Exit Code 127)

**Exit code 127** = "Command not found"

This happened because the old flow was trying to use Docker but Kestra couldn't find the `docker` command. This is now **FIXED** in the new flow.

## What I Fixed

### 1. **Network Mode Changed**
```yaml
# OLD (doesn't work)
taskRunner:
  type: io.kestra.plugin.scripts.runner.docker.Docker
  image: python:3.11-slim
  # No networkMode specified

# NEW (works!)
taskRunner:
  type: io.kestra.plugin.scripts.runner.docker.Docker
  image: python:3.11-slim
  networkMode: host  # This is the key!
```

**Why `host` network?**
- Your `pgdatabase` is accessible on `localhost:5432`
- Using `host` mode lets Docker containers access it
- Alternative: Use service name `pgdatabase` with docker-compose network

### 2. **Correct Service Names**
```yaml
pg_host: pgdatabase  # Matches your docker-compose service name
pg_user: root        # Matches your POSTGRES_USER
pg_pass: root        # Matches your POSTGRES_PASSWORD
pg_db: ny_taxi       # Matches your POSTGRES_DB
```

## Quick Start

### Step 1: Verify Your Setup
```bash
# Check all containers are running
docker ps

# You should see:
# - kestra
# - kestra_postgres  
# - pgdatabase
# - pgadmin

# Check Kestra has Docker access
docker exec kestra docker ps
# Should show the same containers
```

### Step 2: Deploy the Flow
```bash
# Option A: Via UI
# 1. Go to http://localhost:8080
# 2. Login: admin@kestra.io / Admin1234!
# 3. Flows → Create → Paste nyc_taxi_ingestion_working.yaml

# Option B: Via CLI (if installed)
kestra flow namespace update data.pipelines nyc_taxi_ingestion_working.yaml
```

### Step 3: Execute
```bash
# Via UI: Click "Execute" button

# Via CLI:
kestra flow execute data.pipelines nyc_taxi_data_ingestion_working
```

### Step 4: Verify in Database
```bash
# Connect via pgadmin at http://localhost:8085
# Or via CLI:
docker exec -it pgdatabase psql -U root -d ny_taxi -c "SELECT COUNT(*) FROM yellow_taxi_data;"
```

## Common Issues & Solutions

### Issue 1: "Connection refused to pgdatabase"

**Symptoms:**
```
psycopg2.OperationalError: connection to server at "pgdatabase" failed
```

**Solution:**
```bash
# Check if pgdatabase is running
docker ps | grep pgdatabase

# Check if Kestra can reach it
docker exec kestra ping -c 1 pgdatabase

# If ping fails, use host network mode (already in the flow)
```

### Issue 2: "Table already exists"

**Symptoms:**
```
sqlalchemy.exc.ProgrammingError: table "yellow_taxi_data" already exists
```

**Solution:**
```bash
# Drop the table first
docker exec -it pgdatabase psql -U root -d ny_taxi -c "DROP TABLE IF EXISTS yellow_taxi_data;"

# Or modify the flow to use if_exists='replace'
```

### Issue 3: "Docker image pull failed"

**Symptoms:**
```
Error response from daemon: pull access denied
```

**Solution:**
```yaml
# Add pullPolicy to flow
pluginDefaults:
  - type: io.kestra.plugin.scripts.runner.docker.Docker
    values:
      pullPolicy: IF_NOT_PRESENT  # Uses cached images
```

### Issue 4: "Permission denied on /var/run/docker.sock"

**Symptoms:**
```
Cannot connect to Docker daemon
```

**Solution:**
```bash
# Your docker-compose already has this, but double-check:
docker exec kestra ls -la /var/run/docker.sock

# Should show: srw-rw---- ... docker.sock

# If issue persists, restart Kestra:
docker-compose restart kestra
```

## Network Options Explained

Your flow can use **TWO network approaches**:

### Option 1: Host Network (Current - Simplest)
```yaml
taskRunner:
  type: io.kestra.plugin.scripts.runner.docker.Docker
  networkMode: host

# Database connection
pg_host: localhost  # or pgdatabase
```

**Pros:**
- ✅ Simple configuration
- ✅ Access to all host services
- ✅ Works with localhost

**Cons:**
- ⚠️ Less network isolation
- ⚠️ Can't use custom networks easily

### Option 2: Docker Compose Network (More Isolated)
```yaml
taskRunner:
  type: io.kestra.plugin.scripts.runner.docker.Docker
  # Derive network name from your docker-compose
  # Format: {directory}_{network}
  networkMode: "myproject_default"  

# Database connection  
pg_host: pgdatabase  # MUST use service name
```

**To find your network name:**
```bash
docker network ls | grep default
# Example output: abc123_default
```

## Testing Components Individually

### Test 1: Database Connection
```bash
docker exec kestra python3 << 'EOF'
from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@pgdatabase:5432/ny_taxi')
with engine.connect() as conn:
    print("✅ Connected!")
EOF
```

### Test 2: Docker Access
```bash
docker exec kestra docker run --rm hello-world
# Should print: Hello from Docker!
```

### Test 3: Download Data
```bash
docker exec kestra bash -c "wget -q https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz && ls -lh *.gz"
```

## Performance Tuning

### Faster Downloads
```yaml
# Add to download task
commands:
  - wget --no-check-certificate --progress=bar:force --tries=5
```

### Faster Ingestion
```yaml
# Increase chunk size
script: |
  chunk_size = 200000  # Default is 100000
```

### Less Memory Usage
```yaml
# Decrease chunk size
script: |
  chunk_size = 50000
```

### PostgreSQL Optimizations
```sql
-- Run these before ingestion
ALTER TABLE yellow_taxi_data SET (autovacuum_enabled = false);
ALTER TABLE yellow_taxi_data SET (fillfactor = 100);

-- Run these after ingestion
CREATE INDEX idx_pickup ON yellow_taxi_data(tpep_pickup_datetime);
CREATE INDEX idx_location ON yellow_taxi_data("PULocationID", "DOLocationID");
ANALYZE yellow_taxi_data;
ALTER TABLE yellow_taxi_data SET (autovacuum_enabled = true);
```

## Monitoring & Debugging

### View Kestra Logs
```bash
# All logs
docker logs kestra -f

# Filter for errors
docker logs kestra 2>&1 | grep ERROR

# Filter for specific execution
docker logs kestra 2>&1 | grep "execution_id"
```

### View Database Logs
```bash
docker logs pgdatabase -f
```

### Check Disk Space
```bash
# Check volumes
docker system df -v

# Check container disk usage
docker exec kestra df -h
docker exec pgdatabase df -h
```

### Monitor Execution in Real-Time
```bash
# Via UI: Flows → Executions → Click execution → Logs

# Via CLI (if installed):
kestra flow logs data.pipelines nyc_taxi_data_ingestion_working
```

## Advanced: Running Multiple Months

### Sequential (Safer)
```yaml
# Execute manually for each month
inputs:
  year: 2021
  month: 1  # Then 2, 3, 4, etc.
```

### Parallel (Faster)
Create a wrapper flow:

```yaml
id: nyc_taxi_bulk_ingestion
namespace: data.pipelines

inputs:
  - id: year
    type: INT
    defaults: 2021

tasks:
  - id: ingest_all_months
    type: io.kestra.plugin.core.flow.Parallel
    tasks:
      - id: month_1
        type: io.kestra.plugin.core.flow.Subflow
        namespace: data.pipelines
        flowId: nyc_taxi_data_ingestion_working
        inputs:
          year: "{{ inputs.year }}"
          month: 1
          
      - id: month_2
        type: io.kestra.plugin.core.flow.Subflow
        namespace: data.pipelines
        flowId: nyc_taxi_data_ingestion_working
        inputs:
          year: "{{ inputs.year }}"
          month: 2
      
      # ... add more months
```

## Backup & Recovery

### Backup Data
```bash
# Backup specific table
docker exec pgdatabase pg_dump -U root -d ny_taxi -t yellow_taxi_data > backup.sql

# Backup entire database
docker exec pgdatabase pg_dump -U root ny_taxi > ny_taxi_backup.sql
```

### Restore Data
```bash
# Restore
docker exec -i pgdatabase psql -U root -d ny_taxi < backup.sql
```

## Production Checklist

Before going to production:

- [ ] Enable Kestra basic auth (already done in your compose)
- [ ] Use secrets for passwords
- [ ] Set up monitoring/alerting
- [ ] Configure regular backups
- [ ] Test disaster recovery
- [ ] Document dependencies
- [ ] Set up proper logging
- [ ] Configure resource limits
- [ ] Test with production data volume
- [ ] Set up SSL/TLS for PostgreSQL

## Your Complete Workflow

```bash
# 1. Start services
docker-compose up -d

# 2. Wait for services to be healthy
docker ps

# 3. Deploy flow
# Go to http://localhost:8080
# Login: admin@kestra.io / Admin1234!
# Paste the flow YAML

# 4. Execute
# Click "Execute" button

# 5. Monitor
# Watch the execution in real-time
# Check logs for any issues

# 6. Verify
# Go to http://localhost:8085 (pgAdmin)
# Connect to pgdatabase
# Query: SELECT COUNT(*) FROM yellow_taxi_data;

# 7. Stop services
docker-compose down

# 8. Celebrate! 🎉
```

## Need Help?

1. Check Kestra logs: `docker logs kestra`
2. Check the execution logs in UI
3. Verify all services are running: `docker ps`
4. Test database connection manually
5. Check network connectivity

The flow is ready to use with your exact docker-compose setup! 🚀