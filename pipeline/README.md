# NYC Taxi Data Pipeline with Kestra

## Overview
Automated ETL pipeline for NYC taxi data using Kestra orchestration.

## Setup
```bash
docker-compose up -d
```

## Flows
- `taxi-data-etl`: Single month processing
- `taxi-data-backfill-2021`: Process all 2021 data

## Access
- Kestra UI: http://localhost:8081
- PgAdmin: http://localhost:8080

## Homework Answers
1. Yellow taxi Dec 2020 file size: [YOUR ANSWER]
2. Rendered variable: green_tripdata_2020-04.csv
EOF

git init
git add .
git commit -m "Initial commit: Kestra NYC taxi pipeline"
git remote add origin <your-github-repo-url>
git push -u origin main