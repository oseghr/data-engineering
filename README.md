# Data Engineering Repository

A learning-focused data engineering project demonstrating containerization, database management, ETL pipelines, and data engineering best practices using NYC taxi data.

## 📋 Overview

This repository contains educational materials, homework exercises, and example projects that walk through building data pipelines, containerizing applications, deploying infrastructure with Terraform, and building analytics-ready datasets.

Key themes:

- **Containerization** with Docker and Docker Compose
- **Infrastructure as Code** with Terraform
- **Orchestration** using tools like Kestra
- **Data Processing** using Python (ETL/ELT) and SQL
- **Batch & Streaming** pipeline patterns
- **Data Warehouse** loading and reporting

---

## 📁 Repository Structure

### `documentation/`
Learning guides and tutorials:
- `01_intro-to-docker.md` - Docker fundamentals and basic commands
- `02_virtual-environment-setup.md` - Python virtual environments and setup
- `03_dockerize-pipeline.md` - Containerizing data pipelines
- `04_run-postgres-docker.md` - Running PostgreSQL with Docker
- `05_ingest-data.md` - Ingesting NYC Taxi dataset

### `pipeline/`
A standalone ETL pipeline project:
- `pipeline.py` - Core pipeline logic
- `main.py` - Pipeline entrypoint
- `docker-compose.yml` - Local stack (if present)
- `Dockerfile` - Container image definition
- `pyproject.toml` - Python dependencies
- `scripts/` - Helpers for ingestion and setup

### `homework_1_docker_and_terraform/`
Exercises covering Docker, Docker Compose, and SQL.
- `homework.md` / `homework1_solution.md` - Tasks and solutions
- `taxi_zone_lookup.csv` - Reference dataset

### `homework_2_kestra/`
Workflow orchestration with [Kestra](https://kestra.io/):
- `docker-compose.yml` - Kestra server setup
- `flows/` - Example Kestra workflow definitions
- `main.tf` / `terraform.tfstate` - Optional infrastructure components

### `homework_3_datawarehouse/`
Data warehouse loading and SQL analytics:
- `datawarehouse/bq_scripts.sql` - BigQuery load scripts
- `datawarehouse/load_data.py` / `webload.py` - Loading scripts

### `homework_4_analytics/`
Analytics-focused ETL pipelines:
- `load.py`, `loadfhv.py`, `loading.py` - Data loading scripts

### `homework_5_data_platforms/`
A project demonstrating pipeline structure and reporting:
- `project/my-pipeline/` - Dagster-style pipeline definition and assets
- `project/my-pipeline/pipeline/` - Pipeline configuration and SQL assets

### `homework_6_batch_processing/`
Batch processing using Spark:
- `sparkdemo/` - Spark job example and config
- `taxi_zone_lookup.csv` - Lookup data

### `homework_7_streaming/`
Streaming pipeline materials and exercises.

### `newdemo/`
A separate demo project with its own `docker-compose.yml`, Python scripts, and notebooks.

### `testing/`
Utility and test scripts for loading data and running pipelines locally.

---

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (3.13+ works)
- Git

### Quick Start
1. Clone the repo:

```bash
git clone https://github.com/oseghr/data-engineering.git
cd data-engineering
```

2. Pick a folder to work in (e.g., `homework_1_docker_and_terraform/`, `pipeline/`, `homework_6_batch_processing/`).

3. Read the corresponding `homework.md` or `README.md` in that folder for specific execution instructions.

---

## 📌 Notes
- This repository is intended as a learning sandbox; each homework folder is self-contained.
- Some folders contain solution files (`*_solution.md`) for comparison.

---

## 🧠 Want to learn more?
Check the `documentation/` folder for guided walkthroughs and additional context on the core concepts used throughout this repo.




   
