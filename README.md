# Data Engineering Repository

A learning-focused data engineering project demonstrating Docker containerization, PostgreSQL database management, and data pipeline development with NYC taxi data.

## 📋 Overview

This repository contains educational materials and practical implementations for building data pipelines, working with Docker, and managing data infrastructure. The project focuses on:

- **Containerization** with Docker and Docker Compose
- **Database Management** using PostgreSQL
- **Data Pipelines** for ETL operations
- **Data Ingestion** from public datasets (NYC Taxi Data)
- **Infrastructure as Code** with Terraform

## 📁 Repository Structure

### `documentation/`
Complete learning guides covering:
- **01_intro-to-docker.md** - Docker fundamentals, containerization concepts, and basic Docker commands
- **02_virtual-environment-setup.md** - Python virtual environments and data pipeline architecture
- **03_dockerize-pipeline.md** - Building and containerizing data pipelines
- **04_run-postgres-docker.md** - PostgreSQL setup with Docker
- **05_ingest-data.md** - Data ingestion techniques using NYC Taxi dataset

### `pipeline/`
Core data pipeline implementation:
- **pipeline.py** - Main pipeline logic for data processing
- **main.py** - Entry point for pipeline execution
- **Dockerfile** - Containerization for the pipeline
- **pyproject.toml** - Python dependencies (pandas, SQLAlchemy, PostgreSQL support)

### `homework_1_docker_and_terraform/`
Practical homework exercises covering:
- Docker basics and networking
- SQL operations
- Docker Compose
- Terraform infrastructure

### `homework_2_kestra/`
Advanced workflow orchestration with Kestra:
- **docker-compose.yml** - Kestra server setup
- **download** - Data download utilities
- Complete Kestra workflow examples

## 🚀 Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.13+
- Git

### Quick Start with Docker

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data-engineering




   
