# Weather ETL Pipeline

## Overview

A production-style ETL pipeline that collects real-time weather data from the OpenWeather API,
transforms it into a structured format, stores it in PostgreSQL, and orchestrates execution
across multiple cities using Apache Airflow.

The pipeline also includes an interpretation layer that converts raw weather data into
actionable insights for different user types — demonstrating how data pipelines can support
real-world decision-making, not just data storage.

---

## Architecture

```
OpenWeather API
      ↓
Extract (Python)
      ↓
S3 (Raw Data Lake)
      ↓
Transform (Python)
      ↓
PostgreSQL (city_dim + weather_fact)
      ↓
Insights (Advice Layer)

Airflow orchestrates all stages on an hourly schedule.
Logging applied across all stages.
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python | ETL pipeline logic |
| Apache Airflow | Workflow orchestration & scheduling |
| PostgreSQL | Data warehouse (star schema) |
| AWS S3 | Raw data lake layer |
| OpenWeather API | Data source |
| dotenv | Secure configuration management |

---

## Airflow DAG

The pipeline runs as an Airflow DAG with one task per city, executing in parallel on an hourly schedule.

![Airflow DAG](airflow_image/airflow_dag.png)

**Cities monitored:**
- Amman, Jordan
- Dubai, UAE
- Riyadh, Saudi Arabia
- Cairo, Egypt
- Beirut, Lebanon

---

## Pipeline Steps

1. **Extract** — Retrieves real-time weather data via OpenWeather API using lat/lon coordinates. Retries up to 3 times on failure.
2. **Transform** — Cleans and structures the data: type casting, Unix timestamp conversion, datetime formatting.
3. **Load** — Inserts structured data into PostgreSQL with duplicate prevention using ON CONFLICT constraints.

---

## Data Model

```
city_dim                    weather_fact
─────────────────           ──────────────────────────
id (PK)                     id (PK)
city_name (UNIQUE)          city_id (FK → city_dim)
                            temperature
                            humidity
                            description
                            timestamp
                            datetime
                            run_time
```

---

## Features

- Multi-city support — pipeline scales to any number of cities via configuration
- Duplicate prevention using database constraints and ON CONFLICT
- Retry logic — each city retries up to 3 times on API failure
- Environment variables for secure credential management
- Modular code structure — extract, transform, load fully separated
- S3 raw data layer — separates ingestion from processing
- Audience-based insight generation — tailored advice for general users, outdoor workers, and tourists
- Full logging across all pipeline stages

---

## Example Output

```
────────────────────────────────────────
City:        Amman
Temperature: 21.4°C
Humidity:    17%
Description: clear sky
Audience:    general
Advice:      Weather is cool and comfortable today.
────────────────────────────────────────
Pipeline complete — 5 succeeded, 0 failed
```

---

## Project Evolution

This project was built incrementally to simulate real-world data engineering practices:

- **v1** — Single-city Python ETL script with manual execution
- **v2** — Windows Task Scheduler for basic automation
- **v3** — AWS S3 integration as a raw data lake layer
- **v4** — Multi-city support with configurable CITIES list
- **v5** — Apache Airflow for professional orchestration, scheduling, and monitoring

---

## Design Decisions

- Used **latitude and longitude** instead of city name for accurate API data retrieval
- **City names are configured** in code rather than relying on API-returned names, ensuring consistency in the database
- Stored both **Unix timestamp and datetime** to support raw data integrity and readable analysis
- **Star schema** (city_dim + weather_fact) for scalable analytics
- **ON CONFLICT** constraints ensure idempotent inserts — safe to re-run without duplicates
- Separated **database connection logic** for better code organization
- **Advice layer kept separate** from ETL logic to maintain clean separation of concerns

---

## How to Run

1. Create a `.env` file:
```
API_KEY=your_openweather_key
DB_NAME=weather_etl
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run manually:
```bash
python main.py
```

4. Run via Airflow:
```bash
export AIRFLOW_HOME=~/airflow
source ~/airflow-env/bin/activate
airflow webserver --port 8080
airflow scheduler
```
Then open `http://localhost:8080` and trigger the `weather_etl_pipeline` DAG.