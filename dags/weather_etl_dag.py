from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add your project path so Airflow can find your scripts
sys.path.insert(0, "/mnt/d/Weather ETL pipeline")

from scripts.extract import extract_weather
from scripts.transform import transform_data
from scripts.load import load_data

# Cities to process
CITIES = [
    {"name": "Amman",  "lat": 31.9539, "lon": 35.9106},
    {"name": "Dubai",  "lat": 25.2048, "lon": 55.2708},
    {"name": "Riyadh", "lat": 24.7136, "lon": 46.6753},
    {"name": "Cairo",  "lat": 30.0444, "lon": 31.2357},
    {"name": "Beirut", "lat": 33.8938, "lon": 35.5018},
]

# Default DAG arguments
default_args = {
    "owner": "jad",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def run_etl_for_city(city_name, lat, lon):
    """Full ETL for a single city."""
    raw_data = extract_weather(lat, lon, city_name)
    if raw_data is None:
        raise ValueError(f"Extraction failed for {city_name}")
    
    transformed_data = transform_data(raw_data)
    if transformed_data is None:
        raise ValueError(f"Transformation failed for {city_name}")
    
    load_data(transformed_data)
    print(f"ETL complete for {city_name} — Temp: {transformed_data['temperature']}°C")

# Create the DAG
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="Hourly weather ETL pipeline for MENA cities",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "etl", "mena"],
) as dag:

    # Create one task per city
    for city in CITIES:
        PythonOperator(
            task_id=f"etl_{city['name'].lower()}",
            python_callable=run_etl_for_city,
            op_kwargs={
                "city_name": city["name"],
                "lat": city["lat"],
                "lon": city["lon"],
            },
        )