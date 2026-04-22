import logging
import sys
import os

from scripts.extract import extract_weather
from scripts.transform import transform_data
from scripts.load import load_data
from scripts.advice import get_weather_advice
from scripts.s3_upload import save_raw_to_s3

# ── Logging setup ────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_dir  = os.path.join(BASE_DIR, "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "weather_etl.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ── Cities configuration ─────────────────────────────────────────────────────
# Add or remove cities here freely — the pipeline will handle each one.

CITIES = [
    {"name": "Amman",   "lat": 31.9539, "lon": 35.9106},
    {"name": "Dubai",   "lat": 25.2048, "lon": 55.2708},
    {"name": "Riyadh",  "lat": 24.7136, "lon": 46.6753},
    {"name": "Cairo",   "lat": 30.0444, "lon": 31.2357},
    {"name": "Beirut",  "lat": 33.8938, "lon": 35.5018},
]

# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline_for_city(city, audience):
    """Run the full ETL pipeline for a single city entry."""
    name = city["name"]
    logging.info(f"Processing city: {name}")

    # Extract
    raw_data = extract_weather(city["lat"], city["lon"], city["name"])
    if raw_data is None:
        logging.error(f"Extraction failed for {name} — skipping")
        return

    # Upload raw data to S3
    try:
        save_raw_to_s3(raw_data)
    except Exception as e:
        logging.warning(f"S3 upload skipped for {name}: {e}")

    # Transform
    transformed_data = transform_data(raw_data)
    if transformed_data is None:
        logging.error(f"Transformation failed for {name} — skipping")
        return

    logging.info(
        f"Transformed | city={transformed_data['city']} | "
        f"timestamp={transformed_data['timestamp']} | datetime={transformed_data['datetime']}"
    )

    # Load
    load_data(transformed_data)

    # Advice
    advice = get_weather_advice(transformed_data, audience)

    # Output
    print(f"\n{'─' * 40}")
    print(f"City:        {transformed_data['city']}")
    print(f"Temperature: {transformed_data['temperature']}°C")
    print(f"Humidity:    {transformed_data['humidity']}%")
    print(f"Description: {transformed_data['description']}")
    print(f"Audience:    {audience}")
    print(f"Advice:      {advice}")

    logging.info(f"Pipeline finished successfully for {name}")


def main():
    logging.info("=" * 60)
    logging.info("Weather ETL pipeline started")

    # Audience: pass as CLI arg for manual runs, defaults to 'general' for automation
    audience = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "general"

    success_count = 0
    fail_count    = 0

    for city in CITIES:
        try:
            run_pipeline_for_city(city, audience)
            success_count += 1
        except Exception as e:
            logging.exception(f"Unexpected error for {city['name']}: {e}")
            fail_count += 1

    print(f"\n{'=' * 40}")
    print(f"Pipeline complete — {success_count} succeeded, {fail_count} failed")
    print(f"{'=' * 40}")
    logging.info(f"Pipeline complete — {success_count} succeeded, {fail_count} failed")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
    sys.exit(0)