import logging
from scripts.extract import extract_weather
from scripts.transform import transform_data
from scripts.load import load_data
from scripts.advice import get_weather_advice
from scripts.s3_upload import save_raw_to_s3
import sys
import os


# Get folder where main.py is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Build correct path to logs folder
log_dir = os.path.join(BASE_DIR, "logs")

# Create logs folder if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

# Full log file path
log_file = os.path.join(log_dir, "weather_etl.log")

# Setup logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info("Weather ETL pipeline started")

    data = extract_weather()
    if data is None:
        logging.error("Extraction failed")
        return
    try:
        save_raw_to_s3(data)
    except Exception as e:
        logging.warning(f"S3 upload skipped: {e}")  
    
    transformed_data = transform_data(data)
    if transformed_data is None:
        logging.error("Transformation failed")
        return
    
    logging.info(
    f"About to call load_data | city={transformed_data['city']} | "
    f"timestamp={transformed_data['timestamp']} | datetime={transformed_data['datetime']}"
)

    load_data(transformed_data)
    if len(sys.argv) > 1:
        audience = sys.argv[1].strip().lower()
    else:
        audience = "general"   # default for automation

    advice = get_weather_advice(transformed_data, audience)

    print("\nWeather ETL pipeline finished successfully.")
    print(f"City: {transformed_data['city']}")
    print(f"Temperature: {transformed_data['temperature']}°C")
    print(f"Humidity: {transformed_data['humidity']}%")
    print(f"Description: {transformed_data['description']}")
    print(f"Audience: {audience}")
    print(f"Advice: {advice}")

    logging.info("Weather ETL pipeline finished successfully")

if __name__ == "__main__":
    main()
    sys.exit(0)