import logging
from scripts.extract import extract_weather
from scripts.transform import transform_data
from scripts.load import load_data
from scripts.advice import get_weather_advice
from scripts.s3_upload import save_raw_to_s3

logging.basicConfig(
    filename="logs/weather_etl.log",
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

    load_data(transformed_data)
    audience = input("Enter audience (general / worker / tourist): ").strip().lower()
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