import logging
from scripts.extract import extract_weather
from scripts.transform import transform_data
from scripts.load import load_data

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

    transformed_data = transform_data(data)
    if transformed_data is None:
        logging.error("Transformation failed")
        return

    load_data(transformed_data)
    logging.info("Weather ETL pipeline finished successfully")

if __name__ == "__main__":
    main()