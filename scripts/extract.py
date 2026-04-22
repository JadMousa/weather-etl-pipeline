import requests
import os
from dotenv import load_dotenv
import logging
import time

load_dotenv()

API_KEY = os.getenv('API_KEY')


def extract_weather(lat, lon, city_name):
    """
    Extract weather data from OpenWeather API for a given latitude and longitude.
    Retries up to 3 times on failure.
    """
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    )

    for attempt in range(3):
        try:
            response = requests.get(url, timeout=30)

            if response.status_code == 200:
                data = response.json()

                weather_data = {
                    "city": city_name,  # use your name, not API's
                    "temperature": data["main"]["temp"],
                    "humidity": data["main"]["humidity"],
                    "description": data["weather"][0]["description"],
                    "timestamp": data["dt"]
                }

                logging.info(f"Extracted weather for {weather_data['city']} (lat={lat}, lon={lon})")
                return weather_data

            else:
                logging.warning(f"Attempt {attempt + 1}: API Error {response.status_code} for lat={lat}, lon={lon}")

        except Exception as e:
            logging.warning(f"Attempt {attempt + 1}: Request failed for lat={lat}, lon={lon}: {e}")

        time.sleep(2)

    logging.error(f"Extraction failed after 3 retries for lat={lat}, lon={lon}")
    return None