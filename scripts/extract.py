import requests
import os
from dotenv import load_dotenv
import logging
import time

load_dotenv()

API_KEY = os.getenv('API_KEY')

def extract_weather():
    url = f"https://api.openweathermap.org/data/2.5/weather?lat=31.95&lon=35.91&appid={API_KEY}&units=metric"

import time

def extract_weather():
    url = f"https://api.openweathermap.org/data/2.5/weather?lat=31.95&lon=35.91&appid={API_KEY}&units=metric"

    for attempt in range(3):   # retry 3 times
        try:
            response = requests.get(url, timeout=30)

            if response.status_code == 200:
                data = response.json()

                weather_data = {
                    "city": data["name"],
                    "temperature": data["main"]["temp"],
                    "humidity": data["main"]["humidity"],
                    "description": data["weather"][0]["description"],
                    "timestamp": data["dt"]
                }

                return weather_data

            else:
                logging.warning(f"Attempt {attempt+1}: API Error {response.status_code}")

        except Exception as e:
            logging.warning(f"Attempt {attempt+1}: Request failed: {e}")

        time.sleep(2)  # wait before retry

    logging.error("Extraction failed after retries")
    return None