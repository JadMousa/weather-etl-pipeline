import requests
import os
from dotenv import load_dotenv
import logging

load_dotenv()

API_KEY = os.getenv('API_KEY')

def extract_weather():
    url = f"https://api.openweathermap.org/data/2.5/weather?lat=31.95&lon=35.91&appid={API_KEY}&units=metric"

    try:
        response = requests.get(url)

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
            logging.error(f"API Error: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print("Request failed:", e)
        return None