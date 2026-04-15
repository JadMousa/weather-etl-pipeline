from scripts.db_connection import get_connection
import logging

def load_data(data):
    if data is None:
        return

    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
        INSERT INTO weather_data (city, temperature, humidity, description, timestamp, datetime)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (city, timestamp) DO NOTHING
        """

        cursor.execute(query, (
            data["city"],
            data["temperature"],
            data["humidity"],
            data["description"],
            data["timestamp"],
            data["datetime"]
        ))

        conn.commit()

        cursor.close()
        conn.close()

        logging.info("Data inserted successfully")

    except Exception as e:
        print("Database error:", e)