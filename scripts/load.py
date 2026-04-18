from scripts.db_connection import get_connection
import logging
import os
import sys

def load_data(data):
    if data is None:
        logging.error("load_data received None")
        return

    conn = None
    cursor = None

    try:
        logging.info(
            f"DB load starting | python={sys.executable} | "
            f"DB_NAME={os.getenv('DB_NAME')} | DB_HOST={os.getenv('DB_HOST')} | "
            f"city={data['city']} | timestamp={data['timestamp']} | run_time={data['run_time']}"
        )

        conn = get_connection()
        cursor = conn.cursor()

        query = """
        INSERT INTO weather_data (city, temperature, humidity, description, timestamp, datetime, run_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(query, (
            data["city"],
            data["temperature"],
            data["humidity"],
            data["description"],
            data["timestamp"],
            data["datetime"],
            data["run_time"]
        ))

        conn.commit()

        logging.info(f"cursor.rowcount={cursor.rowcount}")

        if cursor.rowcount == 1:
            logging.info("Data inserted successfully")
        else:
            logging.warning(
                f"No row inserted. Likely duplicate conflict for "
                f"city={data['city']} timestamp={data['timestamp']}"
            )

    except Exception as e:
        logging.exception(f"Database error: {e}")
        raise

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()