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

        # 1. Insert city OR get existing city_id
        cursor.execute("""
            INSERT INTO city_dim (city_name)
            VALUES (%s)
            ON CONFLICT (city_name) DO NOTHING
            RETURNING id;
        """, (data["city"],))

        result = cursor.fetchone()

        if result:
            city_id = result[0]
        else:
            cursor.execute(
                "SELECT id FROM city_dim WHERE city_name = %s",
                (data["city"],)
            )
            city_id = cursor.fetchone()[0]

        # 2. Insert weather data (fact table)
        cursor.execute("""
            INSERT INTO weather_fact (
                city_id, temperature, humidity, description, timestamp, datetime, run_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_id, timestamp) DO NOTHING;
        """, (
            city_id,
            data["temperature"],
            data["humidity"],
            data["description"],
            data["timestamp"],
            data["datetime"],
            data["run_time"]
        ))

        conn.commit()

        if cursor.rowcount == 1:
            logging.info("New record inserted into weather_fact")
        else:
            logging.warning("Duplicate detected → skipped insert")

    except Exception as e:
        logging.exception(f"Database error: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()