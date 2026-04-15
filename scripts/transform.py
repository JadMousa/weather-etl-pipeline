from datetime import datetime

def transform_data(data):
    if data is None:
        return None

    transformed = {
        "city": data["city"],
        "temperature": float(data["temperature"]),
        "humidity": int(data["humidity"]),
        "description": data["description"].lower(),
        "timestamp": int(data["timestamp"]),
        "datetime": datetime.fromtimestamp(data["timestamp"])
    }

    return transformed