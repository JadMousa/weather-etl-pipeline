def get_weather_advice(data, audience):
    temp = data["temperature"]
    humidity = data["humidity"]
    description = data["description"]
    audience = audience.lower()

    if audience == "general":
        if temp > 35:
            return "Very hot today. Stay hydrated and avoid outdoor activity at noon."
        elif temp >= 25:
            return "Weather is warm today."
        else:
            return "Weather is cool and comfortable today."

    elif audience == "worker":
        if temp > 35:
            return "High heat risk for outdoor workers. Take breaks and drink water regularly."
        elif temp >= 25:
            return "Moderate conditions for outdoor work. Stay hydrated."
        else:
            return "Conditions are generally good for outdoor work."

    elif audience == "tourist":
        if temp > 35:
            return "Not ideal for midday sightseeing. Early morning or evening is better."
        elif "rain" in description:
            return "Weather may affect outdoor sightseeing plans."
        else:
            return "Good weather for sightseeing."

    return "No advice available for this audience."