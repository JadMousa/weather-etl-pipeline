# Weather ETL Pipeline

## Overview
This project implements an ETL pipeline that collects real-time weather data from the OpenWeather API, transforms it into a structured format, and stores it in PostgreSQL for time-series analysis.

In addition to the core ETL functionality, the project introduces a simple **interpretation layer** that converts raw weather data into **actionable insights for different user types** (e.g., general users, outdoor workers, tourists).
This demonstrates how data pipelines can move beyond storage and support real-world decision-making.

---

## Tech Stack
- Python
- PostgreSQL
- OpenWeather API

---

## Pipeline Steps
1. **Extract**: Retrieve real-time weather data using API calls (`requests`)
2. **Transform**: Clean, standardize, and structure the data (type casting, timestamp conversion)
3. **Load**: Insert structured data into PostgreSQL with duplicate prevention

---

## Features
- Uses environment variables (`.env`) for secure configuration
- Prevents duplicate inserts using database constraints
- Converts Unix timestamps to human-readable datetime
- Logs pipeline execution for monitoring and debugging
- Modular code structure (separation of extract, transform, load)
- **Audience-based insight generation (post-ETL layer)**

---

## Example Output

Enter audience (general / worker / tourist): tourist

City: Amman  
Temperature: 19.73°C  
Humidity: 34%  
Description: overcast clouds  
Advice: Good weather for sightseeing.

---

## Use Case: From Data to Insights

This project extends a traditional ETL pipeline by demonstrating how the same dataset can serve different user needs.

After processing and storing the data, the system generates tailored outputs:

- **General Users** → Daily weather advice  
- **Outdoor Workers / Companies** → Heat risk and safety guidance  
- **Tourists** → Travel and sightseeing recommendations  

This highlights how data pipelines can support **decision-making**, not just data storage.

---

## Key Design Decisions

- Used latitude and longitude instead of city name to ensure accurate data retrieval
- Stored both timestamp and datetime to support raw data integrity and readable analysis
- Implemented duplicate prevention using database constraints
- Separated database connection logic for better code organization
- Kept ETL logic separate from user-oriented interpretation by introducing a dedicated advice layer

## How to Run
1. Create .env file:
   API_KEY=your_key
   DB_NAME=...
   DB_USER=...
   DB_PASSWORD=...
   DB_HOST=localhost

2. Install dependencies:
   pip install -r requirements.txt

3. Run:
   python main.py