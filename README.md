# Weather ETL Pipeline

## Overview
This project implements an ETL pipeline that collects real-time weather data from the OpenWeather API, transforms it into a structured format, and stores it in PostgreSQL for time-series analysis.

## Tech Stack
- Python
- PostgreSQL
- OpenWeather API

## Pipeline Steps
1. Extract: API call using requests
2. Transform: clean and standardize data
3. Load: insert into PostgreSQL

## Features
- Uses environment variables (.env)
- Prevents duplicate inserts
- Converts timestamp to datetime
- Logs pipeline execution

## Key Design Decisions

- Used latitude and longitude instead of city name to ensure accurate data retrieval
- Stored both timestamp and datetime to support raw data integrity and readable analysis
- Implemented duplicate prevention using database constraints
- Separated database connection logic for better code organization

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