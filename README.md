Weather ETL Pipeline

A clean, production-style Python ETL pipeline that extracts hourly weather data from Open-Meteo, transforms it with pandas, loads it into SQLite, and generates daily summary CSVs.
Designed as a portfolio-ready demonstration of modular architecture, config management, logging, and testing.

Quickstart
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python -m src.main


Output:

New records in data/weather.db

Daily summary CSV in data/daily_summary/

Logs in logs/pipeline.log

Architecture Overview

The pipeline follows a real data-engineering pattern:

Extract → API request to Open-Meteo

Transform → JSON → clean pandas DataFrames

Load → SQLite (raw_weather, weather_hourly), with idempotent upserts

Summarize → min/max/mean → CSV

Log → All steps logged for traceability

Test → pytest validates transforms + loading + summary logic

Repository Structure
weather_etl_pipeline/
│
├── config/
│   ├── __init__.py
│   └── settings.yaml
│
├── data/
│   ├── weather.db
│   └── daily_summary/
│       └── daily_summary.csv
│
├── logs/
│   └── pipeline.log
│
├── src/
│   ├── __init__.py
│   ├── config_loader.py
│   ├── db.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── summarize.py
│   └── main.py
│
├── tests/
│
├── .env.example
├── pyproject.toml
├── requirements.txt
└── README.md

Features

Modular src/ package (clean architecture)

.env + YAML configuration (API URL, timeout, variables, lat/long)

Error-handled API calls (response.raise_for_status())

pandas transforms for cleaning + schema enforcement

SQLite storage with:

raw_weather: raw API rows

weather_hourly: clean hourly values with unique timestamp

Idempotent upsert logic (no duplicate rows)

Automatic daily summary CSV generation

Logging to logs/pipeline.log

pytest tests for transform, load, and summary correctness

Run the Pipeline
python -m src.main


This performs:

API extract

DataFrame transforms

SQLite load + upsert

Summary CSV creation

Logging

Logging

Logs appear automatically at:

logs/pipeline.log


Example:

2024-01-01 08:00:12 INFO  HTTP 200, 24 records received
2024-01-01 08:00:12 INFO  Insert into raw_weather complete
2024-01-01 08:00:12 INFO  Hourly upsert complete

Testing

Run all tests:

pytest


Tests validate:

JSON → DataFrame cleaning

Schema creation

Insert + upsert logic

Summary calculations

Example Output

Daily summary CSV:

date,min_temp,max_temp,avg_temp
2024-01-01,-3.2,4.8,1.1

Scheduling Options

Windows Task Scheduler

cron

GitHub Actions (optional CI enhancement)
