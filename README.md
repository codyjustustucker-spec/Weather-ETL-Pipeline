Weather ETL Pipeline

A fully-automated Python ETL pipeline that extracts hourly weather data from Open-Meteo, transforms it into clean DataFrames, loads it into a SQLite database, and generates daily summary CSVs.
Designed as a professional portfolio project demonstrating clean architecture, modular code, testing, configuration management, and logging.

Project Overview

This pipeline runs end-to-end using:

Config-driven API calls

Schema creation and idempotent upserts

pandas transforms

SQLite storage

Automated daily summary generation

Logging to logs/pipeline.log

pytest unit tests

A clean, production-style src/ layout

The repository is structured to mirror real industry data engineering patterns.

Repository Structure
WEATHER_ETL_PIPELINE/
  .venv/
  config/
    __init__.py
    settings.yaml
  data/
    weather.db
    daily_summary/
      daily_summary.csv
  logs/
    pipeline.log
  src/
    __init__.py
    config_loader.py
    db.py
    extract.py
    load.py
    logger.py
    main.py
    summarize.py
    transform.py
  tests/
  .env.example
  Job.txt
  pyproject.toml
  README.md
  requirements.txt

Features

Clean modular architecture (src/ package)

.env + YAML config-based parameters

Request timeout + error handling (response.raise_for_status())

pandas transforms from raw JSON → clean tables

SQLite tables:

raw_weather

weather_hourly (unique timestamp/location rows)

Upsert logic to avoid duplicates

Daily summary CSV (min/max/mean) under data/daily_summary/

Logging to logs/pipeline.log

Unit tests validating transform, load, and summary behavior

Setup
1. Create and activate a virtual environment
python -m venv .venv
.\.venv\Scripts\activate

2. Install dependencies
pip install -r requirements.txt

Configuration
Environment variables

Create your .env based on the example:

cp .env.example .env


Your .env contains secrets or parameters needed for loading YAML settings.

YAML configuration

Located at:

config/settings.yaml


Defines:

API base URL

Request timeout

Hourly variables to pull

Latitude/longitude

Run the Pipeline

From the repository root:

python -m src.main


This executes the full ETL:

Extract weather data from Open-Meteo

Transform JSON → DataFrames

Load into SQLite (data/weather.db)

Upsert hourly values without duplicates

Generate a summary CSV in data/daily_summary/

Record logs in logs/pipeline.log

Logging

Logs are automatically written to:

logs/pipeline.log


Example log entries:

2024-01-01 08:00:12 INFO HTTP 200, 24 records received
2024-01-01 08:00:12 INFO Insert into raw_weather complete
2024-01-01 08:00:12 INFO Hourly upsert complete

Testing

Run unit tests:

pytest


Tests validate:

JSON → DataFrame transforms

Schema creation

Insert and upsert logic

Daily summary correctness

Example Output
Daily summary CSV
date,min_temp,max_temp,avg_temp
2024-01-01,-3.2,4.8,1.1

SQLite tables

raw_weather = raw API response rows

weather_hourly = clean hourly values with unique timestamp/location index

Scheduling

This pipeline can be scheduled to run daily via:

Windows Task Scheduler

Linux cron

GitHub Actions (optional)

Notes

Designed for portfolio clarity and professional readability.

Easily extendable to Postgres, Airflow, or cloud data warehouses.

Code in src/ is structured as a real Python package.