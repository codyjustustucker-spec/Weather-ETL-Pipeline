import sqlite3

DB_PATH = "data/weather.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


def create_schema(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_weather (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ingested_at TEXT NOT NULL,
        payload     TEXT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_hourly (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp         TEXT NOT NULL,
        latitude          REAL NOT NULL,
        longitude         REAL NOT NULL,
        temperature_c     REAL,
        relative_humidity REAL,
        precipitation_mm  REAL,
        weather_code      INTEGER,
        ingestion_date    TEXT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_unique
    ON weather_hourly (timestamp, latitude, longitude, ingestion_date);
    """)

    conn.commit()
