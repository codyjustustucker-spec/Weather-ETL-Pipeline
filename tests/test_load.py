import sqlite3
import pandas as pd
from src import db
from src.load import load_hourly


def test_load_hourly_creates_schema_and_inserts(tmp_path, monkeypatch):
    # point DB_PATH to a temp file
    db_path = tmp_path / "test_weather.db"
    monkeypatch.setattr(db, "DB_PATH", str(db_path))

    # build a tiny DataFrame
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2025-12-01T00:00", "2025-12-01T01:00"]
            ),
            "latitude": [47.0, 47.0],
            "longitude": [-66.0, -66.0],
            "temperature_c": [10.0, 11.0],
            "relative_humidity": [80, 82],
            "precipitation_mm": [0.1, 0.0],
            "weather_code": [1, 2],
            "ingestion_date": ["2025-12-01", "2025-12-01"],
        }
    )

    conn = db.get_connection()
    db.create_schema(conn)
    conn.close()

    # first load → should insert 2 rows
    load_hourly(df)

    # second load with same df → should not create duplicates (UNIQUE index)
    load_hourly(df)

    # check row count
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM weather_hourly;")
    (count,) = cur.fetchone()
    conn.close()

    assert count == 2  # unique constraint working
