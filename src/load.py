import sqlite3
import pandas as pd
import json
import src.db as db
from datetime import datetime
from src.logger import logger


def save_raw(data: dict):
    conn = None
    try:
        conn = db.get_connection()
        db.create_schema(conn)
        cursor = conn.cursor()

        # --- insert raw blob ---
        cursor.execute(
            "INSERT INTO raw_weather (ingested_at, payload) VALUES (?, ?)",
            (datetime.utcnow().isoformat(), json.dumps(data))
        )

        conn.commit()

    except sqlite3.Error as e:
        logger.error(f"save_raw: sqlite3 error → {e}")
        raise  # fatal for the run

    finally:
        if conn is not None:
            conn.close()


def load_hourly(df: pd.DataFrame) -> None:
    """Insert cleaned hourly DataFrame into weather_hourly."""
    conn = None
    try:
        conn = db.get_connection()
        db.create_schema(conn)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            try:
                ts = row["timestamp"]
                if not isinstance(ts, str):
                    ts = ts.isoformat()

                cursor.execute("""
                INSERT OR IGNORE INTO weather_hourly (
                    timestamp, latitude, longitude,
                    temperature_c, relative_humidity,
                    precipitation_mm, weather_code,
                    ingestion_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ts,
                    row["latitude"],
                    row["longitude"],
                    row["temperature_c"],
                    row["relative_humidity"],
                    row["precipitation_mm"],
                    row["weather_code"],
                    row["ingestion_date"],
                ))

            except (KeyError, TypeError) as e:
                logger.warning(f"load_hourly: bad row → {e}, skipped")
                continue

        conn.commit()

    except sqlite3.Error as e:
        logger.error(f"load_hourly: fatal sqlite error → {e}")
        raise

    finally:
        if conn is not None:
            conn.close()
