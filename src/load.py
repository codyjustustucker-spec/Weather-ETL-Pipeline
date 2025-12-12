import sqlite3
import pandas as pd
import json
import src.db as db
from datetime import datetime
from src.logger import logger
from src.backend_client import record_event


def save_raw(data: dict):
    run_id = record_event(stage="load", event="start")

    conn = None
    try:
        conn = db.get_connection()
        db.create_schema(conn)
        cursor = conn.cursor()

        cursor.execute(
            "INSERT INTO raw_weather (ingested_at, payload) VALUES (?, ?)",
            (datetime.utcnow().isoformat(), json.dumps(data))
        )

        conn.commit()

        record_event(
            run_id=run_id,
            stage="load",
            event="success",
            payload={"rows_inserted": 1}
        )

    except Exception as e:
        record_event(
            run_id=run_id,
            stage="load",
            event="fail",
            level="error",
            payload={"error": str(e)}
        )
        raise

    finally:
        if conn is not None:
            conn.close()


def load_hourly(df: pd.DataFrame) -> None:
    run_id = record_event(stage="load", event="start")

    conn = None
    try:
        conn = db.get_connection()
        db.create_schema(conn)
        cursor = conn.cursor()

        attempted = 0

        for _, row in df.iterrows():
            attempted += 1
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

        record_event(
            run_id=run_id,
            stage="load",
            event="success",
            payload={"rows_attempted": attempted}
        )

    except Exception as e:
        record_event(
            run_id=run_id,
            stage="load",
            event="fail",
            level="error",
            payload={"error": str(e)}
        )
        raise

    finally:
        if conn is not None:
            conn.close()
