from pathlib import Path
import pandas as pd
import src.db as db
from src.logger import logger


def write_daily_summary():
    """Read weather_hourly and write daily summary CSV."""
    conn = db.get_connection()
    try:
        df = pd.read_sql_query(
            "SELECT timestamp, latitude, longitude, "
            "temperature_c, precipitation_mm "
            "FROM weather_hourly",
            conn,
            parse_dates=["timestamp"],
        )

        if df.empty:
            logger.info("write_daily_summary: no data, skipping")
            return

        df["date"] = df["timestamp"].dt.date

        daily = df.groupby(["date", "latitude", "longitude"], as_index=False).agg(
            temp_min=("temperature_c", "min"),
            temp_max=("temperature_c", "max"),
            temp_avg=("temperature_c", "mean"),
            total_precip_mm=("precipitation_mm", "sum"),
            num_records=("temperature_c", "count"),
        )

        # Make csv look better
        daily["temp_avg"] = daily["temp_avg"].round(2)
        daily["total_precip_mm"] = daily["total_precip_mm"].round(2)

        out_dir = Path("data/daily_summary")
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "daily_summary.csv"
        daily.to_csv(out_path, index=False)
        logger.info(
            f"write_daily_summary: wrote {len(daily)} rows to {out_path}")

    finally:
        conn.close()
