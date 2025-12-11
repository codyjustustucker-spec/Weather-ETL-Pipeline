import pandas as pd
from datetime import datetime, UTC
from src.logger import logger

datetime.now(UTC)


def hourly_to_df(data: dict, latitude: float, longitude: float) -> pd.DataFrame:
    """Raw API JSON -> clean hourly DataFrame."""
    try:
        h = data["hourly"]
        times = h["time"]
    except KeyError as e:
        logger.error(f"hourly_to_df: missing keys → {e}")
        raise

    n = len(times)

    def column(key: str):
        values = h.get(key)
        if values is None:
            return [None] * n
        values = list(values)
        if len(values) < n:
            values += [None] * (n - len(values))
        return values[:n]

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(times),
            "latitude": [latitude] * n,
            "longitude": [longitude] * n,
            "temperature_c": column("temperature_2m"),
            "relative_humidity": column("relativehumidity_2m"),
            "precipitation_mm": column("precipitation"),
            "weather_code": column("weathercode"),
        }
    )

    df["ingestion_date"] = datetime.now(UTC).date().isoformat()
    return df
