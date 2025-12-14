import pandas as pd
import config
from datetime import datetime, UTC
from src.logger import logger
from src.backend_client import record_event

datetime.now(UTC)


def hourly_to_df(data: dict, latitude: float, longitude: float, config) -> pd.DataFrame:
    run_id = record_event(system_id=config.LSO_SYSTEM_ID,
                          stage="transform", event="start")

    try:
        # ---- transform logic ----
        h = data["hourly"]
        times = h["time"]
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

        # ---- success telemetry ----
        record_event(
            system_id=config.LSO_SYSTEM_ID,
            run_id=run_id,
            stage="transform",
            event="success",
            payload={"rows": len(df)}
        )

        return df

    except Exception as e:
        record_event(
            system_id=config.LSO_SYSTEM_ID,
            run_id=run_id,
            stage="transform",
            event="fail",
            level="error",
            payload={"error": str(e)}
        )
        raise
