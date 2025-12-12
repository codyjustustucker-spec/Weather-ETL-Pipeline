import requests
from src.logger import logger
from src.backend_client import record_event


def fetch_weather(config):
    run_id = record_event(stage="extract", event="start")

    params = {
        "latitude": config.latitude,
        "longitude": config.longitude,
        "hourly": "temperature_2m,relativehumidity_2m,precipitation,weathercode",
        "timezone": "auto",
    }

    try:
        response = requests.get(
            config.api_base_url,
            params=params,
            timeout=config.api_timeout
        )
        response.raise_for_status()
        data = response.json()

        row_count = len(data["hourly"]["time"])

        record_event(
            run_id=run_id,
            stage="extract",
            event="success",
            payload={"rows": row_count, "status_code": response.status_code}
        )

        logger.info(
            f"extract: HTTP {response.status_code}, {row_count} hourly records")
        return data

    except Exception as e:
        record_event(
            run_id=run_id,
            stage="extract",
            event="fail",
            level="error",
            payload={"error": str(e)}
        )

        # keep your existing behavior:
        if isinstance(e, requests.exceptions.Timeout):
            logger.error("extract: timeout contacting API")
            return None
        if isinstance(e, requests.exceptions.RequestException):
            logger.error(f"extract: network/HTTP error → {e}")
            return None
        if isinstance(e, ValueError):
            logger.error(f"extract: JSON decode error → {e}")
            return None

        raise
