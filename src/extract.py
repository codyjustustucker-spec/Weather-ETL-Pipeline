import requests
from src.logger import logger


def fetch_weather(config):
    params = {
        "latitude": config.latitude,
        "longitude": config.longitude,
        "hourly": "temperature_2m,relativehumidity_2m,precipitation,weathercode",
        "timezone": "auto",
    }

    try:
        # --- API call ---
        response = requests.get(
            config.api_base_url,
            params=params,
            timeout=config.api_timeout
        )

        # --- HTTP error? ---
        response.raise_for_status()

        # --- JSON decode ---
        data = response.json()

        logger.info(
            f"extract: HTTP {response.status_code}, {len(data['hourly']['time'])} hourly records"
        )

        return data

    except requests.exceptions.Timeout:
        logger.error("extract: timeout contacting API")
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"extract: network/HTTP error → {e}")
        return None

    except ValueError as e:
        logger.error(f"extract: JSON decode error → {e}")
        return None
