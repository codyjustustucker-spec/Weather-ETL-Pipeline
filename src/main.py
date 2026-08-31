from src.extract import fetch_weather
from src.load import save_raw, load_hourly
from src.transform import hourly_to_df
from src.config_loader import load_config
from src.logger import logger
from src.summarize import write_daily_summary
from src.backend_client import send_events_to_backend

config = load_config()


def main():
    data = fetch_weather(config)
    if data is None:
        logger.error("main: extract failed, aborting")
        raise SystemExit(1)

    save_raw(data, config)
    df = hourly_to_df(data, config.latitude, config.longitude, config)
    load_hourly(df, config)
    write_daily_summary()

    # Optional LSO telemetry integration
    if config.LSO_ENABLED:
        result = send_events_to_backend(
            backend_url=f"{config.LSO_URL}/systems/{config.LSO_SYSTEM_ID}/events"
        )

        print(result)

        if not result.get("cleared"):
            logger.warning(f"telemetry not sent: {result}")


if __name__ == "__main__":
    main()
