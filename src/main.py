from src.extract import fetch_weather
from src.load import save_raw, load_hourly
from src.transform import hourly_to_df
from src.config_loader import load_config
from src.logger import logger
from src.summarize import write_daily_summary

config = load_config()


def main():
    data = fetch_weather(config)   # E (extract data)
    if data is None:
        logger.error("main: extract failed, aborting")
        raise SystemExit(1)
    save_raw(data)                         # T (raw layer)
    df = hourly_to_df(data, config.latitude, config.longitude)
    load_hourly(df)                      # L (clean layer)
    write_daily_summary()


if __name__ == "__main__":
    main()
