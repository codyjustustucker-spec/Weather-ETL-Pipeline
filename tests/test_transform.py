import pandas as pd
from src.transform import hourly_to_df


def test_hourly_to_df_basic():
    # fake API JSON
    data = {
        "hourly": {
            "time": ["2025-12-01T00:00", "2025-12-01T01:00"],
            "temperature_2m": [10.0, 11.0],
            "relativehumidity_2m": [80, 82],
            "precipitation": [0.1, 0.0],
            "weathercode": [1, 2],
        }
    }

    df = hourly_to_df(data, latitude=47.0, longitude=-66.0)

    # shape
    assert len(df) == 2

    # columns
    expected_columns = {
        "timestamp",
        "latitude",
        "longitude",
        "temperature_c",
        "relative_humidity",
        "precipitation_mm",
        "weather_code",
        "ingestion_date",
    }
    assert set(df.columns) == expected_columns

    # values
    assert df["latitude"].iloc[0] == 47.0
    assert df["longitude"].iloc[0] == -66.0
    assert df["temperature_c"].tolist() == [10.0, 11.0]
