import sqlite3
from pathlib import Path
from src import db
from src.summarize import write_daily_summary


def test_write_daily_summary_creates_csv(tmp_path, monkeypatch):
    # point DB to temp file
    db_path = tmp_path / "test_weather.db"
    monkeypatch.setattr(db, "DB_PATH", str(db_path))

    # make sure output dir is under tmp_path/data/daily_summary
    out_root = tmp_path / "data"
    out_dir = out_root / "daily_summary"
    out_dir.mkdir(parents=True, exist_ok=True)
    # monkeypatch Path in summarize if you later parameterize it;
    # for now we just ensure cwd=tmp_path
    monkeypatch.chdir(tmp_path)

    # create schema + seed rows
    conn = db.get_connection()
    db.create_schema(conn)
    cur = conn.cursor()

    rows = [
        (
            "2025-12-01T00:00",
            47.0,
            -66.0,
            10.0,
            80,
            1.0,
            1,
            "2025-12-01",
        ),
        (
            "2025-12-01T01:00",
            47.0,
            -66.0,
            12.0,
            82,
            2.0,
            1,
            "2025-12-01",
        ),
    ]

    cur.executemany(
        """
        INSERT INTO weather_hourly (
            timestamp, latitude, longitude,
            temperature_c, relative_humidity,
            precipitation_mm, weather_code,
            ingestion_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()

    # run summarize
    write_daily_summary()

    # check CSV
    csv_path = Path("data/daily_summary/daily_summary.csv")
    assert csv_path.exists()

    content = csv_path.read_text().strip().splitlines()
    # header + 1 row expected
    assert len(content) == 2

    header = content[0].split(",")
    assert header == [
        "date",
        "latitude",
        "longitude",
        "temp_min",
        "temp_max",
        "temp_avg",
        "total_precip_mm",
        "num_records",
    ]

    data_row = content[1].split(",")
    # temp_min 10.0, temp_max 12.0, avg 11.0, total_precip 3.0, records 2
    assert data_row[0] == "2025-12-01"
    assert float(data_row[3]) == 10.0
    assert float(data_row[4]) == 12.0
    assert float(data_row[6]) == 3.0
    assert int(data_row[7]) == 2
