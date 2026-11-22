"""Unit tests for the ridership ETL pipeline (cleaning, merge, analysis, load)."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import Engine, create_engine, text

from analysis import AnalysisResult, analyze
from config import PipelineConfig
from data_pipeline import DataPipeline, PipelineError

PEAK_HOURS = frozenset({7, 8, 9, 10, 16, 17, 18, 19})


# ------------------------------------------------------------------------ #
# Fixtures
# ------------------------------------------------------------------------ #
@pytest.fixture
def config(tmp_path) -> PipelineConfig:
    return PipelineConfig(
        input_csv=str(tmp_path / "ridership.csv"),
        weather_db=str(tmp_path / "weather.db"),
        output_dir=str(tmp_path / "output"),
        processed_db=str(tmp_path / "processed.db"),
    )


@pytest.fixture
def dummy_ridership() -> pd.DataFrame:
    # 2025-09-01 = Mon, 2025-09-02 = Tue, 2025-09-03 = Wed
    return pd.DataFrame(
        {
            "trip_id": ["t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8"],
            "station_id": ["S1", "S2", "S1", "S3", "S1", "S2", "S3", "S4"],
            "timestamp": [
                "2025-09-01 08:00:00",  # Mon 08 -> peak, 120 pax
                "2025-09-02 15:30:00",  # Tue 15 -> off, pax missing -> dropped
                "2025-09-01 18:00:00",  # Mon 18 -> peak, 95 pax
                "2025-09-02 07:30:00",  # Tue 07 -> peak, 140 pax
                "2025-09-03 12:00:00",  # Wed 12 -> off, 60 pax, ticket missing
                "2025-09-03 09:00:00",  # Wed 09 -> peak, 80 pax
                "2025-09-01 11:00:00",  # Mon 11 -> off, pax missing -> dropped
                "2025-09-03 19:00:00",  # Wed 19 -> peak, 110 pax
            ],
            "ticket_type": ["Single", None, "Monthly", "Single", None, "Weekly", None, "Single"],
            "passenger_count": [120, None, 95, 140, 60, 80, None, 110],
        }
    )


@pytest.fixture
def weather_engine() -> Engine:
    """A small in-memory SQLite database with three days of weather."""
    engine = create_engine("sqlite://")  # in-memory
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE daily_weather "
                "(date TEXT, temp_c REAL, precipitation_mm REAL)"
            )
        )
        for d, t, p in [
            ("2025-09-01", 18.0, 0.0),
            ("2025-09-02", 15.5, 2.5),
            ("2025-09-03", 12.0, 8.0),
        ]:
            conn.execute(
                text(
                    "INSERT INTO daily_weather (date, temp_c, precipitation_mm) "
                    "VALUES (:d, :t, :p)"
                ),
                {"d": d, "t": t, "p": p},
            )
    return engine


@pytest.fixture
def file_weather_db(tmp_path) -> Path:
    """A small on-disk SQLite weather database (for load_ridership-style paths)."""
    path = tmp_path / "weather.db"
    conn = sqlite3.connect(path)
    with conn:
        conn.execute(
            "CREATE TABLE daily_weather (date TEXT, temp_c REAL, precipitation_mm REAL)"
        )
        conn.executemany(
            "INSERT INTO daily_weather VALUES (?, ?, ?)",
            [
                ("2025-09-01", 18.0, 0.0),
                ("2025-09-02", 15.5, 2.5),
                ("2025-09-03", 12.0, 8.0),
            ],
        )
    return path


# ------------------------------------------------------------------------ #
# Cleaning
# ------------------------------------------------------------------------ #
def test_clean_drops_missing_passenger_count(
    config: PipelineConfig, dummy_ridership: pd.DataFrame
) -> None:
    pipeline = DataPipeline(config)
    out = pipeline.clean_ridership(dummy_ridership)
    assert len(out) == 6
    assert not out["passenger_count"].isna().any()


def test_clean_fills_missing_ticket_type(
    config: PipelineConfig, dummy_ridership: pd.DataFrame
) -> None:
    out = DataPipeline(config).clean_ridership(dummy_ridership)
    assert (out["ticket_type"] == "Unknown").sum() == 1  # the surviving Wed row
    assert (out["ticket_type"] == "").sum() == 0
    assert not out["ticket_type"].isna().any()


def test_clean_extracts_date_hour_dow(
    config: PipelineConfig, dummy_ridership: pd.DataFrame
) -> None:
    out = DataPipeline(config).clean_ridership(dummy_ridership)
    for col in ("date", "hour", "day_of_week"):
        assert col in out.columns
    t1 = out[out["trip_id"] == "t1"].iloc[0]
    t4 = out[out["trip_id"] == "t4"].iloc[0]
    assert int(t1["hour"]) == 8
    assert int(t1["day_of_week"]) == 0
    assert t1["date"] == pd.Timestamp("2025-09-01")
    assert int(t4["hour"]) == 7
    assert int(t4["day_of_week"]) == 1


# ------------------------------------------------------------------------ #
# Weather loading + merge
# ------------------------------------------------------------------------ #
def test_load_weather_from_in_memory_engine(
    config: PipelineConfig, weather_engine: Engine
) -> None:
    df = DataPipeline(config).load_weather(weather_engine)
    assert len(df) == 3
    assert set(df.columns) == {"date", "temp_c", "precipitation_mm"}
    assert df["date"].dtype.kind == "M"
    assert float(df["precipitation_mm"].iloc[1]) == pytest.approx(2.5)


def test_merge_works(
    config: PipelineConfig,
    dummy_ridership: pd.DataFrame,
    weather_engine: Engine,
) -> None:
    pipeline = DataPipeline(config)
    cleaned = pipeline.clean_ridership(dummy_ridership)
    weather = pipeline.load_weather(weather_engine)
    merged = pipeline.merge(cleaned, weather)
    assert len(merged) == 6
    assert merged["temp_c"].notna().all()
    assert merged["precipitation_mm"].notna().all()
    merged_dates = {d.date() for d in merged["date"]}
    assert merged_dates == {date(2025, 9, 1), date(2025, 9, 2), date(2025, 9, 3)}


def test_merge_left_joins_new_dates_as_null(
    config: PipelineConfig,
    dummy_ridership: pd.DataFrame,
    weather_engine: Engine,
) -> None:
    pipeline = DataPipeline(config)
    cleaned = pipeline.clean_ridership(dummy_ridership)
    # add a weather day that never appears in the ridership data
    with weather_engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO daily_weather (date, temp_c, precipitation_mm) "
                "VALUES ('2025-09-04', 11.0, 4.0)"
            )
        )
        conn.commit()
    weather = pipeline.load_weather(weather_engine)
    merged = pipeline.merge(cleaned, weather)
    assert len(merged) == 6  # left join: no extra row for 2025-09-04
    assert "2025-09-04" not in {d.strftime("%Y-%m-%d") for d in merged["date"]}


# ------------------------------------------------------------------------ #
# Extraction errors
# ------------------------------------------------------------------------ #
def test_missing_csv_raises_pipeline_error(config: PipelineConfig) -> None:
    with pytest.raises(PipelineError, match="not found"):
        DataPipeline(config).load_ridership()


def test_missing_columns_raise_pipeline_error(tmp_path: Path) -> None:
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text("station_id,passenger_count\nS1,5\n", encoding="utf-8")
    cfg = PipelineConfig(
        input_csv=str(bad_csv),
        weather_db=str(tmp_path / "weather.db"),
        output_dir=str(tmp_path / "output"),
        processed_db=str(tmp_path / "processed.db"),
    )
    with pytest.raises(PipelineError, match="missing required columns"):
        DataPipeline(cfg).load_ridership()


def test_weather_db_not_found_raises(config: PipelineConfig) -> None:
    with pytest.raises(PipelineError, match="not found"):
        DataPipeline(config).load_weather("no_such_weather.db")


def test_weather_missing_table_raises(file_weather_db: Path, config: PipelineConfig) -> None:
    tmp_path = file_weather_db.parent
    bad_db = tmp_path / "bad_weather.db"
    conn = sqlite3.connect(bad_db)
    with conn:
        conn.execute("CREATE TABLE other_table (x INTEGER)")
    with pytest.raises(PipelineError, match="daily_weather"):
        DataPipeline(config).load_weather(str(bad_db))


def test_weather_missing_column_raises(file_weather_db: Path, config: PipelineConfig) -> None:
    tmp_path = file_weather_db.parent
    bad_db = tmp_path / "bad_weather2.db"
    conn = sqlite3.connect(bad_db)
    with conn:
        conn.execute("CREATE TABLE daily_weather (date TEXT, temp_c REAL)")
        conn.execute("INSERT INTO daily_weather VALUES ('2025-09-01', 18.0)")
    with pytest.raises(PipelineError, match="missing required columns"):
        DataPipeline(config).load_weather(str(bad_db))


# ------------------------------------------------------------------------ #
# Peak / off-peak analysis
# ------------------------------------------------------------------------ #
def test_peak_offpeak_means(config: PipelineConfig, dummy_ridership: pd.DataFrame) -> None:
    pipeline = DataPipeline(config)
    cleaned = pipeline.clean_ridership(dummy_ridership)
    peak_avg, offpeak_avg = analyze_peak_offpeak(cleaned, PEAK_HOURS)
    # peak: {8: 120, 18: 95, 7: 140, 9: 80, 19: 110} -> mean = 109.0
    # off-peak: {12: 60} -> mean = 60.0
    assert peak_avg == pytest.approx(109.0)
    assert offpeak_avg == pytest.approx(60.0)


def analyze_peak_offpeak(df: pd.DataFrame, peak_hours: frozenset[int]) -> tuple[float, float]:
    from analysis import avg_passengers_per_hour

    return avg_passengers_per_hour(df, peak_hours)


# ------------------------------------------------------------------------ #
# Correlation + summary
# ------------------------------------------------------------------------ #
def test_correlation_perfect_negative() -> None:
    dates = pd.date_range("2025-09-01", periods=6, freq="D")
    df = pd.DataFrame(
        {
            "date": dates,
            "hour": 8,
            "day_of_week": [0, 1, 2, 3, 4, 5],
            "passenger_count": [120, 100, 80, 60, 40, 20],
            "precipitation_mm": [0.0, 2.0, 4.0, 6.0, 8.0, 10.0],
        }
    )
    result: AnalysisResult = analyze(df, PEAK_HOURS)
    assert result.correlation == pytest.approx(-1.0)
    s = result.summary
    assert s["highest_ridership_day"] == {"date": "2025-09-01", "total_passengers": 120}
    assert s["average_daily_ridership"] == pytest.approx(70.0)
    assert s["correlation_precipitation_vs_ridership"] == pytest.approx(-1.0)
    assert s["total_days_analysed"] == 6


def test_correlation_nan_without_weather() -> None:
    df = pd.DataFrame(
        {
            "date": pd.date_range("2025-09-01", periods=3, freq="D"),
            "hour": 8,
            "day_of_week": [0, 1, 2],
            "passenger_count": [100, 80, 60],
            "precipitation_mm": [float("nan")] * 3,
        }
    )
    result = analyze(df, PEAK_HOURS)
    assert result.correlation is None or pd.isna(result.correlation)


# ------------------------------------------------------------------------ #
# End-to-end process (file-based weather DB, processed DB write)
# ------------------------------------------------------------------------ #
def test_process_end_to_end(tmp_path: Path, dummy_ridership: pd.DataFrame) -> None:
    csv_path = tmp_path / "ridership.csv"
    dummy_ridership.to_csv(csv_path, index=False)
    weather_path = tmp_path / "weather.db"
    conn = sqlite3.connect(weather_path)
    with conn:
        conn.execute(
            "CREATE TABLE daily_weather (date TEXT, temp_c REAL, precipitation_mm REAL)"
        )
        conn.executemany(
            "INSERT INTO daily_weather VALUES (?, ?, ?)",
            [
                ("2025-09-01", 18.0, 0.0),
                ("2025-09-02", 15.5, 2.5),
                ("2025-09-03", 12.0, 8.0),
            ],
        )
    cfg = PipelineConfig(
        input_csv=str(csv_path),
        weather_db=str(weather_path),
        output_dir=str(tmp_path / "output"),
        processed_db=str(tmp_path / "processed.db"),
    )
    merged = DataPipeline(cfg).process()
    assert len(merged) == 6
    processed = pd.read_sql(
        "SELECT * FROM processed_ridership",
        create_engine(f"sqlite:///{cfg.processed_db}"),
    )
    assert len(processed) == 6
    assert set(processed.columns) >= {"trip_id", "station_id", "passenger_count", "temp_c", "precipitation_mm"}
