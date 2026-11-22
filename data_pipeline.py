"""Extraction, cleaning, merge, and load steps for the ridership pipeline."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import SQLAlchemyError

from config import PipelineConfig, REQUIRED_CSV_COLUMNS, REQUIRED_WEATHER_COLUMNS

logger = logging.getLogger("ridership_etl.pipeline")


class PipelineError(Exception):
    """Raised when an input, data-quality, or output operation fails."""


class DataPipeline:
    """Modular, resilient ETL pipeline for ridership + weather data.

    Steps:
      1. Extract  -- raw ridership CSV + daily weather table
      2. Transform-- drop bad rows, fill missing ticket types, parse timestamps, merge
      3. Load     -- write the merged frame to ``processed_ridership`` in SQLite
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    # ------------------------------------------------------------------ #
    # Extract
    # ------------------------------------------------------------------ #
    def load_ridership(self, csv_path: str | Path | None = None) -> pd.DataFrame:
        """Load the raw ridership CSV and validate its required columns."""
        path = Path(csv_path) if csv_path is not None else Path(self.config.input_csv)
        if not path.is_file():
            raise PipelineError(f"Input CSV not found: {path}")
        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError as exc:
            raise PipelineError(f"Input CSV is empty: {path}") from exc
        missing = REQUIRED_CSV_COLUMNS - set(df.columns)
        if missing:
            raise PipelineError(
                f"Input CSV '{path}' is missing required columns: {sorted(missing)}"
            )
        if df.empty:
            raise PipelineError(f"Input CSV contains no data rows: {path}")
        logger.info("Loaded %d ridership rows from %s", len(df), path.name)
        return df

    @staticmethod
    def _weather_engine(source: str | Engine | sqlite3.Connection) -> Engine | sqlite3.Connection:
        """Return an engine/connection usable by pandas.read_sql."""
        if isinstance(source, str):
            db_path = Path(source)
            if not db_path.is_file():
                raise PipelineError(f"Weather database not found: {db_path}")
            return create_engine(f"sqlite:///{db_path}")
        return source  # type: ignore[return-value]

    def load_weather(self, source: str | Engine | sqlite3.Connection) -> pd.DataFrame:
        """Load ``daily_weather`` from a file path or an already-open engine/connection."""
        engine = self._weather_engine(source)
        try:
            df = pd.read_sql("SELECT * FROM daily_weather", engine)
        except (SQLAlchemyError, pd.errors.DatabaseError) as exc:
            raise PipelineError(f"Failed to query daily_weather from {source}: {exc}") from exc
        missing = REQUIRED_WEATHER_COLUMNS - set(df.columns)
        if missing:
            raise PipelineError(
                f"daily_weather is missing required columns: {sorted(missing)}"
            )
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        dropped = int(df["date"].isna().sum())
        if dropped:
            logger.warning("Dropped %d weather rows with unparseable dates", dropped)
        df = df.dropna(subset=["date"])
        if df.empty:
            raise PipelineError("daily_weather contains no rows with a valid date")
        logger.info("Loaded %d daily weather rows", len(df))
        return df

    # ------------------------------------------------------------------ #
    # Transform
    # ------------------------------------------------------------------ #
    def clean_ridership(self, ridership: pd.DataFrame) -> pd.DataFrame:
        """Drop bad rows, fill missing ticket types, parse and decompose timestamps."""
        out = ridership.copy()
        logger.info("Starting cleaning with %d raw rows", len(out))

        # passenger_count must be numeric
        out["passenger_count"] = pd.to_numeric(out["passenger_count"], errors="coerce")
        dropped = int(out["passenger_count"].isna().sum())
        if dropped:
            logger.info("Dropped %d rows with missing / non-numeric passenger_count", dropped)
        out = out.dropna(subset=["passenger_count"])

        # ticket_type missing -> "Unknown"
        out["ticket_type"] = (
            out["ticket_type"].fillna("Unknown")
            .astype("string")
            .replace("", "Unknown")
            .fillna("Unknown")
        )
        logger.info("Missing ticket_type values replaced with 'Unknown'")

        # timestamp -> datetime
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
        dropped = int(out["timestamp"].isna().sum())
        if dropped:
            logger.info("Dropped %d rows with unparseable timestamps", dropped)
        out = out.dropna(subset=["timestamp"])
        if out.empty:
            raise PipelineError("No valid ridership rows remain after cleaning")

        # decompose the timestamp
        out["date"] = out["timestamp"].dt.normalize()
        out["hour"] = out["timestamp"].dt.hour
        out["day_of_week"] = out["timestamp"].dt.day_of_week  # 0 = Monday
        logger.info("Cleaning complete: %d valid rows", len(out))
        return out

    def merge(self, ridership: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
        """Left-merge the cleaned ridership rows with daily weather on ``date``."""
        if ridership.empty or weather.empty:
            raise PipelineError("Cannot merge empty inputs")
        merged = ridership.merge(
            weather[["date", "temp_c", "precipitation_mm"]],
            on="date",
            how="left",
        )
        logger.info(
            "Merged %d ridership rows with %d weather days (left join on date)",
            len(ridership),
            len(weather),
        )
        return merged

    # ------------------------------------------------------------------ #
    # Load
    # ------------------------------------------------------------------ #
    def save_to_sql(
        self, df: pd.DataFrame, table: str = "processed_ridership"
    ) -> None:
        """Write the merged frame to the ``processed_ridership`` table."""
        engine = create_engine(f"sqlite:///{self.config.processed_db}")
        try:
            df.to_sql(table, engine, if_exists="replace", index=False)
        except SQLAlchemyError as exc:
            raise PipelineError(
                f"Failed to write table {table} to {self.config.processed_db}"
            ) from exc
        logger.info(
            "Saved %d rows to %s.%s", len(df), self.config.processed_db, table
        )

    # ------------------------------------------------------------------ #
    # Orchestration
    # ------------------------------------------------------------------ #
    def process(self) -> pd.DataFrame:
        """Run extract -> transform -> load and return the merged frame."""
        ridership = self.load_ridership()
        weather = self.load_weather(self.config.weather_db)
        cleaned = self.clean_ridership(ridership)
        merged = self.merge(cleaned, weather)
        self.save_to_sql(merged)
        return merged
