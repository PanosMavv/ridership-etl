"""Configuration loading for the ridership ETL pipeline.

Values may be set in a project-level ``.env`` file (via python-dotenv) or
overridden from the command line (see main.py).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import FrozenSet

from dotenv import load_dotenv

REQUIRED_CSV_COLUMNS: FrozenSet[str] = frozenset(
    {"trip_id", "station_id", "timestamp", "ticket_type", "passenger_count"}
)
REQUIRED_WEATHER_COLUMNS: FrozenSet[str] = frozenset({"date", "temp_c", "precipitation_mm"})

# Morning and evening rush hours used for the peak / off-peak split.
DEFAULT_PEAK_HOURS: FrozenSet[int] = frozenset({7, 8, 9, 10, 16, 17, 18, 19})


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration for a single pipeline run."""

    input_csv: str
    weather_db: str
    output_dir: str
    processed_db: str
    log_file: str = "etl_pipeline.log"
    peak_hours: FrozenSet[int] = field(default_factory=lambda: DEFAULT_PEAK_HOURS)

    def __post_init__(self) -> None:
        for attr in ("input_csv", "weather_db", "output_dir", "processed_db", "log_file"):
            value = getattr(self, attr)
            object.__setattr__(self, attr, str(Path(value).expanduser().resolve()))

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Build a config from the .env file (falling back to sensible defaults)."""
        load_dotenv()
        return cls(
            input_csv=os.environ.get("RIDERSHIP_CSV", "ridership.csv"),
            weather_db=os.environ.get("WEATHER_DB", "weather.db"),
            output_dir=os.environ.get("OUTPUT_DIR", "output"),
            processed_db=os.environ.get("PROCESSED_DB", "processed_ridership.db"),
            log_file=os.environ.get("LOG_FILE", "etl_pipeline.log"),
        )
