"""Command-line entry point for the ridership + weather ETL pipeline.

Run:
    python main.py --input-csv path/to/ridership.csv --output-dir path/to/output

Configuration can also be pre-seeded in a .env file (RIDERSHIP_CSV,
WEATHER_DB, OUTPUT_DIR, PROCESSED_DB, LOG_FILE). CLI flags override the
.env values.

Exit codes:
    0 -- pipeline completed successfully
    1 -- pipeline failed (clear error logged to the console and log file)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from analysis import analyze
from config import PipelineConfig
from data_pipeline import DataPipeline, PipelineError
from visualizer import plot_heatmap, plot_total_passengers_per_day

logger = logging.getLogger("ridership_etl")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest ridership (CSV) and weather (SQLite) data, clean and merge them, "
            "then output charts and a JSON summary."
        )
    )
    parser.add_argument(
        "--input-csv",
        default=None,
        help="Path to the raw ridership CSV file "
        "(defaults to RIDERSHIP_CSV in .env).",
    )
    parser.add_argument(
        "--weather-db",
        default=None,
        help="Path to the weather SQLite database "
        "(defaults to WEATHER_DB in .env).",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for charts and summary.json "
        "(defaults to OUTPUT_DIR in .env).",
    )
    parser.add_argument(
        "--processed-db",
        default=None,
        help="Path of the SQLite DB storing the processed data "
        "(defaults to PROCESSED_DB in .env).",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Log file path (defaults to LOG_FILE in .env).",
    )
    return parser


def setup_logging(log_file: str) -> None:
    """Attach INFO handlers to the console and the log file."""
    base = logging.getLogger("ridership_etl")
    if base.handlers:
        return
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    base.setLevel(logging.INFO)
    for handler in (
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ):
        handler.setFormatter(formatter)
        base.addHandler(handler)


def build_config(args: argparse.Namespace, log_file: str) -> PipelineConfig:
    env_cfg = PipelineConfig.from_env()
    return PipelineConfig(
        input_csv=str(Path(args.input_csv).resolve()) if args.input_csv else env_cfg.input_csv,
        weather_db=str(Path(args.weather_db).resolve()) if args.weather_db else env_cfg.weather_db,
        output_dir=str(Path(args.output_dir).resolve()) if args.output_dir else env_cfg.output_dir,
        processed_db=str(Path(args.processed_db).resolve()) if args.processed_db else env_cfg.processed_db,
        log_file=log_file,
        peak_hours=env_cfg.peak_hours,
    )


def run(args: argparse.Namespace, log_file: str) -> int:
    cfg = build_config(args, log_file)
    logger.info("Starting pipeline (config: %s)", vars(cfg))
    pipeline = DataPipeline(cfg)
    merged: pd.DataFrame = pipeline.process()

    output_dir = Path(cfg.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory ready: %s", output_dir)

    result = analyze(merged, cfg.peak_hours)
    chart_1 = plot_total_passengers_per_day(
        result.daily_total, output_dir / "total_passengers_per_day.png"
    )
    chart_2 = plot_heatmap(merged, output_dir / "passenger_volume_by_hour_and_dow.png")

    summary_path = output_dir / "summary.json"
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(result.summary, fh, indent=2)

    logger.info("Saved charts: %s | %s", chart_1, chart_2)
    logger.info("Saved summary to %s", summary_path)
    logger.info(
        "Pipeline complete: %d merged rows, %d distinct days, "
        "highest day %s=%d pax, average daily=%s pax, "
        "correlation precip_vs_ridership=%s",
        len(merged),
        result.total_days,
        result.highest_day_date,
        result.highest_day_passengers,
        result.average_daily,
        result.correlation,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    log_file = args.log_file or "etl_pipeline.log"
    try:
        setup_logging(log_file)
        return run(args, log_file)
    except PipelineError as exc:
        logger.error("Pipeline failed: %s", exc)
        return 1
    except Exception:
        logger.exception("Unexpected error; pipeline aborted")
        return 1


if __name__ == "__main__":
    sys.exit(main())
