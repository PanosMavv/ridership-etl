"""Chart generation for the pipeline results (headless / Agg backend)."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

logger = logging.getLogger("ridership_etl.visualizer")

DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _save(fig: plt.Figure, path: Path) -> None:
    """Save the figure, close it, and log the write."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved chart: %s", path)


def plot_total_passengers_per_day(daily_total: pd.DataFrame, path: Path) -> Path:
    """Line chart of total passengers per day over the observation period."""
    if daily_total.empty:
        raise ValueError("daily_total is empty; cannot plot")
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(
        daily_total["date"],
        daily_total["total_passengers"],
        marker="o",
        markersize=2.5,
        linewidth=1.4,
        label="Total passengers",
    )
    ax.set_title("Total Passengers per Day")
    ax.set_xlabel("Date")
    ax.set_ylabel("Total passengers")
    ax.legend(loc="best")
    ax.grid(alpha=0.3, which="major")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    fig.autofmt_xdate()
    fig.tight_layout()
    _save(fig, path)
    return path


def plot_heatmap(df: pd.DataFrame, path: Path) -> Path:
    """Heatmap of total passengers by hour of day (x) and day of week (y)."""
    if df.empty:
        raise ValueError("df is empty; cannot plot")
    pivot = (
        df.pivot_table(
            index="day_of_week",
            columns="hour",
            values="passenger_count",
            aggfunc="sum",
        )
        .reindex(columns=range(24), fill_value=0)
        .reindex(index=range(7), fill_value=0)
    )
    pivot = pivot.fillna(0).astype(int)
    pivot.index = DAY_LABELS
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.heatmap(
        pivot,
        ax=ax,
        annot=True,
        fmt="d",
        cmap="YlGnBu",
        cbar_kws={"label": "Total passengers"},
    )
    ax.set_title("Passenger Volume by Hour and Day of Week")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Day of week")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0, fontsize=8)
    fig.tight_layout()
    _save(fig, path)
    return path
