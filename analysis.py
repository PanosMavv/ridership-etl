"""Exploratory analysis metrics computed on the merged ridership + weather frame."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, FrozenSet

import pandas as pd

logger = logging.getLogger("ridership_etl.analysis")


def _rounded(value: float) -> float | None:
    """Round a float, or return None when it is NaN (so JSON emits null)."""
    if pd.isna(value):
        return None
    return round(value, 2)


@dataclass(frozen=True)
class AnalysisResult:
    """All derived metrics plus a ready-to-dump JSON summary."""

    daily_total: pd.DataFrame
    peak_avg: float
    offpeak_avg: float
    correlation: float
    highest_day_date: str
    highest_day_passengers: int
    average_daily: float
    total_days: int

    @property
    def summary(self) -> dict[str, Any]:
        return {
            "highest_ridership_day": {
                "date": self.highest_day_date,
                "total_passengers": self.highest_day_passengers,
            },
            "average_daily_ridership": round(self.average_daily, 2),
            "correlation_precipitation_vs_ridership": _rounded(self.correlation),
            "avg_passengers_per_hour": {
                "peak": _rounded(self.peak_avg),
                "off_peak": _rounded(self.offpeak_avg),
            },
            "total_days_analysed": self.total_days,
        }


def daily_total_passengers(df: pd.DataFrame) -> pd.DataFrame:
    """Sum passenger_count per calendar day.

    Returns a frame with columns ``date`` and ``total_passengers``.
    """
    return (
        df.groupby("date")["passenger_count"]
        .sum()
        .rename("total_passengers")
        .reset_index()
    )


def avg_passengers_per_hour(
    df: pd.DataFrame, peak_hours: FrozenSet[int]
) -> tuple[float, float]:
    """Mean of the per-hour passenger means, split into peak vs off-peak.

    Returns ``(peak_avg, off_peak_avg)``. An empty slice yields ``nan``.
    """

    def _hourly_mean(mask: pd.Series) -> float:
        sub = df.loc[mask]
        if sub.empty:
            return float("nan")
        return float(sub.groupby("hour")["passenger_count"].mean().mean())

    is_peak = df["hour"].isin(peak_hours)
    return _hourly_mean(is_peak), _hourly_mean(~is_peak)


def precipitation_correlation(
    daily_total: pd.DataFrame, weather: pd.DataFrame
) -> float:
    """Pearson correlation between daily precipitation and daily total passengers."""
    merged = daily_total.merge(
        weather[["date", "precipitation_mm"]], on="date", how="inner"
    )
    valid = merged.dropna(subset=["precipitation_mm"]).drop_duplicates(subset="date")
    if len(valid) < 2:
        return float("nan")
    return float(valid["precipitation_mm"].corr(valid["total_passengers"]))


def analyze(df: pd.DataFrame, peak_hours: FrozenSet[int]) -> AnalysisResult:
    """Compute all analysis metrics from the merged ridership + weather frame."""
    if df.empty:
        raise ValueError("Nothing to analyse: the merged frame is empty")
    daily = daily_total_passengers(df)
    peak_avg, offpeak_avg = avg_passengers_per_hour(df, peak_hours)
    correlation = precipitation_correlation(daily, df)
    top_row = daily.loc[daily["total_passengers"].idxmax()]
    return AnalysisResult(
        daily_total=daily,
        peak_avg=peak_avg,
        offpeak_avg=offpeak_avg,
        correlation=correlation,
        highest_day_date=top_row["date"].strftime("%Y-%m-%d"),
        highest_day_passengers=int(top_row["total_passengers"]),
        average_daily=float(daily["total_passengers"].mean()),
        total_days=int(len(daily)),
    )
