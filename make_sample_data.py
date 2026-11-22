"""Generate synthetic demo inputs (ridership.csv + weather.db).

Usage:
    python make_sample_data.py [--days 45] [--seed 42] [--out-dir .]

The synthetic weather is correlated with ridership (heavy rain reduces
boardings), so the precipitation/ridership correlation in the summary is
meaningful. A small percentage of rows intentionally has a missing
passenger_count or ticket_type to exercise the cleaning logic.
"""

from __future__ import annotations

import argparse
import random
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Sequence


def _precipitation(rng: random.Random) -> float:
    return rng.choices(
        [0.0, 0.0, 0.0, 0.5, 2.5, 6.0, 14.0],
        weights=[45, 25, 15, 5, 6, 3, 1],
    )[0]


def _daily_weather(rows: list[tuple[date, float, float]], days: int, rng: random.Random) -> None:
    start = date(2025, 8, 1)
    for offset in range(days):
        d = start + timedelta(days=offset)
        precip = _precipitation(rng)
        temp = round(rng.uniform(10.0 + (d.month - 7) * 4.0, 24.0), 1)
        rows.append((d, temp, precip))


def write_weather_db(db_path: Path, rows: list[tuple[date, float, float]]) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE daily_weather (date TEXT, temp_c REAL, precipitation_mm REAL)"
    )
    cur.executemany(
        "INSERT INTO daily_weather (date, temp_c, precipitation_mm) VALUES (?, ?, ?)",
        [(d.isoformat(), t, p) for d, t, p in rows],
    )
    conn.commit()
    conn.close()


HEAVY_HOURS = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
HEAVY_WEIGHTS = [1, 6, 7, 6, 3, 2, 3, 2, 2, 2, 1, 6, 7, 6, 2]


def build_ridership_csv(
    csv_path: Path,
    weather_rows: Sequence[tuple[date, float, float]],
    rng: random.Random,
) -> int:
    stations = ["S1", "S2", "S3", "S4"]
    ticket_types = ["Single", "Monthly", "Weekly"]
    lines = ["trip_id,station_id,timestamp,ticket_type,passenger_count"]
    trip_id = 0
    for d, _temp, precip in weather_rows:
        weekend = d.weekday() >= 5
        rain_factor = max(0.25, 1.0 - precip * 0.012)
        per_station = rng.randint(10, 16) if not weekend else rng.randint(6, 12)
        for station in stations:
            for _ in range(rng.randint(4, per_station)):
                hour = rng.choices(HEAVY_HOURS, weights=HEAVY_WEIGHTS)[0]
                ts = datetime(d.year, d.month, d.day, hour, rng.randint(0, 59), rng.randint(0, 59))
                pax = max(1, int(rng.randint(4, 140) * rain_factor))
                # intentionally inject some messy data to exercise the cleaning logic
                passenger_field = "" if rng.random() < 0.02 else str(pax)
                ticket = "" if rng.random() < 0.03 else rng.choice(ticket_types)
                trip_id += 1
                lines.append(
                    f"{trip_id},{station},"
                    f"{ts.strftime('%Y-%m-%d %H:%M:%S')},"
                    f"{ticket},{passenger_field}"
                )
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return trip_id


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Generate demo ridership.csv + weather.db."
    )
    parser.add_argument("--days", type=int, default=45)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", type=Path, default=Path("."))
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    weather_rows: list[tuple[date, float, float]] = []
    _daily_weather(weather_rows, args.days, rng)
    write_weather_db(args.out_dir / "weather.db", weather_rows)
    n = build_ridership_csv(
        args.out_dir / "ridership.csv", weather_rows, rng
    )
    print(
        f"Wrote {args.out_dir / 'ridership.csv'} with {n} trip rows "
        f"and {args.out_dir / 'weather.db'} with {len(weather_rows)} weather days."
    )


if __name__ == "__main__":
    main()
