#!/usr/bin/env python3

import re
from pathlib import Path

import pandas as pd


LOG_DIR = Path("../logs")
OUTPUT_DIR = Path("../data/article_4/parsed")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


METRIC_PATTERN = re.compile(r"(\w+)=([^\s]+)")


def parse_filename(log_file: Path) -> dict:
    """
    Example:

    skewed_wave_corridor_fixedWindow_fixedWatermark_20260604_153000.log
    """

    parts = log_file.stem.split("_")

    return {
        "profile": parts[0],
        "rate_pattern": parts[1],
        "motion_mode": parts[2],
        "window_strategy": parts[3],
        "watermark_strategy": parts[4],
        "run_timestamp": "_".join(parts[-2:])
    }


def parse_metric_line(line: str) -> dict:
    matches = METRIC_PATTERN.findall(line)

    row = {}

    for key, value in matches:

        try:
            if "." in value:
                row[key] = float(value)
            else:
                row[key] = int(value)

        except ValueError:
            row[key] = value

    return row


def parse_log(log_file: Path) -> pd.DataFrame:

    metadata = parse_filename(log_file)

    rows = []

    with open(log_file, "r", encoding="utf-8") as f:

        for line in f:

            if "[METRIC]" not in line:
                continue

            row = metadata.copy()
            row.update(parse_metric_line(line))

            rows.append(row)

    return pd.DataFrame(rows)


def main():

    log_files = sorted(LOG_DIR.glob("*.log"))

    print(f"Found {len(log_files)} log files")

    for log_file in log_files:

        print(f"Parsing {log_file.name}")

        df = parse_log(log_file)

        output_file = OUTPUT_DIR / f"{log_file.stem}.csv"

        df.to_csv(output_file, index=False)

        print(
            f"Saved {len(df)} rows -> {output_file}"
        )


if __name__ == "__main__":
    main()
