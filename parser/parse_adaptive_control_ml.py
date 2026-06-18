#!/usr/bin/env python3

import re
from pathlib import Path

import pandas as pd


LOG_DIR = Path("../logs")
OUTPUT_DIR = Path("../data/article_4/parsed")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

METRIC_PATTERN = re.compile(r"(\w+)=([^\s]+)")

RATES = {
    "constant",
    "burst",
    "wave"
}


def parse_filename(log_file: Path) -> dict | None:
    """
    Supported formats:

    realtime_constant_straight_1000_500_20260603_203825.log

    skewed_constant_straight_1000_3000_20260603_212658.log

    out_of_order_constant_straight_1000_3000_20260604_211629.log

    late_burst_random_walk_5000_5000_20260604_001620.log
    """

    name = log_file.stem
    parts = name.split("_")

    # Need at least:
    # profile + rate + motion + window + watermark + date + time
    if len(parts) < 7:
        return None

    try:
        date_part = parts[-2]
        time_part = parts[-1]

        watermark_delay_ms = int(parts[-3])
        window_size_ms = int(parts[-4])

    except ValueError:
        return None

    experiment_parts = parts[:-4]
    experiment = "_".join(experiment_parts)

    profile = None
    rate_pattern = None
    motion_mode = None

    for rate in RATES:

        marker = f"_{rate}_"

        if marker in experiment:

            left, right = experiment.split(marker, 1)

            profile = left
            rate_pattern = rate
            motion_mode = right

            break

    if (
        profile is None
        or rate_pattern is None
        or motion_mode is None
    ):
        return None

    return {
        "profile": profile,
        "rate_pattern": rate_pattern,
        "motion_mode": motion_mode,
        "window_size_ms": window_size_ms,
        "watermark_delay_ms": watermark_delay_ms,
        "run_timestamp": f"{date_part}_{time_part}"
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

    if metadata is None:
        return pd.DataFrame()

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

    processed_files = 0
    total_rows = 0

    for log_file in log_files:

        metadata = parse_filename(log_file)

        if metadata is None:
            print(f"Skipping {log_file.name}")
            continue

        print(f"Parsing {log_file.name}")

        df = parse_log(log_file)

        if df.empty:
            continue

        output_file = OUTPUT_DIR / f"{log_file.stem}.csv"

        df.to_csv(
            output_file,
            index=False
        )

        processed_files += 1
        total_rows += len(df)

        print(
            f"Saved {len(df)} rows -> {output_file.name}"
        )

    print()
    print("====================================")
    print("Parsing completed")
    print(f"Processed files : {processed_files}")
    print(f"Total rows      : {total_rows}")
    print(f"Output folder   : {OUTPUT_DIR}")
    print("====================================")


if __name__ == "__main__":
    main()
