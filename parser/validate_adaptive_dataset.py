#!/usr/bin/env python3

"""
validate_adaptive_dataset.py

Validates adaptive-control ML dataset.

Usage:
    python validate_adaptive_dataset.py \
        data/article_4/datasets/adaptive_dataset.csv
"""

from pathlib import Path
import sys

import pandas as pd


# ============================================================
# Expected schema
# ============================================================

REQUIRED_COLUMNS = [

    # experiment metadata
    "experiment_id",
    "profile",
    "rate",
    "motion",

    # stream features
    "event_rate",
    "disorder_ratio",
    "late_event_ratio",
    "avg_latency_ms",
    "window_fill_ratio",

    # interaction features
    "interaction_rate",
    "collision_rate",
    "proximity_rate",
    "swarm_rate",
    "conflict_rate",

    # temporal features
    "watermark_lag_ms",
    "processing_latency_ms",

    # targets
    "window_size_ms",
    "watermark_delay_ms"
]


# ============================================================
# Helpers
# ============================================================

def validate_columns(df: pd.DataFrame):

    missing = [
        c for c in REQUIRED_COLUMNS
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing columns: {missing}"
        )

    print("[OK] Required columns present")


def validate_missing_values(df: pd.DataFrame):

    missing = df.isna().sum().sum()

    if missing > 0:
        print(
            f"[WARNING] Dataset contains "
            f"{missing} missing values"
        )
    else:
        print("[OK] No missing values")


def validate_duplicates(df: pd.DataFrame):

    duplicates = df.duplicated().sum()

    if duplicates > 0:
        print(
            f"[WARNING] Dataset contains "
            f"{duplicates} duplicated rows"
        )
    else:
        print("[OK] No duplicates")


def validate_profiles(df):

    expected = {
        "realtime",
        "skewed",
        "late",
        "out_of_order",
        "mixed"
    }

    present = set(df["profile"].unique())

    print(
        f"[INFO] Profiles: {sorted(present)}"
    )

    missing = expected - present

    if missing:
        print(
            f"[WARNING] Missing profiles: "
            f"{sorted(missing)}"
        )
    else:
        print("[OK] All profiles present")


def validate_rates(df):

    expected = {
        "constant",
        "burst",
        "wave"
    }

    present = set(df["rate"].unique())

    print(
        f"[INFO] Rates: {sorted(present)}"
    )

    missing = expected - present

    if missing:
        print(
            f"[WARNING] Missing rates: "
            f"{sorted(missing)}"
        )
    else:
        print("[OK] All rate patterns present")


def validate_motion_modes(df):

    expected = {
        "straight",
        "random_walk",
        "swarm",
        "collision",
        "corridor"
    }

    present = set(df["motion"].unique())

    print(
        f"[INFO] Motion modes: "
        f"{sorted(present)}"
    )

    missing = expected - present

    if missing:
        print(
            f"[WARNING] Missing motion modes: "
            f"{sorted(missing)}"
        )
    else:
        print("[OK] All motion modes present")


def validate_targets(df):

    window_unique = (
        df["window_size_ms"]
        .dropna()
        .nunique()
    )

    watermark_unique = (
        df["watermark_delay_ms"]
        .dropna()
        .nunique()
    )

    print(
        f"[INFO] Unique windows: "
        f"{window_unique}"
    )

    print(
        f"[INFO] Unique watermarks: "
        f"{watermark_unique}"
    )

    if window_unique < 2:
        print(
            "[WARNING] Window target "
            "has insufficient diversity"
        )

    if watermark_unique < 2:
        print(
            "[WARNING] Watermark target "
            "has insufficient diversity"
        )


def validate_numeric_ranges(df):

    checks = {

        "event_rate": (0, None),

        "disorder_ratio": (0, 1),

        "late_event_ratio": (0, 1),

        "collision_rate": (0, 1),

        "proximity_rate": (0, 1),

        "swarm_rate": (0, 1),

        "conflict_rate": (0, 1)
    }

    for col, (low, high) in checks.items():

        if col not in df.columns:
            continue

        series = df[col]

        if low is not None:
            if (series < low).any():
                print(
                    f"[WARNING] {col} "
                    f"contains values < {low}"
                )

        if high is not None:
            if (series > high).any():
                print(
                    f"[WARNING] {col} "
                    f"contains values > {high}"
                )

    print("[OK] Numeric range validation completed")


def print_summary(df):

    print()
    print("=" * 60)
    print("DATASET SUMMARY")
    print("=" * 60)

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print()

    print(
        df.describe(
            include="all"
        ).transpose()
    )


# ============================================================
# Main
# ============================================================

def main():

    if len(sys.argv) != 2:

        print(
            "Usage:\n"
            "python validate_adaptive_dataset.py "
            "<dataset.csv>"
        )
        sys.exit(1)

    dataset_path = Path(sys.argv[1])

    if not dataset_path.exists():
        raise FileNotFoundError(dataset_path)

    print(
        f"[INFO] Loading dataset:\n"
        f"{dataset_path}"
    )

    df = pd.read_csv(dataset_path)

    validate_columns(df)

    validate_missing_values(df)

    validate_duplicates(df)

    validate_profiles(df)

    validate_rates(df)

    validate_motion_modes(df)

    validate_targets(df)

    validate_numeric_ranges(df)

    print_summary(df)

    print()
    print("[SUCCESS] Dataset validation completed")


if __name__ == "__main__":
    main()
