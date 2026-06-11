#!/usr/bin/env python3

"""
validate_adaptive_dataset.py

Article 4 - Reproducible dataset validation.

Supports:
- ML dataset validation (strict)
- Full dataset validation (research logs)
"""

from pathlib import Path
import sys
import pandas as pd


# ============================================================
# SCHEMA DEFINITIONS
# ============================================================

ML_COLUMNS = [

    # stream features
    "event_rate",
    "disorder_ratio",
    "late_event_ratio",
    "average_latency_ms",
    "window_fill_ratio",

    # interaction features
    "interaction_rate",
    "collision_rate",
    "proximity_rate",
    "swarm_rate",
    "conflict_rate",

    # temporal
    "watermark_lag_ms",
    "processing_latency_ms",

    # targets
    "window_size_ms",
    "watermark_delay_ms",
    "timestamp"
]


FULL_EXTRA_COLUMNS = [

    # Article 4 extended controller observability (NEW)
    "window_oscillation",
    "watermark_oscillation",
    "window_oscillation_rate",
    "watermark_oscillation_rate",
    "min_window_ms",
    "max_window_ms",
    "min_watermark_ms",
    "max_watermark_ms",
    "avg_adaptation_interval_ms",
]


# ============================================================
# VALIDATION
# ============================================================

def validate_columns(df: pd.DataFrame, mode: str):

    if mode == "ml":
        required = ML_COLUMNS
    else:
        required = ML_COLUMNS + FULL_EXTRA_COLUMNS

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(f"[ERROR] Missing columns: {missing}")

    print("[OK] Schema validation passed")


def validate_missing_values(df: pd.DataFrame):

    missing = df.isna().sum().sum()

    if missing > 0:
        print(f"[WARNING] Missing values: {missing}")
    else:
        print("[OK] No missing values")


def validate_duplicates(df: pd.DataFrame):

    dup = df.duplicated().sum()

    if dup > 0:
        print(f"[WARNING] Duplicates: {dup}")
    else:
        print("[OK] No duplicates")


def validate_profiles(df):

    expected = {"realtime", "skewed", "late", "out_of_order", "mixed"}

    present = set(df["profile"].unique())

    print(f"[INFO] Profiles: {sorted(present)}")

    missing = expected - present
    if missing:
        print(f"[WARNING] Missing profiles: {sorted(missing)}")
    else:
        print("[OK] Profiles complete")


def validate_rates(df):

    expected = {"constant", "burst", "wave"}

    present = set(df["rate_pattern"].unique())

    print(f"[INFO] Rates: {sorted(present)}")

    missing = expected - present
    if missing:
        print(f"[WARNING] Missing rates: {sorted(missing)}")
    else:
        print("[OK] Rates complete")


def validate_motion_modes(df):

    expected = {"straight", "random_walk", "swarm", "collision", "corridor"}

    present = set(df["motion_mode"].unique())

    print(f"[INFO] Motion modes: {sorted(present)}")

    missing = expected - present
    if missing:
        print(f"[WARNING] Missing motion modes: {sorted(missing)}")
    else:
        print("[OK] Motion modes complete")


def validate_targets(df):

    w = df["window_size_ms"].nunique()
    wm = df["watermark_delay_ms"].nunique()

    print(f"[INFO] Window diversity: {w}")
    print(f"[INFO] Watermark diversity: {wm}")

    if w < 2:
        print("[WARNING] Low window diversity")

    if wm < 2:
        print("[WARNING] Low watermark diversity")


def validate_numeric_ranges(df):

    checks = {
        "event_rate": (0, None),
        "disorder_ratio": (0, 1),
        "late_event_ratio": (0, 1),
        "collision_rate": (0, 1),
        "proximity_rate": (0, 1),
        "swarm_rate": (0, 1),
        "conflict_rate": (0, 1),
    }

    for col, (low, high) in checks.items():
        if col not in df.columns:
            continue

        s = df[col]

        if low is not None and (s < low).any():
            print(f"[WARNING] {col} < {low}")

        if high is not None and (s > high).any():
            print(f"[WARNING] {col} > {high}")

    print("[OK] Range checks done")


def print_summary(df):

    print("\n" + "=" * 60)
    print("DATASET SUMMARY")
    print("=" * 60)

    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}\n")

    print(df.describe(include="all").transpose())


# ============================================================
# MAIN
# ============================================================

def main():

    if len(sys.argv) != 2:
        print("Usage: python validate_adaptive_dataset.py <dataset.csv>")
        sys.exit(1)

    path = Path(sys.argv[1])

    df = pd.read_csv(path)

    # detect dataset mode
    mode = "ml" if "ml" in path.name else "full"

    print(f"[INFO] Mode detected: {mode}")

    validate_columns(df, mode)
    validate_missing_values(df)
    validate_duplicates(df)
    validate_profiles(df)
    validate_rates(df)
    validate_motion_modes(df)
    validate_targets(df)
    validate_numeric_ranges(df)

    print_summary(df)

    print("\n[SUCCESS] Validation complete")


if __name__ == "__main__":
    main()
