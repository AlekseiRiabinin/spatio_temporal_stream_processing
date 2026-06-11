#!/usr/bin/env python3

"""
build_adaptive_dataset.py

Reproducible dataset builder for Article 04.

Key change:
- Separates RAW log dataset (for research)
- from ML dataset (strict schema for training/ONNX)
"""

from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split


# ============================================================
# PATHS
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

PARSED_DIR = ROOT / "data" / "article_4" / "parsed"

DATASET_DIR = ROOT / "data" / "article_4" / "datasets"
TRAIN_DIR = ROOT / "data" / "article_4" / "training"
VALIDATION_DIR = ROOT / "data" / "article_4" / "validation"
TEST_DIR = ROOT / "data" / "article_4" / "test"

DATASET_DIR.mkdir(parents=True, exist_ok=True)
TRAIN_DIR.mkdir(parents=True, exist_ok=True)
VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
TEST_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# STRICT ML FEATURE SCHEMA (CRITICAL FOR REPRODUCIBILITY)
# ============================================================

ML_FEATURES = [
    "event_rate",
    "disorder_ratio",
    "late_event_ratio",
    "average_latency_ms",
    "window_fill_ratio",
    "interaction_rate",
    "collision_rate",
    "proximity_rate",
    "swarm_rate",
    "conflict_rate",
    "watermark_lag_ms",
    "processing_latency_ms",
    "adaptive_window_size_ms",
    "adaptive_watermark_delay_ms",
    "timestamp"
]


# ============================================================
# LOAD PARSED FILES
# ============================================================

def load_parsed_files() -> pd.DataFrame:

    files = sorted(PARSED_DIR.glob("*.csv"))

    if not files:
        raise RuntimeError(f"No parsed CSV files found in {PARSED_DIR}")

    frames = []

    for file in files:
        print(f"[LOAD] {file.name}")
        df = pd.read_csv(file)

        if not df.empty:
            frames.append(df)

    if not frames:
        raise RuntimeError("Parsed files exist but contain no data.")

    return pd.concat(frames, ignore_index=True)


# ============================================================
# CLEANING (SAFE FOR MULTI-EXPERIMENT DATA)
# ============================================================

def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:

    print(f"[CLEAN] initial rows = {len(df)}")

    df = df.copy()

    df = df.drop_duplicates()

    # IMPORTANT:
    # only enforce ML-critical columns (avoid deleting new metric rows)
    df = df.dropna(subset=[c for c in ML_FEATURES if c in df.columns])

    df = df[df["event_rate"] >= 0]
    df = df[df["window_size_ms"] > 0]
    df = df[df["watermark_delay_ms"] > 0]

    print(f"[CLEAN] final rows = {len(df)}")

    return df


# ============================================================
# SPLIT (DETERMINISTIC)
# ============================================================

def split_dataset(df: pd.DataFrame):

    train_df, temp_df = train_test_split(
        df,
        test_size=0.30,
        random_state=42,
        shuffle=True
    )

    validation_df, test_df = train_test_split(
        temp_df,
        test_size=0.50,
        random_state=42,
        shuffle=True
    )

    return train_df, validation_df, test_df


# ============================================================
# MAIN
# ============================================================

def main():

    print("====================================")
    print("BUILD ADAPTIVE CONTROL DATASET")
    print("====================================")

    df = load_parsed_files()

    df = clean_dataset(df)

    # ========================================================
    # 1. RAW DATASET (FULL LOG EXPORT - FOR RESEARCH ONLY)
    # ========================================================

    raw_dataset_file = DATASET_DIR / "adaptive_control_dataset_full.csv"
    df.to_csv(raw_dataset_file, index=False)

    print(f"[SAVE] full dataset -> {raw_dataset_file}")

    # ========================================================
    # 2. STRICT ML DATASET (FOR TRAINING / ONNX / REPRODUCIBILITY)
    # ========================================================

    missing = [c for c in ML_FEATURES if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing ML features: {missing}")

    ml_df = df[ML_FEATURES].copy()

    ml_dataset_file = DATASET_DIR / "adaptive_control_dataset_ml.csv"
    ml_df.to_csv(ml_dataset_file, index=False)

    print(f"[SAVE] ml dataset -> {ml_dataset_file}")

    # ========================================================
    # SPLIT (ONLY ON ML DATASET)
    # ========================================================

    train_df, validation_df, test_df = split_dataset(ml_df)

    train_file = TRAIN_DIR / "train.csv"
    validation_file = VALIDATION_DIR / "validation.csv"
    test_file = TEST_DIR / "test.csv"

    train_df.to_csv(train_file, index=False)
    validation_df.to_csv(validation_file, index=False)
    test_df.to_csv(test_file, index=False)

    # ========================================================
    # STATS
    # ========================================================

    print()
    print("Dataset statistics")
    print("------------------")
    print(f"Total rows      : {len(df)}")
    print(f"Training rows   : {len(train_df)}")
    print(f"Validation rows : {len(validation_df)}")
    print(f"Test rows       : {len(test_df)}")

    print()
    print("Output files")
    print("------------")
    print(train_file)
    print(validation_file)
    print(test_file)
    print(ml_dataset_file)
    print(raw_dataset_file)


if __name__ == "__main__":
    main()
