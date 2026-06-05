#!/usr/bin/env python3

"""
build_adaptive_dataset.py

Builds a unified ML dataset from parsed Article 04 experiment files.

Input:
    data/article_4/parsed/*.csv

Output:
    data/article_4/datasets/adaptive_control_dataset.csv

    data/article_4/training/train.csv
    data/article_4/validation/validation.csv
    data/article_4/test/test.csv
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
# LOAD PARSED FILES
# ============================================================

def load_parsed_files() -> pd.DataFrame:

    files = sorted(PARSED_DIR.glob("*.csv"))

    if not files:
        raise RuntimeError(
            f"No parsed CSV files found in {PARSED_DIR}"
        )

    frames = []

    for file in files:
        print(f"[LOAD] {file.name}")

        df = pd.read_csv(file)

        if not df.empty:
            frames.append(df)

    if not frames:
        raise RuntimeError(
            "Parsed files exist but contain no data."
        )

    return pd.concat(frames, ignore_index=True)


# ============================================================
# CLEANING
# ============================================================

def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:

    print(f"[CLEAN] initial rows = {len(df)}")

    df = df.copy()
    df = df.drop_duplicates()
    df = df.dropna()

    df = df[df["event_rate"] >= 0]
    df = df[df["window_size_ms"] > 0]
    df = df[df["watermark_delay_ms"] > 0]

    print(f"[CLEAN] final rows = {len(df)}")

    return df


# ============================================================
# SPLIT
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

    dataset_file = (
        DATASET_DIR /
        "adaptive_control_dataset.csv"
    )

    df.to_csv(dataset_file, index=False)

    print(
        f"[SAVE] full dataset -> {dataset_file}"
    )

    train_df, validation_df, test_df = (
        split_dataset(df)
    )

    train_file = TRAIN_DIR / "train.csv"
    validation_file = VALIDATION_DIR / "validation.csv"
    test_file = TEST_DIR / "test.csv"

    train_df.to_csv(train_file, index=False)
    validation_df.to_csv(validation_file, index=False)
    test_df.to_csv(test_file, index=False)

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


if __name__ == "__main__":
    main()
