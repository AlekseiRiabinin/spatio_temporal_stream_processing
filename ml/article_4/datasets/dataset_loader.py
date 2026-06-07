"""
Dataset loader for Article 4 adaptive control models.

Responsibilities:
- Load train/validation/test datasets
- Validate schema
- Validate categorical values
- Provide convenient accessors

No preprocessing is performed here.
"""
import pandas as pd

from pathlib import Path
from typing import Tuple

from datasets.feature_schema import (
    FEATURE_COLUMNS,
    TARGET_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
    PROFILES,
    RATE_PATTERNS,
    MOTION_MODES,
)


# ============================================================
# Dataset locations
# ============================================================

DATA_ROOT = Path("data/article_4")

TRAIN_PATH = DATA_ROOT / "training" / "train.csv"
VALIDATION_PATH = DATA_ROOT / "validation" / "validation.csv"
TEST_PATH = DATA_ROOT / "test" / "test.csv"


# ============================================================
# Validation helpers
# ============================================================

def validate_required_columns(df: pd.DataFrame) -> None:
    """
    Ensure all required columns exist.
    """

    required_columns = (
        FEATURE_COLUMNS +
        TARGET_COLUMNS
    )

    missing_columns = [
        col
        for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )


def validate_categorical_values(df: pd.DataFrame) -> None:
    """
    Validate categorical feature values.
    """

    profile_values = set(df["profile"].unique())
    invalid_profiles = profile_values - set(PROFILES)

    if invalid_profiles:
        raise ValueError(
            f"Invalid profile values: {invalid_profiles}"
        )

    rate_pattern_values = set(df["rate_pattern"].unique())
    invalid_rate_patterns = (
        rate_pattern_values - set(RATE_PATTERNS)
    )

    if invalid_rate_patterns:
        raise ValueError(
            f"Invalid rate_pattern values: {invalid_rate_patterns}"
        )

    motion_mode_values = set(df["motion_mode"].unique())
    invalid_motion_modes = (
        motion_mode_values - set(MOTION_MODES)
    )

    if invalid_motion_modes:
        raise ValueError(
            f"Invalid motion_mode values: {invalid_motion_modes}"
        )


def validate_numeric_columns(df: pd.DataFrame) -> None:
    """
    Ensure numeric columns contain numeric values.
    """

    numeric_columns = (
        NUMERIC_FEATURE_COLUMNS +
        TARGET_COLUMNS
    )

    for column in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df[column]):
            raise TypeError(
                f"Column '{column}' is not numeric."
            )


def validate_dataset(df: pd.DataFrame) -> None:
    """
    Run full dataset validation.
    """

    validate_required_columns(df)
    validate_categorical_values(df)
    validate_numeric_columns(df)


# ============================================================
# Loading functions
# ============================================================

def load_dataset(path: Path) -> pd.DataFrame:
    """
    Load and validate a dataset.
    """

    if not path.exists():
        raise FileNotFoundError(
            f"Dataset not found: {path}"
        )

    df = pd.read_csv(path)

    validate_dataset(df)

    return df


def load_train_dataset() -> pd.DataFrame:
    return load_dataset(TRAIN_PATH)


def load_validation_dataset() -> pd.DataFrame:
    return load_dataset(VALIDATION_PATH)


def load_test_dataset() -> pd.DataFrame:
    return load_dataset(TEST_PATH)


def load_all_datasets() -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """
    Returns:
        train_df,
        validation_df,
        test_df
    """

    train_df = load_train_dataset()
    validation_df = load_validation_dataset()
    test_df = load_test_dataset()

    return (
        train_df,
        validation_df,
        test_df,
    )


# ============================================================
# Feature/target extraction
# ============================================================

def split_features_targets(
    df: pd.DataFrame,
):
    """
    Split dataframe into X and Y.
    """

    x = df[FEATURE_COLUMNS].copy()
    y = df[TARGET_COLUMNS].copy()

    return x, y


# ============================================================
# Debug entrypoint
# ============================================================

if __name__ == "__main__":

    train_df, validation_df, test_df = (
        load_all_datasets()
    )

    print(f"Train:      {train_df.shape}")
    print(f"Validation: {validation_df.shape}")
    print(f"Test:       {test_df.shape}")

    print("Dataset validation successful.")
