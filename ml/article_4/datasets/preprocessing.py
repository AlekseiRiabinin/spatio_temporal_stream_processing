"""
Preprocessing pipeline for Article 4 adaptive control models.

Responsibilities:
- One-hot encode categorical features
- Scale numerical features
- Transform train/validation/test datasets
- Persist preprocessing artifacts
"""

from pathlib import Path
from typing import Tuple

import joblib
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    OneHotEncoder,
    StandardScaler,
)

from .feature_schema import (
    CATEGORICAL_FEATURE_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
)


# ============================================================
# Artifact paths
# ============================================================

PROJECT_ROOT = (
    Path.home()
    / "Projects"
    / "spatio_temporal_stream_processing"
    / "ml"
)

ARTIFACTS_DIR = (
    PROJECT_ROOT
    / "article_4/models/artifacts"
)

ARTIFACTS_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

PREPROCESSOR_PATH = (
    ARTIFACTS_DIR
    / "preprocessor.joblib"
)


# ============================================================
# Preprocessor construction
# ============================================================

def build_preprocessor() -> ColumnTransformer:
    """
    Creates preprocessing pipeline.
    """

    categorical_transformer = Pipeline(
        steps=[
            (
                "onehot",
                OneHotEncoder(
                    handle_unknown="ignore",
                    sparse_output=False,
                ),
            ),
        ]
    )

    numeric_transformer = Pipeline(
        steps=[
            (
                "scaler",
                StandardScaler(),
            ),
        ]
    )

    return ColumnTransformer(
        transformers=[
            (
                "categorical",
                categorical_transformer,
                CATEGORICAL_FEATURE_COLUMNS,
            ),
            (
                "numeric",
                numeric_transformer,
                NUMERIC_FEATURE_COLUMNS,
            ),
        ]
    )


# ============================================================
# Fitting
# ============================================================

def fit_preprocessor(x_train: pd.DataFrame,) -> ColumnTransformer:
    """
    Fit preprocessor on training data only.
    """

    preprocessor = build_preprocessor()
    preprocessor.fit(x_train)

    return preprocessor


# ============================================================
# Transformations
# ============================================================

def transform_features(
    preprocessor: ColumnTransformer,
    x: pd.DataFrame,
):
    """
    Transform feature dataframe.
    """

    return preprocessor.transform(x)


def fit_transform_train(x_train: pd.DataFrame,):
    """
    Fit and transform training data.
    """

    preprocessor = fit_preprocessor(x_train)
    x_train_processed = (preprocessor.transform(x_train))

    return (x_train_processed, preprocessor,)


def transform_datasets(
    preprocessor: ColumnTransformer,
    x_train: pd.DataFrame,
    x_validation: pd.DataFrame,
    x_test: pd.DataFrame,
) -> Tuple:
    """
    Transform all datasets.
    """

    x_train_processed = (preprocessor.transform(x_train))
    x_validation_processed = (preprocessor.transform(x_validation))
    x_test_processed = (preprocessor.transform(x_test))

    return (
        x_train_processed,
        x_validation_processed,
        x_test_processed,
    )


# ============================================================
# Persistence
# ============================================================

def save_preprocessor(
    preprocessor: ColumnTransformer,
    path: Path = PREPROCESSOR_PATH,
) -> None:
    """
    Save fitted preprocessor.
    """

    joblib.dump(preprocessor, path,)


def load_preprocessor(path: Path = PREPROCESSOR_PATH,) -> ColumnTransformer:
    """
    Load fitted preprocessor.
    """

    return joblib.load(path)


# ============================================================
# Feature names
# ============================================================

def get_feature_names(preprocessor: ColumnTransformer,):
    """
    Returns transformed feature names.
    """

    return preprocessor.get_feature_names_out()


# ============================================================
# Convenience helper
# ============================================================

def prepare_training_data(
    x_train: pd.DataFrame,
    x_validation: pd.DataFrame,
    x_test: pd.DataFrame,
):
    """
    Complete preprocessing workflow.

    Returns:
        x_train_processed
        x_validation_processed
        x_test_processed
        preprocessor
    """

    preprocessor = fit_preprocessor(x_train)

    (
        x_train_processed,
        x_validation_processed,
        x_test_processed,
    ) = transform_datasets(
        preprocessor,
        x_train,
        x_validation,
        x_test,
    )

    return (
        x_train_processed,
        x_validation_processed,
        x_test_processed,
        preprocessor,
    )
