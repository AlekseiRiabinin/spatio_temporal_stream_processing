"""
Feature schema for Article 4 adaptive control models.

This module defines:

- input features
- target variables
- categorical columns
- numeric columns
- model feature order

All downstream components must import definitions from here
to guarantee consistent training and inference.
"""

from dataclasses import dataclass
from typing import List


# ============================================================
# Dataset columns
# ============================================================

PROFILE_COLUMN = "profile"
RATE_PATTERN_COLUMN = "rate_pattern"
MOTION_MODE_COLUMN = "motion_mode"

RUN_TIMESTAMP_COLUMN = "run_timestamp"


# ============================================================
# Input features (ML inputs)
# ============================================================

NUMERIC_FEATURE_COLUMNS: List[str] = [
    "event_rate",
    "disorder_ratio",
    "late_event_ratio",
    "avg_latency_ms",
    "window_fill_ratio",
    "interaction_rate",
    "collision_rate",
    "proximity_rate",
    "swarm_rate",
    "conflict_rate",
    "watermark_lag_ms",
    "processing_latency_ms",
]

CATEGORICAL_FEATURE_COLUMNS: List[str] = [
    PROFILE_COLUMN,
    RATE_PATTERN_COLUMN,
    MOTION_MODE_COLUMN,
]

FEATURE_COLUMNS: List[str] = (
    CATEGORICAL_FEATURE_COLUMNS
    + NUMERIC_FEATURE_COLUMNS
)


# ============================================================
# Regression targets
# ============================================================

WINDOW_TARGET = "adaptive_window_ms"
WATERMARK_TARGET = "adaptive_watermark_ms"

TARGET_COLUMNS: List[str] = [
    WINDOW_TARGET,
    WATERMARK_TARGET,
]


# ============================================================
# Optional metadata columns
# ============================================================

METADATA_COLUMNS: List[str] = [
    RUN_TIMESTAMP_COLUMN,
]


# ============================================================
# Raw configuration columns
# (useful for analysis only)
# ============================================================

RAW_CONFIGURATION_COLUMNS: List[str] = [
    "window_size_ms",
    "watermark_delay_ms",
]


# ============================================================
# Additional metrics
# (not currently used as ML inputs)
# ============================================================

COUNT_COLUMNS: List[str] = [
    "collision_count",
    "proximity_count",
    "swarm_count",
    "conflict_count",
    "total_events",
    "total_interactions",
]


# ============================================================
# Allowed categorical values
# ============================================================

PROFILES = [
    "realtime",
    "skewed",
    "late",
    "out_of_order",
    "mixed",
]

RATE_PATTERNS = [
    "constant",
    "burst",
    "wave",
]

MOTION_MODES = [
    "straight",
    "corridor",
    "swarm",
    "collision",
    "random_walk",
]


# ============================================================
# Model feature dimensions
# ============================================================

NUM_NUMERIC_FEATURES = len(NUMERIC_FEATURE_COLUMNS)
NUM_CATEGORICAL_FEATURES = len(CATEGORICAL_FEATURE_COLUMNS)

NUM_TARGETS = len(TARGET_COLUMNS)


# ============================================================
# Feature schema object
# ============================================================

@dataclass(frozen=True)
class FeatureSchema:
    feature_columns: List[str]
    target_columns: List[str]

    categorical_columns: List[str]
    numeric_columns: List[str]


SCHEMA = FeatureSchema(
    feature_columns=FEATURE_COLUMNS,
    target_columns=TARGET_COLUMNS,
    categorical_columns=CATEGORICAL_FEATURE_COLUMNS,
    numeric_columns=NUMERIC_FEATURE_COLUMNS,
)
