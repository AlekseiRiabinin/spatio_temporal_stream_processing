"""
Model configuration for Article 4 adaptive control.

Contains configuration classes and default parameters for all models.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


@dataclass
class XGBoostConfig:
    """Configuration for XGBoost models."""
    
    # Core parameters
    n_estimators: int = 100
    max_depth: int = 4
    learning_rate: float = 0.1
    
    # Sampling parameters
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    colsample_bylevel: float = 0.8
    
    # Regularization
    min_child_weight: int = 3
    reg_alpha: float = 0.1
    reg_lambda: float = 1.0
    gamma: float = 0.0
    
    # Training
    random_state: int = 42
    early_stopping_rounds: int = 10
    
    # Device
    use_gpu: Optional[bool] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for model initialization."""
        return {
            'n_estimators': self.n_estimators,
            'max_depth': self.max_depth,
            'learning_rate': self.learning_rate,
            'subsample': self.subsample,
            'colsample_bytree': self.colsample_bytree,
            'colsample_bylevel': self.colsample_bylevel,
            'min_child_weight': self.min_child_weight,
            'reg_alpha': self.reg_alpha,
            'reg_lambda': self.reg_lambda,
            'gamma': self.gamma,
            'random_state': self.random_state,
            'early_stopping_rounds': self.early_stopping_rounds,
            'use_gpu': self.use_gpu,
        }
    
    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> 'XGBoostConfig':
        """Create config from dictionary."""
        return cls(**params)


@dataclass
class HyperparameterSearchSpace:
    """Search space for hyperparameter tuning."""
    
    # Default search space for XGBoost
    n_estimators: List[int] = field(default_factory=lambda: [50, 100, 150, 200])
    max_depth: List[int] = field(default_factory=lambda: [3, 4, 5, 6])
    learning_rate: List[float] = field(default_factory=lambda: [0.05, 0.1, 0.15, 0.2])
    min_child_weight: List[int] = field(default_factory=lambda: [1, 3, 5])
    subsample: List[float] = field(default_factory=lambda: [0.7, 0.8, 0.9])
    colsample_bytree: List[float] = field(default_factory=lambda: [0.7, 0.8, 0.9])
    reg_alpha: List[float] = field(default_factory=lambda: [0, 0.1, 0.2])
    reg_lambda: List[float] = field(default_factory=lambda: [0.5, 1.0, 1.5])
    
    def to_dict(self) -> Dict[str, List[Any]]:
        """Convert to dictionary for grid search."""
        return {
            'n_estimators': self.n_estimators,
            'max_depth': self.max_depth,
            'learning_rate': self.learning_rate,
            'min_child_weight': self.min_child_weight,
            'subsample': self.subsample,
            'colsample_bytree': self.colsample_bytree,
            'reg_alpha': self.reg_alpha,
            'reg_lambda': self.reg_lambda,
        }


@dataclass
class LightSearchSpace:
    """Light search space for quick testing purposes."""
    
    # Reduced search space for fast experimentation
    n_estimators: List[int] = field(default_factory=lambda: [50, 100])
    max_depth: List[int] = field(default_factory=lambda: [3, 4])
    learning_rate: List[float] = field(default_factory=lambda: [0.05, 0.1])
    min_child_weight: List[int] = field(default_factory=lambda: [3, 5])
    subsample: List[float] = field(default_factory=lambda: [0.8])
    colsample_bytree: List[float] = field(default_factory=lambda: [0.8])
    reg_alpha: List[float] = field(default_factory=lambda: [0.05, 0.1])
    reg_lambda: List[float] = field(default_factory=lambda: [0.5, 1.0])
    
    def to_dict(self) -> Dict[str, List[Any]]:
        """Convert to dictionary for grid search."""
        return {
            'n_estimators': self.n_estimators,
            'max_depth': self.max_depth,
            'learning_rate': self.learning_rate,
            'min_child_weight': self.min_child_weight,
            'subsample': self.subsample,
            'colsample_bytree': self.colsample_bytree,
            'reg_alpha': self.reg_alpha,
            'reg_lambda': self.reg_lambda,
        }


@dataclass
class ReducedSearchSpace:
    """Reduced search space for balanced tuning (recommended for final training)."""
    
    # Balanced search space - good coverage without exhaustive time
    n_estimators: List[int] = field(default_factory=lambda: [100, 150, 200])
    max_depth: List[int] = field(default_factory=lambda: [4, 5, 6])
    learning_rate: List[float] = field(default_factory=lambda: [0.05, 0.1, 0.15])
    min_child_weight: List[int] = field(default_factory=lambda: [3, 5])
    subsample: List[float] = field(default_factory=lambda: [0.8, 0.9])
    colsample_bytree: List[float] = field(default_factory=lambda: [0.8, 0.9])
    reg_alpha: List[float] = field(default_factory=lambda: [0.05, 0.1])
    reg_lambda: List[float] = field(default_factory=lambda: [0.5, 1.0])
    
    def to_dict(self) -> Dict[str, List[Any]]:
        """Convert to dictionary for grid search."""
        return {
            'n_estimators': self.n_estimators,
            'max_depth': self.max_depth,
            'learning_rate': self.learning_rate,
            'min_child_weight': self.min_child_weight,
            'subsample': self.subsample,
            'colsample_bytree': self.colsample_bytree,
            'reg_alpha': self.reg_alpha,
            'reg_lambda': self.reg_lambda,
        }


@dataclass
class ControlBounds:
    """Safety bounds for control parameters."""
    
    min_window_ms: float = 1000
    max_window_ms: float = 30000
    min_watermark_ms: float = 0
    max_watermark_ms: float = 5000
    watermark_max_pct_of_window: float = 0.3
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary."""
        return {
            'min_window_ms': self.min_window_ms,
            'max_window_ms': self.max_window_ms,
            'min_watermark_ms': self.min_watermark_ms,
            'max_watermark_ms': self.max_watermark_ms,
            'watermark_max_pct_of_window': self.watermark_max_pct_of_window,
        }


# Default configurations
DEFAULT_XGBOOST_CONFIG = XGBoostConfig()
DEFAULT_SEARCH_SPACE = HyperparameterSearchSpace()
LIGHT_SEARCH_SPACE = LightSearchSpace()
REDUCED_SEARCH_SPACE = ReducedSearchSpace()
DEFAULT_CONTROL_BOUNDS = ControlBounds()
