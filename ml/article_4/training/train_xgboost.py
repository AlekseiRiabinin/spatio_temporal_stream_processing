"""
Training utilities for XGBoost models.
"""

from typing import Optional, List, Dict
import numpy as np

from models.xgboost_model import AdaptiveXGBoostGPU, AdaptiveControlModelsGPU
from models.model_config import XGBoostConfig


def train_single_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: Optional[np.ndarray] = None,
    y_val: Optional[np.ndarray] = None,
    config: Optional[XGBoostConfig] = None,
    name: str = "model",
    feature_names: Optional[List[str]] = None,
    verbose: bool = True,
) -> AdaptiveXGBoostGPU:
    """
    Train a single XGBoost model.
    
    Args:
        X_train: Training features
        y_train: Training targets
        X_val: Validation features
        y_val: Validation targets
        config: Model configuration
        name: Model name
        feature_names: Feature names
        verbose: Print progress
    
    Returns:
        Trained model
    """
    if config is None:
        config = XGBoostConfig()
    
    model = AdaptiveXGBoostGPU(name=name, **config.to_dict(), verbose=verbose)
    model.fit(X_train, y_train, X_val, y_val, feature_names=feature_names)
    
    return model


def train_dual_model(
    X_train: np.ndarray,
    y_train_window: np.ndarray,
    y_train_watermark: np.ndarray,
    X_val: Optional[np.ndarray] = None,
    y_val_window: Optional[np.ndarray] = None,
    y_val_watermark: Optional[np.ndarray] = None,
    config_window: Optional[XGBoostConfig] = None,
    config_watermark: Optional[XGBoostConfig] = None,
    feature_names: Optional[List[str]] = None,
    use_gpu: Optional[bool] = None,
    verbose: bool = True,
) -> AdaptiveControlModelsGPU:
    """
    Train dual XGBoost models (window + watermark).
    
    Args:
        X_train: Training features
        y_train_window: Training window targets
        y_train_watermark: Training watermark targets
        X_val: Validation features
        y_val_window: Validation window targets
        y_val_watermark: Validation watermark targets
        config_window: Configuration for window model
        config_watermark: Configuration for watermark model
        feature_names: Feature names
        use_gpu: Use GPU acceleration
        verbose: Print progress
    
    Returns:
        Trained dual model container
    """
    models = AdaptiveControlModelsGPU(use_gpu=use_gpu)
    
    if config_window:
        models.model_a = AdaptiveXGBoostGPU(
            name="Model_A_Window",
            use_gpu=use_gpu,
            **config_window.to_dict(),
            verbose=verbose
        )
    
    if config_watermark:
        models.model_b = AdaptiveXGBoostGPU(
            name="Model_B_Watermark",
            use_gpu=use_gpu,
            **config_watermark.to_dict(),
            verbose=verbose
        )
    
    models.fit(
        X_train, y_train_window, y_train_watermark,
        X_val, y_val_window, y_val_watermark,
        feature_names=feature_names
    )
    
    return models


def train_from_best_params(
    X_train: np.ndarray,
    y_train_window: np.ndarray,
    y_train_watermark: np.ndarray,
    X_val: np.ndarray,
    y_val_window: np.ndarray,
    y_val_watermark: np.ndarray,
    window_params: Dict,
    watermark_params: Dict,
    feature_names: Optional[List[str]] = None,
    use_gpu: Optional[bool] = None,
    verbose: bool = True,
) -> AdaptiveControlModelsGPU:
    """
    Train models using best parameters from hyperparameter search.
    
    Args:
        X_train: Training features
        y_train_window: Training window targets
        y_train_watermark: Training watermark targets
        X_val: Validation features
        y_val_window: Validation window targets
        y_val_watermark: Validation watermark targets
        window_params: Best parameters for window model
        watermark_params: Best parameters for watermark model
        feature_names: Feature names
        use_gpu: Use GPU acceleration
        verbose: Print progress
    
    Returns:
        Trained dual model container
    """
    models = AdaptiveControlModelsGPU(use_gpu=use_gpu)
    
    # Create models with best parameters
    models.model_a = AdaptiveXGBoostGPU(
        name="Model_A_Window_Final",
        use_gpu=use_gpu,
        verbose=verbose,
        **window_params
    )
    
    models.model_b = AdaptiveXGBoostGPU(
        name="Model_B_Watermark_Final",
        use_gpu=use_gpu,
        verbose=verbose,
        **watermark_params
    )
    
    # Train
    models.fit(
        X_train, y_train_window, y_train_watermark,
        X_val, y_val_window, y_val_watermark,
        feature_names=feature_names
    )
    
    return models
