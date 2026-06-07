"""
Model factory for creating configured model instances.

Provides factory methods for creating models with different configurations.
"""

import numpy as np
from typing import Optional

from models.model_config import (
    XGBoostConfig,
    DEFAULT_XGBOOST_CONFIG,
    LIGHT_XGBOOST_CONFIG,
    HEAVY_XGBOOST_CONFIG,
)
from models.xgboost_model import AdaptiveXGBoostGPU, AdaptiveControlModelsGPU


class ModelFactory:
    """Factory for creating configured model instances."""
    
    @staticmethod
    def create_xgboost_model(
        name: str = "model",
        config: Optional[XGBoostConfig] = None,
        **kwargs
    ) -> AdaptiveXGBoostGPU:
        """
        Create a single XGBoost model with given configuration.
        
        Args:
            name: Model name
            config: XGBoostConfig instance
            **kwargs: Override specific parameters
            
        Returns:
            Configured AdaptiveXGBoostGPU instance
        """
        if config is None:
            config = DEFAULT_XGBOOST_CONFIG
        
        # Merge config with kwargs
        params = config.to_dict()
        params.update(kwargs)
        
        return AdaptiveXGBoostGPU(name=name, **params)
    
    @staticmethod
    def create_dual_model(
        config_a: Optional[XGBoostConfig] = None,
        config_b: Optional[XGBoostConfig] = None,
        use_gpu: Optional[bool] = None,
    ) -> AdaptiveControlModelsGPU:
        """
        Create dual model (window + watermark) with configurations.
        
        Args:
            config_a: Configuration for window model
            config_b: Configuration for watermark model
            use_gpu: Force GPU usage
            
        Returns:
            Configured AdaptiveControlModelsGPU instance
        """
        model_a = ModelFactory.create_xgboost_model(
            name="Model_A_Window",
            config=config_a,
            use_gpu=use_gpu,
        )
        model_b = ModelFactory.create_xgboost_model(
            name="Model_B_Watermark",
            config=config_b,
            use_gpu=use_gpu,
        )
        
        return AdaptiveControlModelsGPU(model_a=model_a, model_b=model_b)
    
    @staticmethod
    def create_default_models(use_gpu: Optional[bool] = True) -> AdaptiveControlModelsGPU:
        """Create models with default configuration."""
        return ModelFactory.create_dual_model(
            config_a=DEFAULT_XGBOOST_CONFIG,
            config_b=DEFAULT_XGBOOST_CONFIG,
            use_gpu=use_gpu,
        )
    

class TunerFactory:
    """Factory for creating hyperparameter tuners."""
    
    @staticmethod
    def create_tuner(
        X_train: np.ndarray,
        y_train_window: np.ndarray,
        y_train_watermark: np.ndarray,
        X_val: np.ndarray,
        y_val_window: np.ndarray,
        y_val_watermark: np.ndarray,
        use_gpu: Optional[bool] = True,
        verbose: bool = True,
    ):
        """Create a GPUGridSearch instance."""
        from models.xgboost_model import GPUGridSearch
        return GPUGridSearch(
            X_train, y_train_window, y_train_watermark,
            X_val, y_val_window, y_val_watermark,
            use_gpu=use_gpu,
            verbose=verbose,
        )
