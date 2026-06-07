"""
Hyperparameter search for XGBoost models.

Contains grid search and random search utilities for hyperparameter tuning.
"""

import random
import time
import numpy as np
from typing import Dict, Optional, Tuple, List, Any
from sklearn.model_selection import ParameterGrid

from models.xgboost_model import AdaptiveXGBoostGPU
from models.model_config import DEFAULT_SEARCH_SPACE


class GPUGridSearch:
    """GPU-accelerated hyperparameter tuning."""
    
    def __init__(
        self,
        X_train: np.ndarray,
        y_train_window: np.ndarray,
        y_train_watermark: np.ndarray,
        X_val: np.ndarray,
        y_val_window: np.ndarray,
        y_val_watermark: np.ndarray,
        use_gpu: Optional[bool] = True,
        verbose: bool = True,
    ):
        self.X_train = X_train
        self.y_train_window = y_train_window
        self.y_train_watermark = y_train_watermark
        self.X_val = X_val
        self.y_val_window = y_val_window
        self.y_val_watermark = y_val_watermark
        self.use_gpu = use_gpu
        self.verbose = verbose
        
        # Detect device for tuning
        from models.xgboost_model import get_best_device
        self.device = get_best_device(use_gpu, verbose)['device']
    
    def quick_random_search(self, n_combinations: int = 50):
        """Quick random search for both models."""
        param_space = DEFAULT_SEARCH_SPACE.to_dict()
        all_params = list(ParameterGrid(param_space))
        random.shuffle(all_params)
        random_params = all_params[:n_combinations]
        
        print(f"\n🔍 Quick Random Search ({n_combinations} combinations) on {self.device.upper()}")
        
        # Search window model
        best_window_params, best_window_metrics = self._search_single_model(
            self.y_train_window, self.y_val_window, random_params, "Window"
        )
        
        # Search watermark model
        best_wm_params, best_wm_metrics = self._search_single_model(
            self.y_train_watermark, self.y_val_watermark, random_params, "Watermark"
        )
        
        return (best_window_params, best_window_metrics), (best_wm_params, best_wm_metrics)
    
    def grid_search_window_model(
        self,
        param_grid: Dict[str, List[Any]],
        n_iterations: Optional[int] = None,
    ) -> Tuple[Dict, Dict]:
        """Grid search for window model with GPU acceleration."""
        return self._grid_search_model(
            model_name="Window",
            y_train=self.y_train_window,
            y_val=self.y_val_window,
            param_grid=param_grid,
            n_iterations=n_iterations,
        )
    
    def grid_search_watermark_model(
        self,
        param_grid: Dict[str, List[Any]],
        n_iterations: Optional[int] = None,
    ) -> Tuple[Dict, Dict]:
        """Grid search for watermark model with GPU acceleration."""
        return self._grid_search_model(
            model_name="Watermark",
            y_train=self.y_train_watermark,
            y_val=self.y_val_watermark,
            param_grid=param_grid,
            n_iterations=n_iterations,
        )
    
    def _grid_search_model(
        self,
        model_name: str,
        y_train: np.ndarray,
        y_val: np.ndarray,
        param_grid: Dict[str, List[Any]],
        n_iterations: Optional[int] = None,
    ) -> Tuple[Dict, Dict]:
        """Internal grid search implementation."""
        all_params = list(ParameterGrid(param_grid))
        if n_iterations:
            random.shuffle(all_params)
            all_params = all_params[:n_iterations]
        
        best_rmse = float('inf')
        best_params = None
        best_metrics = None
        
        if self.verbose:
            print(f"\n🔍 Grid Search for {model_name} Model")
            print(f"   Device: {self.device.upper()}")
            print(f"   Combinations: {len(all_params)}")
            if self.device == 'gpu':
                print(f"   Estimated time: ~{len(all_params) * 0.3:.1f}s on GPU")
            else:
                print(f"   Estimated time: ~{len(all_params) * 2:.1f}s on CPU")
            print("-" * 60)
        
        start_time = time.time()
        
        for idx, params in enumerate(all_params, 1):
            if self.verbose and idx % 50 == 0:
                elapsed = time.time() - start_time
                eta = (elapsed / idx) * (len(all_params) - idx)
                print(f"   Progress: {idx}/{len(all_params)} ({idx/len(all_params)*100:.1f}%) | ETA: {eta:.1f}s")
            
            model = AdaptiveXGBoostGPU(
                name=f"{model_name}_Tuning",
                use_gpu=self.use_gpu,
                verbose=False,
                **params
            )
            
            model.fit(self.X_train, y_train, self.X_val, y_val)
            metrics = model.evaluate(self.X_val, y_val)
            
            if metrics['rmse'] < best_rmse:
                best_rmse = metrics['rmse']
                best_params = params
                best_metrics = metrics
                
                if self.verbose:
                    print(f"   ✨ New best RMSE: {best_rmse:.4f} | Params: {params}")
        
        elapsed = time.time() - start_time
        
        if self.verbose:
            print("-" * 60)
            print(f"✓ Grid search complete in {elapsed:.1f}s")
            print(f"  Best RMSE: {best_rmse:.4f}")
            print(f"  Best params: {best_params}")
            print("=" * 60)
        
        return best_params, best_metrics
    
    def _search_single_model(self, y_train, y_val, param_list, name):
        """Search for a single model."""
        best_rmse = float('inf')
        best_params = None
        best_metrics = None
        
        for params in param_list:
            model = AdaptiveXGBoostGPU(
                name=f"{name}_Tuning",
                use_gpu=self.use_gpu,
                verbose=False,
                **params
            )
            model.fit(self.X_train, y_train, self.X_val, y_val)
            metrics = model.evaluate(self.X_val, y_val)
            
            if metrics['rmse'] < best_rmse:
                best_rmse = metrics['rmse']
                best_params = params
                best_metrics = metrics
        
        return best_params, best_metrics
