"""
GPU-accelerated XGBoost for Article 4 adaptive control.

Optimized for hyperparameter tuning with ~4000 records.
GPU provides 6-10x speedup during grid search.
"""

from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any
import json
import time

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import ParameterGrid


# ============================================================
# Paths
# ============================================================

PROJECT_ROOT = Path(__file__).parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "article_4" / "models" / "xgboost"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

MODEL_A_PATH = MODELS_DIR / "model_a_window.joblib"
MODEL_B_PATH = MODELS_DIR / "model_b_watermark.joblib"


# ============================================================
# GPU Detection
# ============================================================

def is_gpu_available() -> bool:
    """Check if GPU is available for XGBoost."""
    try:
        build_info = xgb.build_info()
        if build_info.get('USE_CUDA', False):
            # Quick test with small dataset
            X_test = np.random.randn(100, 10)
            y_test = np.random.randn(100)
            model = xgb.XGBRegressor(
                n_estimators=1,
                tree_method='gpu_hist',
                predictor='gpu_predictor'
            )
            model.fit(X_test, y_test)
            return True
    except:
        pass
    return False


def get_best_device(use_gpu: Optional[bool] = None, verbose: bool = True) -> Dict:
    """
    Auto-select best available device.
    
    Returns:
        Dictionary with device configuration
    """
    gpu_available = is_gpu_available()
    
    if use_gpu is False:
        return {'device': 'cpu', 'tree_method': 'hist', 'predictor': 'cpu_predictor'}
    elif use_gpu is True and gpu_available:
        if verbose:
            print("🚀 GPU ACCELERATION ENABLED (6-10x speedup for hyperparameter tuning)")
        return {'device': 'gpu', 'tree_method': 'gpu_hist', 'predictor': 'gpu_predictor'}
    elif use_gpu is True and not gpu_available:
        print("⚠️  GPU requested but not available - falling back to CPU")
        return {'device': 'cpu', 'tree_method': 'hist', 'predictor': 'cpu_predictor'}
    else:  # Auto
        if gpu_available:
            if verbose:
                print("🚀 GPU ACCELERATION ENABLED (6-10x speedup for hyperparameter tuning)")
            return {'device': 'gpu', 'tree_method': 'gpu_hist', 'predictor': 'gpu_predictor'}
        else:
            if verbose:
                print("💻 Using CPU (install xgboost-gpu for 6-10x faster tuning)")
            return {'device': 'cpu', 'tree_method': 'hist', 'predictor': 'cpu_predictor'}


# ============================================================
# GPU-Accelerated Model
# ============================================================

class AdaptiveXGBoostGPU:
    """
    GPU-accelerated XGBoost model for adaptive control.
    
    Optimized for:
    - Hyperparameter tuning (6-10x faster with GPU)
    - Small dataset (4000 records)
    - Two independent models (window + watermark)
    """
    
    def __init__(
        self,
        name: str = "model",
        n_estimators: int = 100,
        max_depth: int = 4,
        learning_rate: float = 0.1,
        subsample: float = 0.8,
        colsample_bytree: float = 0.8,
        colsample_bylevel: float = 0.8,
        min_child_weight: int = 3,
        reg_alpha: float = 0.1,
        reg_lambda: float = 1.0,
        gamma: float = 0.0,
        random_state: int = 42,
        early_stopping_rounds: int = 10,
        use_gpu: Optional[bool] = None,  # None=auto, True=force, False=force CPU
        verbose: bool = True,
    ):
        self.name = name
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.learning_rate = learning_rate
        self.subsample = subsample
        self.colsample_bytree = colsample_bytree
        self.colsample_bylevel = colsample_bylevel
        self.min_child_weight = min_child_weight
        self.reg_alpha = reg_alpha
        self.reg_lambda = reg_lambda
        self.gamma = gamma
        self.random_state = random_state
        self.early_stopping_rounds = early_stopping_rounds
        self.verbose = verbose
        
        # Get best device configuration
        self.device_config = get_best_device(use_gpu, verbose)
        
        self.model = None
        self.feature_names = None
        self.is_fitted = False
        self.training_time = None
        
    def _build_model(self) -> xgb.XGBRegressor:
        """Build XGBoost regressor with GPU/CPU optimization."""
        return xgb.XGBRegressor(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            learning_rate=self.learning_rate,
            subsample=self.subsample,
            colsample_bytree=self.colsample_bytree,
            colsample_bylevel=self.colsample_bylevel,
            min_child_weight=self.min_child_weight,
            reg_alpha=self.reg_alpha,
            reg_lambda=self.reg_lambda,
            gamma=self.gamma,
            random_state=self.random_state,
            tree_method=self.device_config['tree_method'],
            predictor=self.device_config['predictor'],
            objective='reg:squarederror',
            n_jobs=-1,  # CPU cores for data loading
            verbosity=0 if not self.verbose else 1,
        )
    
    def fit(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val: Optional[np.ndarray] = None,
        feature_names: Optional[List[str]] = None,
    ) -> 'AdaptiveXGBoostGPU':
        """Train model with timing."""
        
        self.feature_names = feature_names
        self.model = self._build_model()
        
        # Prepare evaluation set
        eval_set = [(X_train, y_train)]
        if X_val is not None and y_val is not None:
            eval_set.append((X_val, y_val))
        
        if self.verbose:
            print(f"\n📊 Training {self.name} on {self.device_config['device'].upper()}")
            print(f"   Training samples: {X_train.shape[0]}")
            if X_val is not None:
                print(f"   Validation samples: {X_val.shape[0]}")
        
        # Train with timing
        start_time = time.time()
        self.model.fit(
            X_train, y_train,
            eval_set=eval_set,
            eval_metric='rmse',
            verbose=self.verbose,
            early_stopping_rounds=self.early_stopping_rounds if X_val is not None else None,
        )
        self.training_time = time.time() - start_time
        
        self.is_fitted = True
        
        if self.verbose:
            print(f"   ✓ {self.name} trained in {self.training_time:.2f}s\n")
        
        return self
    
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict target value."""
        if not self.is_fitted:
            raise RuntimeError(f"{self.name} must be fitted before prediction.")
        return self.model.predict(X)
    
    def evaluate(self, X: np.ndarray, y_true: np.ndarray) -> Dict[str, float]:
        """Evaluate model performance."""
        if not self.is_fitted:
            raise RuntimeError(f"{self.name} must be fitted before evaluation.")
        
        y_pred = self.predict(X)
        
        return {
            'mse': mean_squared_error(y_true, y_pred),
            'rmse': np.sqrt(mean_squared_error(y_true, y_pred)),
            'mae': mean_absolute_error(y_true, y_pred),
            'r2': r2_score(y_true, y_pred),
        }
    
    def get_feature_importance(self) -> pd.DataFrame:
        """Get feature importance."""
        if not self.is_fitted:
            raise RuntimeError(f"{self.name} must be fitted first.")
        
        if self.feature_names is None:
            raise ValueError("Feature names not provided during training.")
        
        importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.model.feature_importances_,
        }).sort_values('importance', ascending=False)
        
        importance['importance_percent'] = (
            importance['importance'] / importance['importance'].sum() * 100
        )
        
        return importance
    
    def save(self, path: Path) -> None:
        """Save model to disk."""
        if not self.is_fitted:
            raise RuntimeError(f"Cannot save unfitted {self.name}")
        
        model_data = {
            'model': self.model,
            'feature_names': self.feature_names,
            'params': self.get_params(),
            'device_config': self.device_config,
            'name': self.name,
            'training_time': self.training_time,
        }
        joblib.dump(model_data, path)
        if self.verbose:
            print(f"   💾 Saved {self.name} to {path}")
    
    @classmethod
    def load(cls, path: Path, use_gpu: Optional[bool] = None, verbose: bool = True) -> 'AdaptiveXGBoostGPU':
        """Load model from disk."""
        model_data = joblib.load(path)
        
        # Create instance with saved params but allow device override
        instance = cls(
            name=model_data['name'],
            **model_data['params'],
            use_gpu=use_gpu,  # Allow device override at load time
            verbose=verbose,
        )
        instance.model = model_data['model']
        instance.feature_names = model_data['feature_names']
        instance.is_fitted = True
        instance.training_time = model_data.get('training_time')
        
        if verbose:
            print(f"   📂 Loaded {instance.name} from {path}")
            print(f"   Running on {instance.device_config['device'].upper()}")
        
        return instance
    
    def get_params(self) -> Dict:
        """Get model parameters."""
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
        }


# ============================================================
# GPU-Accelerated Hyperparameter Tuner
# ============================================================

class GPUGridSearch:
    """
    GPU-accelerated hyperparameter tuning for both models.
    
    Typical speedup: 6-10x compared to CPU grid search.
    """
    
    def __init__(
        self,
        X_train: np.ndarray,
        y_train_window: np.ndarray,
        y_train_watermark: np.ndarray,
        X_val: np.ndarray,
        y_val_window: np.ndarray,
        y_val_watermark: np.ndarray,
        use_gpu: Optional[bool] = None,
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
        device_config = get_best_device(use_gpu, verbose)
        self.device = device_config['device']
        
    def grid_search_window_model(
        self,
        param_grid: Dict[str, List[Any]],
        n_iterations: Optional[int] = None,
    ) -> Tuple[Dict, Dict]:
        """
        Grid search for window model with GPU acceleration.
        
        Args:
            param_grid: Parameter grid to search
            n_iterations: Limit number of combinations (for random search)
            
        Returns:
            (best_params, best_metrics)
        """
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
        """
        Grid search for watermark model with GPU acceleration.
        
        Args:
            param_grid: Parameter grid to search
            n_iterations: Limit number of combinations
            
        Returns:
            (best_params, best_metrics)
        """
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
        
        # Generate parameter combinations
        all_params = list(ParameterGrid(param_grid))
        if n_iterations:
            import random
            random.shuffle(all_params)
            all_params = all_params[:n_iterations]
        
        best_rmse = float('inf')
        best_params = None
        best_metrics = None
        
        if self.verbose:
            print(f"\n🔍 Grid Search for {model_name} Model")
            print(f"   Device: {self.device.upper()}")
            print(f"   Combinations: {len(all_params)}")
            print(f"   Estimated time: ~{len(all_params) * 0.3:.1f}s on GPU" if self.device == 'gpu' 
                  else f"   Estimated time: ~{len(all_params) * 2:.1f}s on CPU")
            print("-" * 60)
        
        start_time = time.time()
        
        for idx, params in enumerate(all_params, 1):
            if self.verbose and idx % 50 == 0:
                elapsed = time.time() - start_time
                eta = (elapsed / idx) * (len(all_params) - idx)
                print(f"   Progress: {idx}/{len(all_params)} ({idx/len(all_params)*100:.1f}%) | ETA: {eta:.1f}s")
            
            # Create and train model
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
    
    def quick_random_search(
        self,
        n_combinations: int = 50,
    ) -> Tuple[Dict, Dict]:
        """
        Quick random search for both models.
        
        Useful for initial exploration before full grid search.
        
        Args:
            n_combinations: Number of random combinations to try
            
        Returns:
            (best_window_params, best_watermark_params)
        """
        
        # Define search space
        param_space = {
            'n_estimators': [50, 100, 150, 200],
            'max_depth': [3, 4, 5, 6],
            'learning_rate': [0.05, 0.1, 0.15, 0.2],
            'min_child_weight': [1, 3, 5],
            'subsample': [0.7, 0.8, 0.9],
            'colsample_bytree': [0.7, 0.8, 0.9],
            'reg_alpha': [0, 0.1, 0.2],
            'reg_lambda': [0.5, 1.0, 1.5],
        }
        
        # Generate random combinations
        import random
        all_params = list(ParameterGrid(param_space))
        random.shuffle(all_params)
        random_params = all_params[:n_combinations]
        
        print(f"\n🔍 Quick Random Search ({n_combinations} combinations)")
        print(f"   Device: {self.device.upper()}")
        print("-" * 60)
        
        # Search for window model
        print("\n📊 Searching for WINDOW model...")
        window_params, window_metrics = self._grid_search_model(
            "Window_Quick",
            self.y_train_window,
            self.y_val_window,
            {k: [v] for k, v in random_params[0].items()},  # Hack to reuse method
        )
        
        # Actually do proper random search
        best_window_rmse = float('inf')
        best_window_params = None
        best_window_metrics = None
        
        start_time = time.time()
        for idx, params in enumerate(random_params, 1):
            model = AdaptiveXGBoostGPU(
                name="Window_Quick",
                use_gpu=self.use_gpu,
                verbose=False,
                **params
            )
            model.fit(self.X_train, self.y_train_window, self.X_val, self.y_val_window)
            metrics = model.evaluate(self.X_val, self.y_val_window)
            
            if metrics['rmse'] < best_window_rmse:
                best_window_rmse = metrics['rmse']
                best_window_params = params
                best_window_metrics = metrics
        
        # Search for watermark model
        best_wm_rmse = float('inf')
        best_wm_params = None
        best_wm_metrics = None
        
        for idx, params in enumerate(random_params, 1):
            model = AdaptiveXGBoostGPU(
                name="Watermark_Quick",
                use_gpu=self.use_gpu,
                verbose=False,
                **params
            )
            model.fit(self.X_train, self.y_train_watermark, self.X_val, self.y_val_watermark)
            metrics = model.evaluate(self.X_val, self.y_val_watermark)
            
            if metrics['rmse'] < best_wm_rmse:
                best_wm_rmse = metrics['rmse']
                best_wm_params = params
                best_wm_metrics = metrics
        
        elapsed = time.time() - start_time
        
        print(f"\n✓ Random search complete in {elapsed:.1f}s")
        print(f"\nBest Window Model: RMSE={best_window_rmse:.4f}")
        print(f"  Params: {best_window_params}")
        print(f"\nBest Watermark Model: RMSE={best_wm_rmse:.4f}")
        print(f"  Params: {best_wm_params}")
        
        return (best_window_params, best_window_metrics), (best_wm_params, best_wm_metrics)


# ============================================================
# Dual Model Controller (GPU Accelerated)
# ============================================================

class AdaptiveControlModelsGPU:
    """
    Container for both GPU-accelerated ML models.
    
    Model A: Predicts window_size_ms
    Model B: Predicts watermark_delay_ms
    """
    
    def __init__(
        self,
        model_a: Optional[AdaptiveXGBoostGPU] = None,
        model_b: Optional[AdaptiveXGBoostGPU] = None,
        use_gpu: Optional[bool] = None,
    ):
        self.model_a = model_a or AdaptiveXGBoostGPU(name="Model_A_Window", use_gpu=use_gpu)
        self.model_b = model_b or AdaptiveXGBoostGPU(name="Model_B_Watermark", use_gpu=use_gpu)
        self.use_gpu = use_gpu
        
    def fit(
        self,
        X_train: np.ndarray,
        y_train_window: np.ndarray,
        y_train_watermark: np.ndarray,
        X_val: Optional[np.ndarray] = None,
        y_val_window: Optional[np.ndarray] = None,
        y_val_watermark: Optional[np.ndarray] = None,
        feature_names: Optional[List[str]] = None,
    ) -> 'AdaptiveControlModelsGPU':
        """Train both models independently."""
        
        self.model_a.fit(
            X_train, y_train_window,
            X_val, y_val_window,
            feature_names=feature_names,
        )
        
        self.model_b.fit(
            X_train, y_train_watermark,
            X_val, y_val_watermark,
            feature_names=feature_names,
        )
        
        return self
    
    def predict(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Get predictions from both models."""
        window_pred = self.model_a.predict(X)
        watermark_pred = self.model_b.predict(X)
        return window_pred, watermark_pred
    
    def predict_with_arbitration(
        self,
        X: np.ndarray,
        min_window_ms: float = 1000,
        max_window_ms: float = 30000,
        min_watermark_ms: float = 0,
        max_watermark_ms: float = 5000,
        watermark_max_pct_of_window: float = 0.3,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Predict with safety bounds for production."""
        window_pred, watermark_pred = self.predict(X)
        
        window_pred = np.clip(window_pred, min_window_ms, max_window_ms)
        watermark_pred = np.clip(watermark_pred, min_watermark_ms, max_watermark_ms)
        watermark_pred = np.minimum(watermark_pred, window_pred * watermark_max_pct_of_window)
        
        return window_pred, watermark_pred
    
    def evaluate(
        self,
        X_test: np.ndarray,
        y_test_window: np.ndarray,
        y_test_watermark: np.ndarray,
    ) -> Dict[str, Any]:
        """Evaluate both models."""
        
        window_metrics = self.model_a.evaluate(X_test, y_test_window)
        watermark_metrics = self.model_b.evaluate(X_test, y_test_watermark)
        
        return {
            'window_model': window_metrics,
            'watermark_model': watermark_metrics,
            'combined_rmse': np.sqrt(
                (window_metrics['mse'] + watermark_metrics['mse']) / 2
            ),
            'training_times': {
                'window': self.model_a.training_time,
                'watermark': self.model_b.training_time,
            },
            'devices': {
                'window': self.model_a.device_config['device'],
                'watermark': self.model_b.device_config['device'],
            }
        }
    
    def save(self, model_a_path: Optional[Path] = None, model_b_path: Optional[Path] = None):
        """Save both models."""
        self.model_a.save(model_a_path or MODEL_A_PATH)
        self.model_b.save(model_b_path or MODEL_B_PATH)
    
    @classmethod
    def load(cls, model_a_path: Optional[Path] = None, model_b_path: Optional[Path] = None, use_gpu: Optional[bool] = None):
        """Load both models."""
        model_a = AdaptiveXGBoostGPU.load(model_a_path or MODEL_A_PATH, use_gpu=use_gpu)
        model_b = AdaptiveXGBoostGPU.load(model_b_path or MODEL_B_PATH, use_gpu=use_gpu)
        return cls(model_a=model_a, model_b=model_b)
    
    def export_to_onnx(self, output_dir: Optional[Path] = None) -> Tuple[Path, Path]:
        """Export to ONNX for Flink deployment."""
        try:
            import onnxmltools
            from onnxmltools.convert import convert_xgboost
            from onnxmltools.convert.common.data_types import FloatTensorType
            import onnx
            
            output_dir = output_dir or MODELS_DIR
            output_dir.mkdir(parents=True, exist_ok=True)
            
            window_path = output_dir / "model_a_window.onnx"
            watermark_path = output_dir / "model_b_watermark.onnx"
            
            n_features = len(self.model_a.feature_names) if self.model_a.feature_names else 13
            initial_type = [('float_input', FloatTensorType([None, n_features]))]
            
            if self.model_a.is_fitted:
                onnx_model = convert_xgboost(self.model_a.model, initial_types=initial_type)
                onnx.save(onnx_model, str(window_path))
                print(f"   ✓ Exported window model to {window_path}")
            
            if self.model_b.is_fitted:
                onnx_model = convert_xgboost(self.model_b.model, initial_types=initial_type)
                onnx.save(onnx_model, str(watermark_path))
                print(f"   ✓ Exported watermark model to {watermark_path}")
            
            # Save metadata for Flink
            metadata = {
                'feature_names': self.model_a.feature_names,
                'n_features': n_features,
                'window_model_path': str(window_path),
                'watermark_model_path': str(watermark_path),
                'control_bounds': {
                    'min_window_ms': 1000,
                    'max_window_ms': 30000,
                    'min_watermark_ms': 0,
                    'max_watermark_ms': 5000,
                }
            }
            
            with open(output_dir / "model_metadata.json", 'w') as f:
                json.dump(metadata, f, indent=2)
            
            return window_path, watermark_path
            
        except ImportError as e:
            print(f"⚠️  ONNX export failed: {e}")
            print("   Install: pip install onnxmltools onnx")
            return None, None


# ============================================================
# Factory Functions
# ============================================================

def create_models_with_gpu(use_gpu: Optional[bool] = None) -> AdaptiveControlModelsGPU:
    """Create models with GPU acceleration for hyperparameter tuning."""
    return AdaptiveControlModelsGPU(use_gpu=use_gpu)


# ============================================================
# Usage Example
# ============================================================

if __name__ == "__main__":
    
    print("=" * 70)
    print("GPU-Accelerated Adaptive Control Models")
    print("=" * 70)
    
    # Check GPU availability
    gpu_available = is_gpu_available()
    print(f"\nGPU Available: {gpu_available}")
    
    # Generate sample data
    np.random.seed(42)
    n_samples = 4000
    n_features = 13
    
    X = np.random.randn(n_samples, n_features)
    y_window = np.random.uniform(1000, 30000, n_samples)
    y_watermark = np.random.uniform(0, 5000, n_samples)
    
    # Split
    split_train = int(0.7 * n_samples)
    split_val = int(0.85 * n_samples)
    
    X_train = X[:split_train]
    X_val = X[split_train:split_val]
    X_test = X[split_val:]
    
    yw_train = y_window[:split_train]
    yw_val = y_window[split_train:split_val]
    yw_test = y_window[split_val:]
    
    ywm_train = y_watermark[:split_train]
    ywm_val = y_watermark[split_train:split_val]
    ywm_test = y_watermark[split_val:]
    
    # Example 1: Quick random search with GPU
    print("\n" + "=" * 70)
    print("EXAMPLE 1: GPU-Accelerated Random Search")
    print("=" * 70)
    
    tuner = GPUGridSearch(
        X_train, yw_train, ywm_train,
        X_val, yw_val, ywm_val,
        use_gpu=None,  # Auto-detect
        verbose=True,
    )
    
    # Quick random search (50 combinations)
    (best_window_params, _), (best_wm_params, _) = tuner.quick_random_search(n_combinations=50)
    
    # Example 2: Train final models with best parameters
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Training Final Models with Best Parameters")
    print("=" * 70)
    
    models = create_models_with_gpu(use_gpu=None)
    
    # Use best params from search
    models.model_a = AdaptiveXGBoostGPU(name="Model_A_Window", **best_window_params)
    models.model_b = AdaptiveXGBoostGPU(name="Model_B_Watermark", **best_wm_params)
    
    # Train
    feature_names = [f"feature_{i}" for i in range(n_features)]
    models.fit(X_train, yw_train, ywm_train, X_val, yw_val, ywm_val, feature_names)
    
    # Evaluate
    metrics = models.evaluate(X_test, yw_test, ywm_test)
    print(f"\n📊 Final Evaluation:")
    print(f"   Window RMSE: {metrics['window_model']['rmse']:.2f} ms")
    print(f"   Watermark RMSE: {metrics['watermark_model']['rmse']:.2f} ms")
    print(f"   Combined RMSE: {metrics['combined_rmse']:.2f} ms")
    print(f"   Devices: {metrics['devices']}")
    
    # Save models
    models.save()
    
    # Export to ONNX for Flink
    print("\n📦 Exporting to ONNX for Flink deployment...")
    models.export_to_onnx()
    
    print("\n✅ Ready for production with GPU-accelerated tuning!")
