"""
GPU-accelerated XGBoost for Article 4 adaptive control.

Core model classes for training and inference.
"""

from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any
import time

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


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
    """Auto-select best available device."""
    gpu_available = is_gpu_available()
    
    if use_gpu is False:
        return {'device': 'cpu', 'tree_method': 'hist', 'predictor': 'cpu_predictor'}
    elif use_gpu is True and gpu_available:
        if verbose:
            print("🚀 GPU ACCELERATION ENABLED")
        return {'device': 'gpu', 'tree_method': 'gpu_hist', 'predictor': 'gpu_predictor'}
    elif use_gpu is True and not gpu_available:
        print("⚠️ GPU requested but not available - falling back to CPU")
        return {'device': 'cpu', 'tree_method': 'hist', 'predictor': 'cpu_predictor'}
    else:
        if gpu_available:
            if verbose:
                print("🚀 GPU ACCELERATION ENABLED")
            return {'device': 'gpu', 'tree_method': 'gpu_hist', 'predictor': 'gpu_predictor'}
        else:
            if verbose:
                print("💻 Using CPU")
            return {'device': 'cpu', 'tree_method': 'hist', 'predictor': 'cpu_predictor'}


# ============================================================
# Core Model Class
# ============================================================

class AdaptiveXGBoostGPU:
    """GPU-accelerated XGBoost model for adaptive control."""
    
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
        use_gpu: Optional[bool] = True,
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
        
        self.device_config = get_best_device(use_gpu, verbose)
        self.model = None
        self.feature_names = None
        self.is_fitted = False
        self.training_time = None
        
    def _build_model(self) -> xgb.XGBRegressor:
        """Build XGBoost regressor."""
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
            n_jobs=-1,
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
        """Train model."""
        self.feature_names = feature_names
        self.model = self._build_model()
        
        eval_set = [(X_train, y_train)]
        if X_val is not None and y_val is not None:
            eval_set.append((X_val, y_val))
        
        if self.verbose:
            print(f"\n📊 Training {self.name} on {self.device_config['device'].upper()}")
            print(f"   Training samples: {X_train.shape[0]}")
        
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
    def load(
        cls, path: Path,
        use_gpu: Optional[bool] = True,
        verbose: bool = True
    ) -> 'AdaptiveXGBoostGPU':
        """Load model from disk."""
        model_data = joblib.load(path)
        
        instance = cls(
            name=model_data['name'],
            **model_data['params'],
            use_gpu=use_gpu,
            verbose=verbose,
        )
        instance.model = model_data['model']
        instance.feature_names = model_data['feature_names']
        instance.is_fitted = True
        instance.training_time = model_data.get('training_time')
        
        if verbose:
            print(f"   📂 Loaded {instance.name} from {path}")
        
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
# Dual Model Controller
# ============================================================

class AdaptiveControlModelsGPU:
    """Container for both GPU-accelerated ML models."""
    
    def __init__(
        self,
        model_a: Optional[AdaptiveXGBoostGPU] = None,
        model_b: Optional[AdaptiveXGBoostGPU] = None,
        use_gpu: Optional[bool] = True,
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
        """Train both models."""
        self.model_a.fit(X_train, y_train_window, X_val, y_val_window, feature_names=feature_names)
        self.model_b.fit(X_train, y_train_watermark, X_val, y_val_watermark, feature_names=feature_names)
        return self
    
    def predict(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Get predictions from both models."""
        return self.model_a.predict(X), self.model_b.predict(X)
    
    def predict_with_arbitration(
        self,
        X: np.ndarray,
        min_window_ms: float = 1000,
        max_window_ms: float = 30000,
        min_watermark_ms: float = 0,
        max_watermark_ms: float = 5000,
        watermark_max_pct_of_window: float = 0.3,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Predict with safety bounds."""
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
        return {
            'window_model': self.model_a.evaluate(X_test, y_test_window),
            'watermark_model': self.model_b.evaluate(X_test, y_test_watermark),
            'combined_rmse': np.sqrt(
                (self.model_a.evaluate(X_test, y_test_window)['mse'] + 
                 self.model_b.evaluate(X_test, y_test_watermark)['mse']) / 2
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
    
    def save(
        self,
        model_a_path: Optional[Path] = None,
        model_b_path: Optional[Path] = None
    ):
        """Save both models."""
        self.model_a.save(model_a_path or MODEL_A_PATH)
        self.model_b.save(model_b_path or MODEL_B_PATH)
    
    @classmethod
    def load(
        cls,
        model_a_path: Optional[Path] = None,
        model_b_path: Optional[Path] = None,
        use_gpu: Optional[bool] = None
    ):
        """Load both models."""
        model_a = AdaptiveXGBoostGPU.load(model_a_path or MODEL_A_PATH, use_gpu=use_gpu)
        model_b = AdaptiveXGBoostGPU.load(model_b_path or MODEL_B_PATH, use_gpu=use_gpu)
        return cls(model_a=model_a, model_b=model_b)
