"""
Export utilities for XGBoost models to ONNX format.
"""

from pathlib import Path
from typing import Optional, Tuple
import json

from models.xgboost_model import AdaptiveControlModelsGPU


def export_models_to_onnx(
    models: AdaptiveControlModelsGPU,
    output_dir: Optional[Path] = None,
    model_a_name: str = "model_a_window.onnx",
    model_b_name: str = "model_b_watermark.onnx",
) -> Tuple[Path, Path]:
    """
    Export trained XGBoost models to ONNX format for Flink deployment.
    
    Args:
        models: Trained dual model container
        output_dir: Output directory for ONNX files
        model_a_name: Filename for window model
        model_b_name: Filename for watermark model
    
    Returns:
        Paths to exported ONNX models
    """
    try:
        from onnxmltools.convert import convert_xgboost
        from onnxmltools.convert.common.data_types import FloatTensorType
        import onnx
        
        if output_dir is None:
            # Use default directory
            from models.xgboost_model import MODELS_DIR
            output_dir = MODELS_DIR
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        window_path = output_dir / model_a_name
        watermark_path = output_dir / model_b_name
        
        n_features = len(models.model_a.feature_names) if models.model_a.feature_names else 13
        initial_type = [('float_input', FloatTensorType([None, n_features]))]
        
        if models.model_a.is_fitted:
            onnx_model = convert_xgboost(models.model_a.model, initial_types=initial_type)
            onnx.save(onnx_model, str(window_path))
            print(f"   ✓ Exported window model to {window_path}")
        
        if models.model_b.is_fitted:
            onnx_model = convert_xgboost(models.model_b.model, initial_types=initial_type)
            onnx.save(onnx_model, str(watermark_path))
            print(f"   ✓ Exported watermark model to {watermark_path}")
        
        # Save metadata
        metadata = {
            'feature_names': models.model_a.feature_names,
            'n_features': n_features,
            'window_model_path': str(window_path),
            'watermark_model_path': str(watermark_path),
        }
        
        with open(output_dir / "onnx_export_metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return window_path, watermark_path
        
    except ImportError as e:
        print(f"⚠️ ONNX export failed: {e}")
        print("   Install: pip install onnxmltools onnx")
        return None, None


def export_single_model_to_onnx(
    model,
    output_path: Path,
    feature_names: Optional[list] = None,
    n_features: int = 13,
) -> Optional[Path]:
    """
    Export a single XGBoost model to ONNX.
    
    Args:
        model: Trained XGBoost model
        output_path: Path to save ONNX model
        feature_names: Feature names (optional)
        n_features: Number of features if feature_names not provided
    
    Returns:
        Path to exported ONNX model or None if failed
    """
    try:
        from onnxmltools.convert import convert_xgboost
        from onnxmltools.convert.common.data_types import FloatTensorType
        import onnx
        
        n_features = len(feature_names) if feature_names else n_features
        initial_type = [('float_input', FloatTensorType([None, n_features]))]
        
        onnx_model = convert_xgboost(model.model, initial_types=initial_type)
        onnx.save(onnx_model, str(output_path))
        print(f"   ✓ Exported model to {output_path}")
        
        return output_path
        
    except ImportError as e:
        print(f"⚠️ ONNX export failed: {e}")
        return None
