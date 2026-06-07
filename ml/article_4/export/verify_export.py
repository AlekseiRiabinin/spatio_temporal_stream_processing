"""
Verification utilities for exported ONNX models.
"""

import numpy as np
from pathlib import Path
from typing import Tuple, Dict, Any


def verify_onnx_export(
    original_model,
    onnx_path: Path,
    X_test: np.ndarray,
    rtol: float = 1e-5,
    atol: float = 1e-7,
) -> Dict[str, Any]:
    """
    Verify that ONNX export produces same outputs as original model.
    
    Args:
        original_model: Original XGBoost model
        onnx_path: Path to exported ONNX model
        X_test: Test features
        rtol: Relative tolerance
        atol: Absolute tolerance
    
    Returns:
        Dictionary with verification results
    """
    try:
        import onnxruntime as ort
        
        # Get original predictions
        original_predictions = original_model.predict(X_test)
        
        # Load ONNX model and run inference
        sess = ort.InferenceSession(str(onnx_path))
        input_name = sess.get_inputs()[0].name
        onnx_predictions = sess.run(None, {input_name: X_test.astype(np.float32)})[0]
        
        # Compare predictions
        if original_predictions.ndim == 1:
            original_predictions = original_predictions.reshape(-1, 1)
        
        if onnx_predictions.ndim == 1:
            onnx_predictions = onnx_predictions.reshape(-1, 1)
        
        # Calculate differences
        diff = np.abs(original_predictions - onnx_predictions)
        max_diff = np.max(diff)
        mean_diff = np.mean(diff)
        std_diff = np.std(diff)
        
        # Check if within tolerance
        is_close = np.allclose(original_predictions, onnx_predictions, rtol=rtol, atol=atol)
        
        result = {
            'verified': is_close,
            'max_difference': max_diff,
            'mean_difference': mean_diff,
            'std_difference': std_diff,
            'original_shape': original_predictions.shape,
            'onnx_shape': onnx_predictions.shape,
            'onnx_path': str(onnx_path),
        }
        
        if is_close:
            print(f"   ✓ ONNX export verified: max_diff={max_diff:.2e}")
        else:
            print(f"   ✗ ONNX export mismatch: max_diff={max_diff:.2e}")
        
        return result
        
    except ImportError as e:
        print(f"⚠️ Verification failed: {e}")
        print("   Install: pip install onnxruntime")
        return {'verified': False, 'error': str(e)}


def verify_dual_export(
    models,
    onnx_window_path: Path,
    onnx_watermark_path: Path,
    X_test: np.ndarray,
    rtol: float = 1e-5,
) -> Tuple[Dict, Dict]:
    """
    Verify both ONNX exports for dual model system.
    
    Args:
        models: Trained dual model container
        onnx_window_path: Path to window ONNX model
        onnx_watermark_path: Path to watermark ONNX model
        X_test: Test features
        rtol: Relative tolerance
    
    Returns:
        Verification results for both models
    """
    print("\n📋 Verifying ONNX exports...")
    print("-" * 40)
    
    window_result = verify_onnx_export(
        models.model_a, onnx_window_path, X_test, rtol=rtol
    )
    
    watermark_result = verify_onnx_export(
        models.model_b, onnx_watermark_path, X_test, rtol=rtol
    )
    
    print("\n" + "=" * 40)
    if window_result['verified'] and watermark_result['verified']:
        print("✅ Both ONNX exports verified successfully!")
    else:
        print("⚠️ Some ONNX exports failed verification")
    
    return window_result, watermark_result
