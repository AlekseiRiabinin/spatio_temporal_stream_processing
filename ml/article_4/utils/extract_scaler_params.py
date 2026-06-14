"""
Extract StandardScaler parameters from the Article 4 preprocessor
and save them as JSON for ONNX/Scala inference.

Output:
ml/article_4/models/onnx/scaler_params.json
"""

import json
import joblib
from pathlib import Path
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

PREPROCESSOR_PATH = BASE_DIR / "article_4/models/artifacts/preprocessor.joblib"
ONNX_DIR = BASE_DIR / "article_4/models/onnx"
ONNX_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_JSON = ONNX_DIR / "scaler_params.json"


# ---------------------------------------------------------
# Load preprocessor
# ---------------------------------------------------------

def load_preprocessor(path: Path):
    return joblib.load(path)


# ---------------------------------------------------------
# Extract StandardScaler from:
# ColumnTransformer["numeric"] → Pipeline["scaler"]
# ---------------------------------------------------------

def extract_numeric_scaler(preprocessor: ColumnTransformer) -> StandardScaler:
    numeric = preprocessor.named_transformers_.get("numeric")

    if numeric is None:
        raise ValueError("No transformer named 'numeric' found in ColumnTransformer.")

    if not isinstance(numeric, Pipeline):
        raise ValueError("Expected 'numeric' transformer to be a Pipeline.")

    scaler = numeric.named_steps.get("scaler")

    if not isinstance(scaler, StandardScaler):
        raise ValueError("numeric['scaler'] is not a StandardScaler.")

    return scaler


# ---------------------------------------------------------
# Extract numeric feature order
# ---------------------------------------------------------

def extract_feature_order(preprocessor: ColumnTransformer):
    for name, transformer, columns in preprocessor.transformers:
        if name == "numeric":
            return list(columns)
    raise ValueError("No 'numeric' transformer found.")


# ---------------------------------------------------------
# Save JSON
# ---------------------------------------------------------

def save_json(mean, scale, features, path: Path):
    data = {
        "numeric_features": features,
        "mean": mean.tolist(),
        "scale": scale.tolist(),
    }

    with open(path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Scaler parameters saved to: {path}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

if __name__ == "__main__":
    print(f"Loading preprocessor from: {PREPROCESSOR_PATH}")

    pre = load_preprocessor(PREPROCESSOR_PATH)
    scaler = extract_numeric_scaler(pre)
    features = extract_feature_order(pre)

    save_json(
        mean=scaler.mean_,
        scale=scaler.scale_,
        features=features,
        path=OUTPUT_JSON,
    )


# python article_4/utils/extract_scaler_params.py
