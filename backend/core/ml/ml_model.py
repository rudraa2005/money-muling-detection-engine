"""
ML Risk Model — Logistic Regression Wrapper.

Provides train, save/load, predict, and evaluate capabilities for a
binary Logistic Regression classifier that outputs probability scores [0, 1].

Replaces XGBoost for significantly faster inference and better transparency.

Time Complexity:
    train: O(n_samples × n_features)
    predict: O(n_samples × n_features)  (~<0.1ms for typical batch)
Memory: O(n_features)
"""

import json
import logging
import os
import pickle
from typing import Any, Dict, Optional, Tuple

import numpy as np

try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        accuracy_score,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )
    from sklearn.preprocessing import StandardScaler

    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False

try:
    from xgboost import XGBClassifier

    _XGBOOST_AVAILABLE = True
except ImportError:
    _XGBOOST_AVAILABLE = False

logger = logging.getLogger(__name__)

# Default model hyperparameters by estimator family.
_DEFAULT_LOGISTIC_PARAMS: Dict[str, Any] = {
    "C": 1.0,
    "solver": "lbfgs",
    "max_iter": 2000,
    "random_state": 42,
    "class_weight": "balanced",
}

_DEFAULT_XGB_PARAMS: Dict[str, Any] = {
    "n_estimators": 300,
    "max_depth": 5,
    "learning_rate": 0.05,
    "subsample": 0.85,
    "colsample_bytree": 0.85,
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "random_state": 42,
    "n_jobs": 4,
}


class RiskModel:
    """
    Account-level risk scoring model with built-in feature scaling.
    """

    def __init__(
        self,
        params: Optional[Dict[str, Any]] = None,
        model_type: str = "logistic_regression",
    ):
        """Initialize with optional hyperparameter overrides."""
        if not _ML_AVAILABLE:
            logger.warning("scikit-learn not installed. ML scoring unavailable.")
            self._model = None
            return

        self._model_type = str(model_type).strip().lower()
        if self._model_type in {"logistic", "logistic_regression", "logreg"}:
            self._model_type = "logistic_regression"
            defaults = _DEFAULT_LOGISTIC_PARAMS
        elif self._model_type in {"xgboost", "xgb"}:
            if not _XGBOOST_AVAILABLE:
                raise RuntimeError("xgboost is not installed")
            self._model_type = "xgboost"
            defaults = _DEFAULT_XGB_PARAMS
        else:
            raise ValueError(f"Unsupported model_type: {model_type}")

        self._params = {**defaults, **(params or {})}
        self._model: Optional[LogisticRegression] = None
        self._scaler = StandardScaler()
        self._metadata: Dict[str, Any] = {}

    @property
    def is_available(self) -> bool:
        return _ML_AVAILABLE

    @property
    def is_trained(self) -> bool:
        return self._model is not None

    @property
    def metadata(self) -> Dict[str, Any]:
        return dict(self._metadata)

    def train(
        self,
        X: np.ndarray,
        y: np.ndarray,
        eval_set: Optional[list] = None,
    ) -> "RiskModel":
        """Train the classifier with automatic feature scaling."""
        if not _ML_AVAILABLE:
            raise RuntimeError("scikit-learn not installed")

        # Scale features
        X_scaled = self._scaler.fit_transform(X)

        if self._model_type == "xgboost":
            self._model = XGBClassifier(**self._params)
        else:
            self._model = LogisticRegression(**self._params)
        self._model.fit(X_scaled, y)
        
        n_pos = np.sum(y == 1)
        self._metadata.update(
            {
                "model_type": self._model_type,
                "n_features": int(X.shape[1]),
                "n_samples": int(len(y)),
                "positive_samples": int(n_pos),
                "negative_samples": int(len(y) - n_pos),
            }
        )
        logger.info("Model trained on %d samples (%d positive)", len(y), int(n_pos))
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict probability scores with scaled features."""
        if self._model is None:
            # No trained model loaded: force caller to fall back to rule engine
            logger.error("RiskModel.predict called without a trained model; returning zeros.")
            return np.zeros(X.shape[0], dtype=float)

        X = np.asarray(X, dtype=np.float32)
        expected_features = getattr(self._scaler, "n_features_in_", None) or getattr(self._model, "n_features_in_", None)
        if expected_features is not None and X.shape[1] != int(expected_features):
            target = int(expected_features)
            if X.shape[1] > target:
                logger.warning("Feature dimension mismatch (got=%d expected=%d). Truncating extra features.", X.shape[1], target)
                X = X[:, :target]
            else:
                logger.warning("Feature dimension mismatch (got=%d expected=%d). Zero-padding missing features.", X.shape[1], target)
                pad = np.zeros((X.shape[0], target - X.shape[1]), dtype=X.dtype)
                X = np.hstack([X, pad])

        X_scaled = self._scaler.transform(X)
        return self._model.predict_proba(X_scaled)[:, 1]

    def save(
        self,
        directory: str,
        version: int = 1,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Save model and scaler to a versioned pickle file."""
        if self._model is None:
            raise RuntimeError("No model to save — train first")

        os.makedirs(directory, exist_ok=True)
        filename = f"risk_model_v{version}.pkl"
        path = os.path.join(directory, filename)
        
        bundle = {
            "model": self._model,
            "scaler": self._scaler,
            "version": version,
            "model_type": self._model_type,
        }
        
        with open(path, "wb") as f:
            pickle.dump(bundle, f)

        # Save metadata alongside
        meta_path = os.path.join(directory, f"risk_model_v{version}_meta.json")
        meta = {
            "version": version,
            "model_type": self._model_type,
            "params": self._params,
            "format": "risk_model_bundle_v2",
        }
        meta.update(self._metadata)
        if extra_metadata:
            meta.update(extra_metadata)
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

        logger.info("Model bundle saved to %s", path)
        return path

    def load(self, path: str) -> "RiskModel":
        """Load a previously saved model bundle."""
        if not _ML_AVAILABLE:
            raise RuntimeError("scikit-learn not installed")

        if not os.path.exists(path):
            logger.warning("Model bundle not found: %s.", path)
            return self

        try:
            with open(path, "rb") as f:
                bundle = pickle.load(f)
            
            if isinstance(bundle, dict) and "model" in bundle:
                self._model = bundle["model"]
                self._scaler = bundle.get("scaler", StandardScaler())
                self._model_type = str(bundle.get("model_type", self._model_type))
                logger.info("Model bundle (v%s) loaded from %s", bundle.get("version"), path)
            else:
                # Direct model pickle (legacy)
                self._model = bundle
                self._scaler = StandardScaler() # Fallback
                logger.info("Legacy model loaded from %s (using default scaler)", path)
                
        except Exception as e:
            logger.warning("Failed to load pickle model: %s. Trying XGBoost JSON fallback...", str(e))
            json_path = path.replace(".pkl", ".json")
            if os.path.exists(json_path):
                try:
                    import xgboost as xgb
                    self._model = xgb.XGBClassifier()
                    self._model.load_model(json_path)
                    self._scaler = StandardScaler() # XGBoost JSON doesn't store scaler, using identity fallback
                    logger.info("XGBoost JSON model loaded from %s", json_path)
                except Exception as ex:
                    logger.error("XGBoost JSON fallback failed: %s", str(ex))
                    self._model = None
            else:
                self._model = None
                self._scaler = StandardScaler()

        meta_path = path.replace(".pkl", "_meta.json")
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    self._metadata = json.load(f)
                self._model_type = str(self._metadata.get("model_type", self._model_type))
            except Exception as e:
                logger.warning("Failed to load model metadata: %s", e)
                self._metadata = {}
        
        return self

    def evaluate(
        self,
        X: np.ndarray,
        y: np.ndarray,
    ) -> Dict[str, Any]:
        """
        Evaluate model performance.

        Args:
            X: Feature matrix
            y: True binary labels

        Returns:
            Dict with accuracy, precision, recall, f1, roc_auc, and confusion_matrix.
        """
        if self._model is None:
            raise RuntimeError("Model not trained or loaded")

        probs = self.predict(X)
        preds = (probs >= 0.5).astype(int)

        try:
            auc = roc_auc_score(y, probs)
        except ValueError:
            auc = 0.0

        cm = confusion_matrix(y, preds)

        return {
            "accuracy": round(float(accuracy_score(y, preds)), 4),
            "precision": round(float(precision_score(y, preds, zero_division=0)), 4),
            "recall": round(float(recall_score(y, preds, zero_division=0)), 4),
            "f1": round(float(f1_score(y, preds, zero_division=0)), 4),
            "roc_auc": round(auc, 4),
            "confusion_matrix": cm.tolist(),
        }

    def get_feature_importance(self) -> Optional[np.ndarray]:
        """Return feature importance (coefficients) if model is trained."""
        if self._model is None:
            return None
        return self._model.coef_[0]
