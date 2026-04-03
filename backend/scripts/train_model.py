"""
Train the production account-level risk model on labeled CSVs.

Default training sources:
  - backend/data/financial_transactions_3000_A.csv
  - backend/data/financial_transactions_3000_B.csv

The script:
  1. Builds account-level features from the production detectors.
  2. Trains an XGBoost classifier.
  3. Selects a validation threshold that maximizes F1.
  4. Saves the model bundle plus evaluation metadata.
  5. Benchmarks the runtime pipeline in rule-only and hybrid modes.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split

SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = SCRIPT_DIR.parent
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from core.ml.ml_model import RiskModel
from core.ml.training_data import combine_labeled_account_datasets, extract_positive_accounts
from services.processing_pipeline import ProcessingService
import services.processing_pipeline as runtime_pipeline

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_CSVS = [
    BACKEND_ROOT / "data" / "financial_transactions_3000_A.csv",
    BACKEND_ROOT / "data" / "financial_transactions_3000_B.csv",
]
DEFAULT_LABEL_COLUMN = "is_fraud"
DEFAULT_MODEL_DIR = BACKEND_ROOT / "core" / "ml" / "models"


def _metric_dict(y_true: np.ndarray, probs: np.ndarray, threshold: float) -> Dict[str, float]:
    preds = (probs >= threshold).astype(int)
    try:
        auc = float(roc_auc_score(y_true, probs))
    except ValueError:
        auc = 0.0

    return {
        "accuracy": float(accuracy_score(y_true, preds)),
        "precision": float(precision_score(y_true, preds, zero_division=0)),
        "recall": float(recall_score(y_true, preds, zero_division=0)),
        "f1": float(f1_score(y_true, preds, zero_division=0)),
        "roc_auc": auc,
    }


def _select_threshold(y_true: np.ndarray, probs: np.ndarray) -> tuple[float, Dict[str, float]]:
    best_threshold = 0.5
    best_metrics = _metric_dict(y_true, probs, best_threshold)
    best_key = (best_metrics["f1"], best_metrics["precision"], best_metrics["accuracy"])

    for threshold in np.linspace(0.2, 0.8, 61):
        metrics = _metric_dict(y_true, probs, float(threshold))
        key = (metrics["f1"], metrics["precision"], metrics["accuracy"])
        if key > best_key:
            best_threshold = float(threshold)
            best_metrics = metrics
            best_key = key

    return best_threshold, best_metrics


def _evaluate_runtime_pipeline(
    runtime_results: Sequence[tuple[list[dict], set[str], set[str]]],
    score_threshold: float = 0.0,
) -> Dict[str, float]:
    tp = fp = fn = tn = 0

    for suspicious_accounts, all_accounts, positives in runtime_results:
        predicted = {
            str(item["account_id"])
            for item in suspicious_accounts
            if item.get("account_id") is not None
            and float(item.get("suspicion_score", 0.0)) >= score_threshold
        }

        tp += len(predicted & positives)
        fp += len(predicted - positives)
        fn += len(positives - predicted)
        tn += len(all_accounts - (predicted | positives))

    total = tp + fp + fn + tn
    precision = (tp / (tp + fp)) if (tp + fp) else 0.0
    recall = (tp / (tp + fn)) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    accuracy = ((tp + tn) / total) if total else 0.0
    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def _collect_runtime_results(
    csv_paths: Sequence[Path],
    label_column: str,
    use_ml: bool,
) -> list[tuple[list[dict], set[str], set[str]]]:
    old_enabled = runtime_pipeline.ML_ENABLED
    old_cached_model = runtime_pipeline._CACHED_MODEL
    old_cached_path = runtime_pipeline._CACHED_MODEL_PATH

    runtime_pipeline.ML_ENABLED = use_ml
    runtime_pipeline._CACHED_MODEL = None
    runtime_pipeline._CACHED_MODEL_PATH = None

    collected: list[tuple[list[dict], set[str], set[str]]] = []
    service = ProcessingService()
    try:
        for csv_path in csv_paths:
            df = pd.read_csv(csv_path)
            positives = extract_positive_accounts(df, label_column=label_column)
            all_accounts = {
                str(acct)
                for acct in pd.concat(
                    [df["sender_id"].astype(str), df["receiver_id"].astype(str)],
                    ignore_index=True,
                ).unique()
            }
            result = service.process(df)
            suspicious_accounts = list(result.get("suspicious_accounts", []))
            collected.append((suspicious_accounts, all_accounts, positives))
    finally:
        runtime_pipeline.ML_ENABLED = old_enabled
        runtime_pipeline._CACHED_MODEL = old_cached_model
        runtime_pipeline._CACHED_MODEL_PATH = old_cached_path

    return collected


def _select_runtime_output_threshold(
    runtime_results: Sequence[tuple[list[dict], set[str], set[str]]],
) -> tuple[float, Dict[str, float]]:
    best_threshold = 50.0
    best_metrics = _evaluate_runtime_pipeline(runtime_results, score_threshold=best_threshold)
    best_key = (best_metrics["f1"], best_metrics["accuracy"], best_metrics["precision"])

    for threshold in range(50, 101):
        metrics = _evaluate_runtime_pipeline(runtime_results, score_threshold=float(threshold))
        key = (metrics["f1"], metrics["accuracy"], metrics["precision"])
        if key > best_key:
            best_threshold = float(threshold)
            best_metrics = metrics
            best_key = key

    return best_threshold, best_metrics


def _resolve_csvs(paths: Iterable[str]) -> list[Path]:
    resolved: list[Path] = []
    for value in paths:
        candidate = Path(value)
        if not candidate.is_absolute():
            candidate = (REPO_ROOT / value).resolve()
        resolved.append(candidate)
    return resolved


def main() -> int:
    parser = argparse.ArgumentParser(description="Train the production account-level risk model.")
    parser.add_argument(
        "--csv",
        action="append",
        default=[str(path.relative_to(REPO_ROOT)) for path in DEFAULT_CSVS],
        help="Labeled CSV path. Repeat for multiple files.",
    )
    parser.add_argument(
        "--label-column",
        default=DEFAULT_LABEL_COLUMN,
        help="Transaction label column name. Default: is_fraud",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=1,
        help="Model version number for the saved bundle. Default: 1",
    )
    args = parser.parse_args()

    csv_paths = _resolve_csvs(args.csv)
    for path in csv_paths:
        if not path.exists():
            raise FileNotFoundError(f"Training CSV not found: {path}")

    logger.info("Building labeled account dataset from %d CSV files...", len(csv_paths))
    X, y, account_rows = combine_labeled_account_datasets(
        [str(path) for path in csv_paths],
        label_column=args.label_column,
    )
    logger.info(
        "Training matrix ready: samples=%d features=%d positives=%d negatives=%d",
        X.shape[0],
        X.shape[1],
        int(y.sum()),
        int(len(y) - y.sum()),
    )

    X_train_full, X_test, y_train_full, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )
    X_train, X_val, y_train, y_val = train_test_split(
        X_train_full,
        y_train_full,
        test_size=0.2,
        random_state=42,
        stratify=y_train_full,
    )

    scale_pos_weight = float((len(y_train) - y_train.sum()) / max(y_train.sum(), 1))
    model = RiskModel(
        model_type="xgboost",
        params={
            "n_estimators": 320,
            "max_depth": 5,
            "learning_rate": 0.05,
            "subsample": 0.85,
            "colsample_bytree": 0.85,
            "min_child_weight": 2,
            "reg_alpha": 0.05,
            "reg_lambda": 2.0,
            "tree_method": "hist",
            "scale_pos_weight": scale_pos_weight,
        },
    )
    model.train(X_train, y_train)

    val_probs = model.predict(X_val)
    decision_threshold, val_metrics = _select_threshold(y_val, val_probs)
    test_probs = model.predict(X_test)
    test_metrics = _metric_dict(y_test, test_probs, decision_threshold)

    logger.info(
        "Validation threshold selected at %.2f | val_f1=%.4f val_precision=%.4f",
        decision_threshold,
        val_metrics["f1"],
        val_metrics["precision"],
    )
    logger.info(
        "Held-out test metrics | acc=%.4f prec=%.4f rec=%.4f f1=%.4f auc=%.4f",
        test_metrics["accuracy"],
        test_metrics["precision"],
        test_metrics["recall"],
        test_metrics["f1"],
        test_metrics["roc_auc"],
    )

    model_dir = DEFAULT_MODEL_DIR
    model_dir.mkdir(parents=True, exist_ok=True)

    metadata: Dict[str, object] = {
        "accuracy": round(test_metrics["accuracy"] * 100, 2),
        "precision": round(test_metrics["precision"] * 100, 2),
        "recall": round(test_metrics["recall"] * 100, 2),
        "f1": round(test_metrics["f1"] * 100, 2),
        "roc_auc": round(test_metrics["roc_auc"] * 100, 2),
        "decision_threshold": round(float(decision_threshold), 4),
        "training_datasets": [str(path.relative_to(REPO_ROOT)) for path in csv_paths],
        "label_column": args.label_column,
        "validation_accuracy": round(val_metrics["accuracy"] * 100, 2),
        "validation_precision": round(val_metrics["precision"] * 100, 2),
        "validation_recall": round(val_metrics["recall"] * 100, 2),
        "validation_f1": round(val_metrics["f1"] * 100, 2),
        "accounts_seen": int(len(account_rows)),
    }

    output_path = model.save(str(model_dir), version=args.version, extra_metadata=metadata)

    logger.info("Evaluating runtime pipeline with the new model bundle...")
    hybrid_results = _collect_runtime_results(csv_paths, args.label_column, use_ml=True)
    output_score_threshold, hybrid_metrics = _select_runtime_output_threshold(hybrid_results)
    logger.info(
        "Runtime output threshold selected at %.0f | acc=%.4f prec=%.4f rec=%.4f f1=%.4f",
        output_score_threshold,
        hybrid_metrics["accuracy"],
        hybrid_metrics["precision"],
        hybrid_metrics["recall"],
        hybrid_metrics["f1"],
    )

    rule_results = _collect_runtime_results(csv_paths, args.label_column, use_ml=False)
    rule_output_threshold, rule_metrics = _select_runtime_output_threshold(rule_results)

    metadata.update(
        {
            "output_score_threshold": round(output_score_threshold, 2),
            "rule_output_score_threshold": round(rule_output_threshold, 2),
            "hybrid_accuracy": round(hybrid_metrics["accuracy"] * 100, 2),
            "hybrid_precision": round(hybrid_metrics["precision"] * 100, 2),
            "hybrid_recall": round(hybrid_metrics["recall"] * 100, 2),
            "hybrid_f1": round(hybrid_metrics["f1"] * 100, 2),
            "rule_based_accuracy": round(rule_metrics["accuracy"] * 100, 2),
            "rule_based_precision": round(rule_metrics["precision"] * 100, 2),
            "rule_based_recall": round(rule_metrics["recall"] * 100, 2),
            "rule_based_f1": round(rule_metrics["f1"] * 100, 2),
            "total_accuracy": round(hybrid_metrics["accuracy"] * 100, 2),
        }
    )
    model.save(str(model_dir), version=args.version, extra_metadata=metadata)

    print("\n" + "=" * 72)
    print("Saved model bundle:", output_path)
    print(f"Decision threshold: {decision_threshold:.2f}")
    print(f"Output score threshold: {output_score_threshold:.0f}")
    print(
        "Held-out classifier metrics:",
        f"acc={test_metrics['accuracy']:.4f}",
        f"prec={test_metrics['precision']:.4f}",
        f"rec={test_metrics['recall']:.4f}",
        f"f1={test_metrics['f1']:.4f}",
        f"auc={test_metrics['roc_auc']:.4f}",
    )
    print(
        "Runtime rule-only metrics:",
        f"acc={rule_metrics['accuracy']:.4f}",
        f"prec={rule_metrics['precision']:.4f}",
        f"rec={rule_metrics['recall']:.4f}",
        f"f1={rule_metrics['f1']:.4f}",
    )
    print(
        "Runtime hybrid metrics:",
        f"acc={hybrid_metrics['accuracy']:.4f}",
        f"prec={hybrid_metrics['precision']:.4f}",
        f"rec={hybrid_metrics['recall']:.4f}",
        f"f1={hybrid_metrics['f1']:.4f}",
    )
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
