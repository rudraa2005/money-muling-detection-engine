"""
Unsupervised Anomaly Detection using Isolation Forest.

Identifies transactions that are statistical outliers based on
amount, timing, and frequency, without requiring labels.
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

def detect_anomalies(df: pd.DataFrame) -> pd.Series:
    """
    Train an Isolation Forest on the current dataset and return anomaly scores.
    
    Returns:
        pd.Series: Scores where 1.0 is highly anomalous and 0.0 is normal.
    """
    if df.empty:
        return pd.Series(dtype=float)

    df_local = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df_local["timestamp"]):
        df_local["timestamp"] = pd.to_datetime(df_local["timestamp"], errors="coerce")

    if df_local["timestamp"].isna().any():
        logger.error("Anomaly detection skipped due to invalid timestamps.")
        return pd.Series(0.0, index=df.index)

    # 1. Feature Engineering for Anomaly Detection
    # Focus on raw transactional behavior
    features = pd.DataFrame(index=df_local.index)
    features['amount'] = df_local['amount']
    
    # Hour of day (0-23)
    features['hour'] = df_local['timestamp'].dt.hour
    
    # Simple day of week (0-6)
    features['day_of_week'] = df_local['timestamp'].dt.dayofweek
    
    # Time since last txn for sender (requires sorting)
    df_sorted = df_local.sort_values(['sender_id', 'timestamp'])
    time_diff = (
        df_sorted.groupby('sender_id')['timestamp']
        .diff()
        .dt.total_seconds()
        .fillna(0)
    )
    features['time_diff'] = time_diff.reindex(df_local.index, fill_value=0)
    
    # 2. Model Training
    # contamination='auto' lets the model decide the outlier fraction
    # n_estimators=100 is usually enough for 10k rows
    model = IsolationForest(n_estimators=40, contamination=0.01, random_state=42, n_jobs=-1)
    
    # Fit and Predict
    # Isolation Forest returns -1 for outliers and 1 for inliers
    # decision_function returns raw scores (lower = more anomalous)
    try:
        model.fit(features)
        raw_scores = model.decision_function(features)
        
        # Normalize scores to [0, 1] where 1 is MOST anomalous
        # decision_function scores are typically in range [-0.5, 0.5]
        # We transform them: (max - score) / (max - min)
        min_s = raw_scores.min()
        max_s = raw_scores.max()
        
        if max_s > min_s:
            normalized = (max_s - raw_scores) / (max_s - min_s)
        else:
            normalized = np.zeros_like(raw_scores)
            
        return pd.Series(normalized, index=df_local.index)
        
    except Exception as e:
        logger.error(f"Anomaly detection failed: {e}")
        return pd.Series(0.0, index=df.index)

def aggregate_anomaly_scores(df: pd.DataFrame, anomaly_scores: pd.Series) -> Dict[str, float]:
    """
    Aggregate transactional anomaly scores to the account level.
    Uses the maximum anomaly score seen for an account to capture "worst-case" behavior.
    """
    df_with_scores = df.copy()
    df_with_scores['anomaly_score'] = anomaly_scores
    
    # Map to sender (since they initiate the anomaly)
    sender_scores = df_with_scores.groupby('sender_id')['anomaly_score'].max().to_dict()
    
    return sender_scores
