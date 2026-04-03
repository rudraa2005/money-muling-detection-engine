"""
Utilities for building labeled account-level ML datasets.

These helpers reuse the production detection modules so the offline model sees
the same feature space and structural signals as runtime inference.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Sequence, Tuple

import networkx as nx
import numpy as np
import pandas as pd

from core.amount_structuring import detect_amount_structuring
from core.centrality.betweenness import compute_centrality
from core.centrality.closeness import compute_closeness_centrality
from core.flow.balance_oscillation import detect_balance_oscillation
from core.flow.retention_analysis import detect_low_retention
from core.flow.throughput_analysis import detect_high_throughput
from core.flow.velocity_analysis import compute_high_velocity_accounts
from core.forwarding_latency import detect_rapid_forwarding
from core.graph.graph_builder import build_graph
from core.ml.feature_vector_builder import build_feature_vectors, vectors_to_matrix
from core.ring_detection.diversity_analysis import detect_burst_diversity
from core.ring_detection.smurfing import detect_smurfing
from core.structural.cascade_depth import detect_cascade_depth
from core.structural.clustering_analysis import detect_high_clustering
from core.structural.cycle_detection import detect_cycles
from core.structural.scc_analysis import detect_scc
from core.structural.shell_detection import detect_shell_chains
from core.temporal.activity_consistency import detect_irregular_activity
from core.temporal.burst_detection import detect_activity_spikes
from core.temporal.forwarding_latency import detect_rapid_pass_through
from core.dormancy_analysis import detect_dormant_activation

REQUIRED_TX_COLUMNS = {
    "transaction_id",
    "sender_id",
    "receiver_id",
    "amount",
    "timestamp",
}


def extract_positive_accounts(
    df: pd.DataFrame,
    label_column: str = "is_fraud",
) -> set[str]:
    """Derive account-level positives from transaction-level labels."""
    if label_column not in df.columns:
        raise ValueError(f"Label column not found: {label_column}")

    labels = df[label_column]
    if pd.api.types.is_numeric_dtype(labels):
        mask = labels.fillna(0).astype(float) > 0
    else:
        normalized = labels.astype(str).str.strip().str.lower()
        mask = normalized.isin({"1", "true", "t", "yes", "y", "fraud", "suspicious"})

    fraud_tx = df[mask]
    return {
        str(acct)
        for acct in pd.concat(
            [fraud_tx["sender_id"].astype(str), fraud_tx["receiver_id"].astype(str)],
            ignore_index=True,
        ).unique()
    }


def extract_schema_signals(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """Build account-level schema features from the transaction stream."""
    df_local = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df_local["timestamp"]):
        df_local["timestamp"] = pd.to_datetime(df_local["timestamp"], errors="coerce")

    df_local["is_round"] = (df_local["amount"] % 100 == 0).astype(float)
    df_local["is_night"] = (df_local["timestamp"].dt.hour < 6).astype(float)

    q3 = df_local["amount"].quantile(0.75)
    iqr = q3 - df_local["amount"].quantile(0.25)
    outlier_thresh = q3 + 1.5 * iqr

    senders = df_local.groupby("sender_id").agg({"is_round": "max", "is_night": "max", "amount": "max"})
    receivers = df_local.groupby("receiver_id").agg({"is_round": "max", "is_night": "max", "amount": "max"})

    signals: Dict[str, Dict[str, float]] = {}
    for acct in set(senders.index) | set(receivers.index):
        sender_row = senders.loc[acct] if acct in senders.index else None
        receiver_row = receivers.loc[acct] if acct in receivers.index else None
        signals[str(acct)] = {
            "is_round_amount": max(
                float(sender_row["is_round"]) if sender_row is not None else 0.0,
                float(receiver_row["is_round"]) if receiver_row is not None else 0.0,
            ),
            "is_night_transaction": max(
                float(sender_row["is_night"]) if sender_row is not None else 0.0,
                float(receiver_row["is_night"]) if receiver_row is not None else 0.0,
            ),
            "is_high_amount_outlier": 1.0
            if max(
                float(sender_row["amount"]) if sender_row is not None else 0.0,
                float(receiver_row["amount"]) if receiver_row is not None else 0.0,
            )
            > outlier_thresh
            else 0.0,
        }
    return signals


def _prepare_transactions(df: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(REQUIRED_TX_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"Missing required transaction columns: {', '.join(missing)}")

    df_local = df.copy()
    df_local["sender_id"] = df_local["sender_id"].astype(str)
    df_local["receiver_id"] = df_local["receiver_id"].astype(str)
    df_local["amount"] = df_local["amount"].astype(float)
    df_local["timestamp"] = pd.to_datetime(df_local["timestamp"], errors="coerce")
    if df_local["timestamp"].isna().any():
        raise ValueError("Training data contains invalid timestamps.")
    return df_local


def build_labeled_account_dataset(
    df: pd.DataFrame,
    label_column: str = "is_fraud",
) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    """
    Build account-level features and labels from a labeled transaction dataframe.
    """
    df_local = _prepare_transactions(df)
    positive_accounts = extract_positive_accounts(df_local, label_column=label_column)

    G = build_graph(df_local)
    schema_signals = extract_schema_signals(df_local)
    pagerank = nx.pagerank(G.to_directed())
    local_clustering = nx.clustering(nx.Graph(G))
    structural_scores = {
        str(acct): {
            "pagerank": float(pagerank.get(acct, 0.0)),
            "local_clustering": float(local_clustering.get(acct, 0.0)),
        }
        for acct in G.nodes()
    }

    cycle_rings = detect_cycles(G, df_local)
    cycle_accounts = {str(member) for ring in cycle_rings for member in ring["members"]}
    _, aggregators, dispersers, _ = detect_smurfing(df_local)
    _, shell_accounts = detect_shell_chains(G, df_local)
    high_velocity, _ = compute_high_velocity_accounts(df_local)
    rapid_pass_through, _ = detect_rapid_pass_through(df_local)
    activity_spike, _ = detect_activity_spikes(df_local)
    high_centrality, _ = compute_centrality(G)
    low_retention = detect_low_retention(df_local)
    high_throughput = detect_high_throughput(df_local)
    balance_oscillation = detect_balance_oscillation(df_local)
    burst_diversity, _ = detect_burst_diversity(df_local)
    scc_members, _ = detect_scc(G)
    _, cascade_depth = detect_cascade_depth(G, df_local)
    irregular_activity = detect_irregular_activity(df_local)
    suspicious_seed = (
        cycle_accounts
        | {str(a) for a in aggregators}
        | {str(a) for a in dispersers}
        | {str(a) for a in shell_accounts}
    )
    high_closeness, _ = compute_closeness_centrality(G, suspicious_seed)
    high_clustering, _ = detect_high_clustering(G, suspicious_seed)
    rapid_forwarding, _ = detect_rapid_forwarding(df_local)
    dormant_activation = detect_dormant_activation(df_local)
    structured_fragmentation = detect_amount_structuring(df_local)

    all_accounts = {str(acct) for acct in G.nodes()}
    vectors, account_list = build_feature_vectors(
        all_accounts=all_accounts,
        cycle_accounts=cycle_accounts,
        aggregators={str(a) for a in aggregators},
        dispersers={str(a) for a in dispersers},
        shell_accounts={str(a) for a in shell_accounts},
        high_velocity={str(a) for a in high_velocity},
        rapid_pass_through={str(a) for a in rapid_pass_through},
        activity_spike={str(a) for a in activity_spike},
        high_centrality={str(a) for a in high_centrality},
        low_retention={str(a) for a in low_retention},
        high_throughput={str(a) for a in high_throughput},
        balance_oscillation={str(a) for a in balance_oscillation},
        burst_diversity={str(a) for a in burst_diversity},
        scc_members={str(a) for a in scc_members},
        cascade_depth={str(a) for a in cascade_depth},
        irregular_activity={str(a) for a in irregular_activity},
        high_closeness={str(a) for a in high_closeness},
        high_clustering={str(a) for a in high_clustering},
        rapid_forwarding={str(a) for a in rapid_forwarding},
        dormant_activation={str(a) for a in dormant_activation},
        structured_fragmentation={str(a) for a in structured_fragmentation},
        G=G,
        df=df_local,
        schema_signals=schema_signals,
        structural_scores=structural_scores,
    )
    X = vectors_to_matrix(vectors, account_list)
    y = np.array([1 if acct in positive_accounts else 0 for acct in account_list], dtype=np.int32)
    return X, y, account_list


def load_labeled_account_dataset(
    csv_path: str,
    label_column: str = "is_fraud",
) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    """Load a labeled CSV and build account-level features/labels."""
    df = pd.read_csv(csv_path)
    return build_labeled_account_dataset(df, label_column=label_column)


def combine_labeled_account_datasets(
    csv_paths: Sequence[str],
    label_column: str = "is_fraud",
) -> Tuple[np.ndarray, np.ndarray, List[Tuple[str, str]]]:
    """Combine multiple labeled CSVs into one account-level training matrix."""
    matrices: List[np.ndarray] = []
    labels: List[np.ndarray] = []
    account_rows: List[Tuple[str, str]] = []

    for csv_path in csv_paths:
        X, y, account_list = load_labeled_account_dataset(csv_path, label_column=label_column)
        matrices.append(X)
        labels.append(y)
        account_rows.extend((csv_path, acct) for acct in account_list)

    if not matrices:
        raise ValueError("No labeled CSV paths were provided.")

    return np.vstack(matrices), np.concatenate(labels), account_rows
