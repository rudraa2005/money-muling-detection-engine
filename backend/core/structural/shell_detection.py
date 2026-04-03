"""
Shell Chain Detection Module.

Identifies shell accounts (low-activity intermediaries) and finds chains.

Time Complexity: O(V × 3^D) bounded by shell degree ≤ 3, D = max depth (capped at 8)
Memory: O(V + chains × chain_length)
"""

import logging
import time
from typing import Any, Dict, List, Set, Tuple

import networkx as nx
import pandas as pd

logger = logging.getLogger(__name__)

from app.config import (
    SHELL_HOLDING_TIME_HOURS,
    SHELL_MAX_DEGREE,
    SHELL_MAX_TRANSACTIONS,
    SHELL_MIN_CHAIN_LENGTH,
)


def _identify_shell_accounts(G: nx.MultiDiGraph, df: pd.DataFrame) -> Set[str]:
    """Identify shell accounts using vectorized grouping for O(N) speed."""
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    # Pre-calculate node-level stats
    # 1. Transaction counts
    in_counts = df.groupby("receiver_id").size()
    out_counts = df.groupby("sender_id").size()
    
    # 2. Holding times (min timestamp per account as sender vs receiver)
    first_in = df.groupby("receiver_id")["timestamp"].min()
    first_out = df.groupby("sender_id")["timestamp"].min()

    shell_accounts: Set[str] = set()

    # Pre-calculate degrees as dicts
    in_degrees = dict(G.in_degree())
    out_degrees = dict(G.out_degree())

    for node in G.nodes():
        node_str = str(node)
        
        # Static graph check
        n_in_deg = in_degrees.get(node, 0)
        n_out_deg = out_degrees.get(node, 0)
        total_degree = n_in_deg + n_out_deg
        
        if total_degree > SHELL_MAX_DEGREE or total_degree == 0:
            continue

        # Must have BOTH incoming and outgoing edges (pass-through behavior)
        if n_in_deg == 0 or n_out_deg == 0:
            continue

        # Transaction count check
        n_in = in_counts.get(node_str, 0)
        n_out = out_counts.get(node_str, 0)
        if (n_in + n_out) > SHELL_MAX_TRANSACTIONS:
            continue

        # Must have both incoming and outgoing transactions
        if n_in == 0 or n_out == 0:
            continue

        # Holding time check
        t_in = first_in.get(node_str)
        t_out = first_out.get(node_str)
        if pd.notna(t_in) and pd.notna(t_out):
            holding_hours = (t_out - t_in).total_seconds() / 3600
            if holding_hours > SHELL_HOLDING_TIME_HOURS:
                continue

        shell_accounts.add(node_str)

    return shell_accounts


def _find_shell_chains(
    G: nx.MultiDiGraph, shell_accounts: Set[str]
) -> List[List[str]]:
    """DFS to find chains ≥ SHELL_MIN_CHAIN_LENGTH where all intermediates are shell."""
    if not shell_accounts:
        return []
        
    simple_G = nx.DiGraph(G)
    chains: List[List[str]] = []
    visited_chains: Set[tuple] = set()

    # Search only from nodes that are likely to be part of a chain
    start_nodes = [node for node in simple_G.nodes() if str(node) in shell_accounts or any(str(nbr) in shell_accounts for nbr in simple_G.successors(node))]

    for start_node in start_nodes:
        stack = [(start_node, [start_node])]

        while stack:
            current, path = stack.pop()

            for neighbor in simple_G.successors(current):
                if neighbor in path:
                    continue

                new_path = path + [neighbor]
                intermediates = new_path[1:-1]

                if intermediates and all(n in shell_accounts for n in intermediates):
                    if len(new_path) >= SHELL_MIN_CHAIN_LENGTH:
                        chain_key = tuple(new_path)
                        if chain_key not in visited_chains:
                            visited_chains.add(chain_key)
                            chains.append(new_path)

                if neighbor in shell_accounts and len(new_path) < 8:
                    stack.append((neighbor, new_path))

    return chains


def detect_shell_chains(
    G: nx.MultiDiGraph, df: pd.DataFrame, exclude_nodes: Set[str] | None = None
) -> Tuple[List[Dict[str, Any]], Set[str]]:
    """
    Detect shell chain patterns with tightened constraints and deduplication.
    """
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    shell_accounts = _identify_shell_accounts(G, df)
    if exclude_nodes:
        shell_accounts = shell_accounts - exclude_nodes
    chains = _find_shell_chains(G, shell_accounts)

    # 1. PRE-FILTER CHAINS: Apply minimum flow and velocity constraints
    filtered_chains = []
    for chain in chains:
        # Sum total amount flowing through the chain
        chain_txns = df[(df["sender_id"].isin(chain)) & (df["receiver_id"].isin(chain))]
        if chain_txns.empty:
            continue
        total_flow = chain_txns["amount"].sum()
        
        # Velocity check: transaction frequency
        time_span = 0.0
        if len(chain_txns) > 1:
            time_span = (
                chain_txns["timestamp"].max() - chain_txns["timestamp"].min()
            ).total_seconds() / 3600
        velocity = len(chain_txns) / max(1, time_span)
        
        # Thresholds: Min flow 1000, Min velocity 0.5 tx/hr (if span > 0)
        if total_flow >= 1000 or (len(chain_txns) >= 3 and velocity >= 0.5):
            filtered_chains.append(chain)

    # Convert chains to sets for merging
    raw_rings = []
    for chain in filtered_chains:
        raw_rings.append(set(chain))

    # Jaccard-based merging (60% intersection)
    merged_sets = []
    for r_set in raw_rings:
        is_merged = False
        for i, m_set in enumerate(merged_sets):
            intersection = r_set & m_set
            if len(intersection) / min(len(r_set), len(m_set)) >= 0.6:
                merged_sets[i] = m_set | r_set
                is_merged = True
                break
        if not is_merged:
            merged_sets.append(r_set)

    rings: List[Dict[str, Any]] = []
    total_shell_members: Set[str] = set()
    for i, m_set in enumerate(merged_sets, 1):
        members = sorted(list(m_set))
        total_shell_members.update(members)
        
        member_patterns = {
            str(m): ["shell_chain_participant", "flow_chain_member"] 
            for m in members
        }
        
        rings.append(
            {
                "ring_id": f"RING_SHELL_{i:03d}",
                "members": members,
                "member_patterns": member_patterns,
                "pattern_type": "shell_chain",
                "risk_score": float(min(100, 30 + len(members) * 8)),
            }
        )

    return rings, total_shell_members

