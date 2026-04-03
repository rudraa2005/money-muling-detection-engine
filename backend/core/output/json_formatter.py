"""
JSON Output Formatter.

Produces the strict required output structure:
{
    "suspicious_accounts": [...],
    "fraud_rings": [...],
    "summary": {...},
    "graph_data": {...}
}

Pattern names are mapped to competition-required format.

Time Complexity: O(V log V) for sorting
Memory: O(V + R)
"""

from typing import Any, Dict, List, Optional


def _build_account_ring_map(rings: List[Dict[str, Any]]) -> Dict[str, str]:
    """Map each account to its primary (first-encountered) ring_id."""
    account_ring: Dict[str, str] = {}
    for ring in rings:
        for member in ring["members"]:
            if member not in account_ring:
                account_ring[member] = ring["ring_id"]
    return account_ring


# Patterns that are internal markers, not user-facing
_INTERNAL_PATTERNS = {"merchant_like", "payroll_like", "multi_pattern", "nonlinear_amplifier"}

# Map internal pattern names → competition-required names
_PATTERN_NAME_MAP = {
    "cycle": "cycle_length_3",
    "smurfing_aggregator": "fan_in",
    "smurfing_disperser": "fan_out",
    "shell_account": "shell_chain",
    "high_velocity": "high_velocity",
    "rapid_pass_through": "rapid_forwarding",
    "sudden_activity_spike": "activity_spike",
    "high_betweenness_centrality": "high_centrality",
    "low_retention_pass_through": "low_retention",
    "high_throughput_ratio": "high_throughput",
    "balance_oscillation_pass_through": "balance_oscillation",
    "high_burst_diversity": "burst_diversity",
    "large_scc_membership": "scc_cluster",
    "deep_layered_cascade": "layered_cascade",
    "irregular_activity_spike": "irregular_activity",
    "high_closeness_centrality": "high_closeness",
    "high_local_clustering": "high_clustering",
    "rapid_forwarding": "rapid_forwarding",
    "dormant_activation_spike": "dormant_activation",
    "structured_fragmentation": "structured_fragmentation",
}


def _map_pattern_name(internal_name: str) -> str:
    """Map an internal pattern name to the competition-required name."""
    if internal_name.startswith("cycle_length_"):
        return internal_name  # Already in correct format
    return _PATTERN_NAME_MAP.get(internal_name, internal_name)


def format_output(
    scores: Dict[str, Dict[str, Any]],
    all_rings: List[Dict[str, Any]],
    total_accounts: int,
    graph_data: Optional[Dict[str, Any]] = None,
    min_suspicion_score: float = 5.0,
) -> Dict[str, Any]:
    """Build the final JSON-compatible output dict."""
    account_ring_map = _build_account_ring_map(all_rings)
    min_suspicion_score = max(0.0, min(100.0, float(min_suspicion_score)))

    suspicious_accounts: List[Dict[str, Any]] = []
    for account_id, data in scores.items():
        detected_patterns = list(dict.fromkeys(
            _map_pattern_name(p) for p in data["patterns"] if p not in _INTERNAL_PATTERNS
        ))

        if not detected_patterns:
            continue

        if "final_risk_score" in data:
            display_score = round(float(data["final_risk_score"]) * 100, 2)
        else:
            display_score = round(float(data["score"]), 2)

        display_score = max(0.0, min(100.0, display_score))
        if display_score < min_suspicion_score:
            continue

        suspicious_accounts.append(
            {
                "account_id": str(account_id),
                "suspicion_score": display_score,
                "detected_patterns": detected_patterns,
                "ring_id": account_ring_map.get(account_id, "RING_NONE"),
            }
        )

    suspicious_accounts.sort(key=lambda x: x["suspicion_score"], reverse=True)

    fraud_rings: List[Dict[str, Any]] = []
    pattern_type_map = {
        "smurfing_fan_in": "fan_in",
        "smurfing_fan_out": "fan_out",
        "cycle": "cycle",
        "shell_chain": "shell_chain",
    }
    
    for ring in all_rings:
        pattern_type = ring.get("pattern_type", "unknown")
        if pattern_type.startswith("cycle_length_"):
            pattern_type = "cycle"
        elif pattern_type in pattern_type_map:
            pattern_type = pattern_type_map[pattern_type]
        elif pattern_type.startswith("smurfing"):
            pattern_type = "fan_in" if "fan_in" in pattern_type else "fan_out"
        
        ring_obj = {
            "ring_id": ring["ring_id"],
            "member_accounts": ring["members"],
            "pattern_type": pattern_type,
            "risk_score": round(float(ring["risk_score"]), 2),
        }
        if "density_score" in ring:
            ring_obj["density_score"] = round(float(ring["density_score"]), 2)
        fraud_rings.append(ring_obj)

    fraud_rings.sort(key=lambda x: x["risk_score"], reverse=True)

    summary = {
        "total_accounts_analyzed": total_accounts,
        "suspicious_accounts_flagged": len(suspicious_accounts),
        "fraud_rings_detected": len(fraud_rings),
        "processing_time_seconds": 0.0,
    }

    result: Dict[str, Any] = {
        "suspicious_accounts": suspicious_accounts,
        "fraud_rings": fraud_rings,
        "summary": summary,
    }

    # For UI use: add graph_data and detailed fields only if requested or as extra
    if graph_data:
        result["graph_data"] = graph_data
        # Add back breakdown for the UI popups
        for acc in suspicious_accounts:
            acc_id = acc["account_id"]
            if acc_id in scores:
                acc["risk_timeline"] = scores[acc_id].get("timeline", [])
                acc["score_breakdown"] = scores[acc_id].get("breakdown", {})

    return result
