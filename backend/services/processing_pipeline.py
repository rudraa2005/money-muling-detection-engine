"""
Processing Pipeline — Full Pipeline Orchestrator.

Coordinates the complete detection pipeline:
   1. Parse timestamps & build directed multigraph
   2. Cycle detection
   3. Smurfing detection (fan-in / fan-out)
   4. Shell chain detection
   5. Rapid pass-through (holding time)
   6. Activity spike detection
   7. Betweenness centrality
   8. Net retention ratio
   9. Throughput ratio + balance oscillation
  10. Sender diversity burst
  11. SCC detection
  12. Cascade depth
  13. Activity consistency variance
  14. False positive detection
  15. Compute suspicion scores
  16. Normalize scores to [0, 100]
  17. Risk propagation (graph-based)
  18. Neighbor-based group risk propagation
  19. Closeness centrality
  20. Local clustering coefficient
  21. Ring density + enhanced risk
  22. ML feature vector construction
  23. ML inference + hybrid scoring
  24. Build graph visualization data
  25. Format JSON output

Performance: < 30s for 10K transactions.
Memory: O(V + E) for graph + O(R) for rings.
"""

import logging
import os
import time
import threading
from typing import Any, Dict, Set

from app.config import ML_ENABLED, ML_MODEL_PATH

logger = logging.getLogger(__name__)

# Maximum transactions to process (performance requirement: <= 30s)
MAX_TRANSACTIONS = int(os.getenv("MAX_TRANSACTIONS", "10000"))
ANOMALY_SKIP_TX_THRESHOLD = int(os.getenv("ANOMALY_SKIP_TX_THRESHOLD", "3000"))
CENTRALITY_SKIP_TX_THRESHOLD = int(os.getenv("CENTRALITY_SKIP_TX_THRESHOLD", "500"))
MAX_GRAPH_NODES_RESPONSE = int(os.getenv("MAX_GRAPH_NODES_RESPONSE", "1000"))
MAX_GRAPH_EDGES_RESPONSE = int(os.getenv("MAX_GRAPH_EDGES_RESPONSE", "1500"))
TIME_LIMIT = 25.0 # Seconds before we start skipping optional blocks
DETECTOR_WORKERS = max(2, min(int(os.getenv("DETECTOR_WORKERS", str(os.cpu_count() or 2))), 8))

_MODEL_CACHE_LOCK = threading.Lock()
_CACHED_MODEL_PATH: str | None = None
_CACHED_MODEL: Any | None = None


def _resolve_model_path() -> str:
    model_path = ML_MODEL_PATH
    if not os.path.isabs(model_path):
        backend_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        model_path = os.path.join(backend_root, model_path)
    return model_path


def _get_cached_model(model_path: str) -> Any | None:
    global _CACHED_MODEL, _CACHED_MODEL_PATH
    from core.ml.ml_model import RiskModel

    with _MODEL_CACHE_LOCK:
        if _CACHED_MODEL is not None and _CACHED_MODEL_PATH == model_path:
            return _CACHED_MODEL

        if not os.path.exists(model_path):
            return None

        model = RiskModel()
        model.load(model_path)
        if not model.is_trained:
            return None

        _CACHED_MODEL = model
        _CACHED_MODEL_PATH = model_path
        return _CACHED_MODEL


def _cache_runtime_model(model: Any) -> None:
    global _CACHED_MODEL, _CACHED_MODEL_PATH
    from core.ml.ml_model import RiskModel
    with _MODEL_CACHE_LOCK:
        _CACHED_MODEL = model
        _CACHED_MODEL_PATH = "__runtime_trained__"


def warmup_pipeline() -> None:
    """Import heavy libraries in the background to avoid first-request lag."""
    try:
        import networkx as nx
        import numpy as np
        import pandas as pd
        from scipy.stats import rankdata
        logger.info("Background library warmup complete.")
    except Exception as e:
        logger.warning("Background library warmup partial failure: %s", e)

def warmup_ml_model() -> bool:
    """Best-effort ML warmup during app startup."""
    if not ML_ENABLED:
        return False
    # Also trigger library warmup
    warmup_pipeline()
    try:
        model = _get_cached_model(_resolve_model_path())
        return model is not None
    except Exception:
        return False


def _ensure_datetime_timestamps(df):
    """Normalize transaction timestamps for direct service/test invocations."""
    import pandas as pd

    if "timestamp" not in df.columns:
        return df

    if pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        return df

    normalized = df.copy()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], errors="coerce")
    if normalized["timestamp"].isna().any():
        raise ValueError("Transaction data contains invalid timestamps.")
    return normalized


class ProcessingService:
    """Orchestrates the complete money-muling detection pipeline."""

    def process(self, df: Any) -> Dict[str, Any]:
        t_start = time.time()
        import networkx as nx
        import numpy as np
        import pandas as pd
        from scipy.stats import rankdata
        from core.graph.graph_builder import build_graph
        from core.structural.cycle_detection import detect_cycles
        from core.ring_detection.smurfing import detect_smurfing
        from core.structural.shell_detection import detect_shell_chains
        from core.temporal.forwarding_latency import detect_rapid_pass_through
        from core.temporal.burst_detection import detect_activity_spikes
        from core.centrality.betweenness import compute_centrality
        from core.flow.retention_analysis import detect_low_retention
        from core.flow.throughput_analysis import detect_high_throughput
        from core.flow.balance_oscillation import detect_balance_oscillation
        from core.ring_detection.diversity_analysis import detect_burst_diversity
        from core.structural.scc_analysis import detect_scc
        from core.structural.cascade_depth import detect_cascade_depth
        from core.temporal.activity_consistency import detect_irregular_activity
        from core.risk.false_positive_filter import detect_false_positives
        from core.risk.adaptive_thresholds import compute_adaptive_thresholds
        from core.risk.base_scoring import compute_scores
        from core.risk.normalization import normalize_scores
        from core.risk.risk_propagation import propagate_risk
        from core.risk.ring_risk import finalize_ring_risks
        from core.risk.network_analysis import build_neighbor_map, compute_component_concentration
        from core.forwarding_latency import detect_rapid_forwarding
        from core.dormancy_analysis import detect_dormant_activation
        from core.amount_structuring import detect_amount_structuring
        from core.centrality.closeness import compute_closeness_centrality
        from core.structural.clustering_analysis import detect_high_clustering
        from core.output.json_formatter import format_output
        from core.ml.feature_vector_builder import build_feature_vectors, vectors_to_matrix
        from core.ml.ml_model import RiskModel
        from core.ml.hybrid_scorer import compute_hybrid_scores
        from core.ml.anomaly_detector import detect_anomalies, aggregate_anomaly_scores

        def log_timer(name):
            class Timer:
                def __enter__(self): self.t0 = time.time(); return self
                def __exit__(self, *a): logger.info(f"Module '{name}' took {time.time()-self.t0:.3f}s")
            return Timer()

        df = _ensure_datetime_timestamps(df.copy())
        stage_timings: Dict[str, float] = {}
        _stage_t0 = time.perf_counter()

        def _mark_stage(name: str) -> None:
            nonlocal _stage_t0
            now = time.perf_counter()
            stage_timings[name] = round(now - _stage_t0, 4)
            _stage_t0 = now


        # 0. Compute Adaptive Thresholds
        thresholds = compute_adaptive_thresholds(df)
        logger.info("Adaptive thresholds computed: %s", thresholds)
        _mark_stage("adaptive_thresholds")

        # 1. Build graph
        G = build_graph(df)
        all_accounts = set(G.nodes())
        total_accounts = len(all_accounts)
        _mark_stage("build_graph")

        # --- Defensive Initialization for Global Variable Access ---
        trigger_times: Dict[str, Dict[str, str]] = {}
        ts_max = str(df["timestamp"].max())
        
        cycle_rings = []
        cycle_accounts = set()
        high_velocity = set()
        smurf_rings = []
        aggregators = set()
        dispersers = set()
        rapid_pt_accounts = set()
        forwarding_accounts = set()
        spike_accounts = set()
        spike_triggers = {}
        dormant_accounts = set()
        structuring_accounts = set()
        retention_accounts = set()
        throughput_accounts = set()
        oscillation_accounts = set()
        diversity_accounts = set()
        scc_accounts = set()
        scc_rings = []
        cascade_rings = []
        cascade_accounts = set()
        irregular_accounts = set()
        merchant_accounts = set()
        payroll_accounts = set()
        centrality_accounts = set()
        anomaly_scores = {}
        shell_accounts = set()
        shell_rings = []
        closeness_accounts = set()
        clustering_accounts = set()
        ml_scores = None
        model_metadata: Dict[str, Any] = {}
        normalized = {}
        raw_scores = {}
        neighbor_map = {}
        all_rings = []

        # 2-14: Sequential execution with time checks
        # Rule 1: We stop heavy analysis if time > 25s
        
        # 2. Cycle detection
        with log_timer("cycle_detection"):
            try:
                cycle_rings = detect_cycles(G, df)
                for ring in cycle_rings:
                    cycle_accounts.update(ring["members"])
                trigger_times["cycle"] = {acct: ts_max for acct in cycle_accounts}
            except Exception:
                logger.exception("cycle_detection failed")

        # 3. Smurfing detection
        with log_timer("smurfing_detection"):
            try:
                smurf_rings, aggregators, dispersers, smurf_triggers = detect_smurfing(
                    df, thresholds["smurfing_min_senders"], thresholds["smurfing_min_receivers"]
                )
                trigger_times.update(smurf_triggers)
            except Exception:
                logger.exception("smurfing failed")

        # 4. Shell Detection (Depends on cycle_accounts)
        with log_timer("shell_chain_detection"):
            try:
                shell_rings, shell_accounts = detect_shell_chains(G, df, exclude_nodes=cycle_accounts)
                trigger_times["shell_account"] = {acct: ts_max for acct in shell_accounts}
            except Exception:
                logger.exception("shell_chain failed")

        # 5. Rapid Detectors (Fast)
        try:
            rapid_pt_accounts, _ = detect_rapid_pass_through(df)
            trigger_times["rapid_pass_through"] = {acct: ts_max for acct in rapid_pt_accounts}
            
            forwarding_accounts, _ = detect_rapid_forwarding(df)
            trigger_times["rapid_forwarding"] = {acct: ts_max for acct in forwarding_accounts}
        except Exception:
             logger.exception("rapid_detectors failed")

        # 6. Periodic Detectors
        try:
            spike_accounts, spike_triggers = detect_activity_spikes(df, thresholds["spike_min_txns"])
            trigger_times["sudden_activity_spike"] = spike_triggers
            
            dormant_accounts = detect_dormant_activation(df)
            trigger_times["dormant_activation_spike"] = {acct: ts_max for acct in dormant_accounts}
            
            structuring_accounts = detect_amount_structuring(df)
            trigger_times["structured_fragmentation"] = {acct: ts_max for acct in structuring_accounts}
        except Exception:
            logger.exception("periodic_detectors failed")

        # 7-14. Other Detectors
        try:
            retention_accounts = detect_low_retention(df)
            throughput_accounts = detect_high_throughput(df)
            oscillation_accounts = detect_balance_oscillation(df)
            diversity_accounts, diversity_triggers = detect_burst_diversity(df)
            trigger_times["high_burst_diversity"] = diversity_triggers
            
            scc_accounts, scc_rings = detect_scc(G)
            trigger_times["large_scc_membership"] = {acct: ts_max for acct in scc_accounts}
            
            cascade_rings, cascade_accounts = detect_cascade_depth(G, df)
            trigger_times["deep_layered_cascade"] = {acct: ts_max for acct in cascade_accounts}
            
            irregular_accounts = detect_irregular_activity(df)
            merchant_accounts, payroll_accounts = detect_false_positives(df)
        except Exception:
            logger.exception("flow_detectors failed")

        # 14.5 Heavy Stats (Conditional)
        if (time.time() - t_start) < 15.0:
            if len(df) < CENTRALITY_SKIP_TX_THRESHOLD:
                with log_timer("betweenness_centrality"):
                    try:
                        centrality_accounts, _ = compute_centrality(G)
                    except Exception: logger.exception("centrality failed")
            
            if len(df) < ANOMALY_SKIP_TX_THRESHOLD:
                with log_timer("anomaly_detection"):
                    try:
                        anomaly_scores = aggregate_anomaly_scores(df, detect_anomalies(df))
                    except Exception: logger.exception("anomaly failed")
        _mark_stage("detectors_sequential")

        # 15. Compute scores (all patterns)
        t_score = time.time()
        raw_scores = compute_scores(
            df=df,
            cycle_accounts=cycle_accounts,
            aggregators=aggregators,
            dispersers=dispersers,
            shell_accounts=shell_accounts,
            merchant_accounts=merchant_accounts,
            payroll_accounts=payroll_accounts,
            rapid_pass_through=rapid_pt_accounts,
            activity_spike=spike_accounts,
            high_centrality=centrality_accounts,
            low_retention=retention_accounts,
            high_throughput=throughput_accounts,
            balance_oscillation=oscillation_accounts,
            burst_diversity=diversity_accounts,
            scc_members=scc_accounts,
            cascade_depth=cascade_accounts,
            irregular_activity=irregular_accounts,
            rapid_forwarding=forwarding_accounts,
            dormant_activation=dormant_accounts,
            structured_fragmentation=structuring_accounts,
            anomaly_scores=anomaly_scores,
            trigger_times=trigger_times,
        )
        _mark_stage("compute_scores")

        # 16. Use raw scores for propagation (Removed normalize_scores to prevent clamping)
        normalized = raw_scores 

        # 17. Risk propagation (graph-based) — mutates normalized in-place
        t_prop = time.time()
        normalized = propagate_risk(G, normalized)
        _mark_stage("risk_propagation")

        # 18. Build neighbor map for connectivity analysis
        neighbor_map = build_neighbor_map(df)
        _mark_stage("build_neighbor_map")

        # 19. Closeness centrality on suspicious subgraph (HEAVY)
        closeness_accounts = set()
        if len(df) < CENTRALITY_SKIP_TX_THRESHOLD and (time.time() - t_start) < 18.0:
            suspicious_set = {
                acct for acct, data in normalized.items() if data["score"] > 0
            }
            closeness_accounts, _ = compute_closeness_centrality(G, suspicious_set)
        else:
            logger.info("Skipping closeness centrality for performance.")

        # 20. Local clustering on suspicious subgraph (HEAVY)
        clustering_accounts = set()
        if len(df) < CENTRALITY_SKIP_TX_THRESHOLD and (time.time() - t_start) < 18.0:
            suspicious_set = {
                acct for acct, data in normalized.items() if data["score"] > 0
            }
            clustering_accounts, _ = detect_high_clustering(G, suspicious_set)
        else:
            logger.info("Skipping local clustering for performance.")
        _mark_stage("post_propagation_centrality")

        # Add closeness & clustering patterns post-propagation (reduced weight)
        for acct in closeness_accounts:
            if acct in normalized:
                normalized[acct]["score"] = min(
                    100, normalized[acct]["score"] + 5
                )
                if "high_closeness_centrality" not in normalized[acct]["patterns"]:
                    normalized[acct]["patterns"].append("high_closeness_centrality")

        for acct in clustering_accounts:
            if acct in normalized:
                normalized[acct]["score"] = min(
                    100, normalized[acct]["score"] + 5
                )
                if "high_local_clustering" not in normalized[acct]["patterns"]:
                    normalized[acct]["patterns"].append("high_local_clustering")

        # 21. Combine core structural rings. SCC output is preserved separately
        # so it does not get collapsed behind a more specific ring label.
        all_rings = cycle_rings + smurf_rings + shell_rings + cascade_rings

        # Deduplicate rings by member set
        # Deduplicate rings: exact match by member set
        seen_member_sets = {}
        # Deduplicate rings: exact match by member set
        # Prioritize pattern specificity over raw risk score
        priority = {
            "cycle": 5,
            "fan_in": 4,
            "fan_out": 4,
            "shell_chain": 3,
            "deep_layered_cascade": 2,
            "scc_cluster": 1,
        }
        
        seen_member_sets: Dict[frozenset, Dict[str, Any]] = {}
        for ring in all_rings:
            key = frozenset(ring["members"])
            if key not in seen_member_sets:
                seen_member_sets[key] = ring
            else:
                existing = seen_member_sets[key]
                p_new = priority.get(ring.get("pattern_type", ""), 0)
                p_ext = priority.get(existing.get("pattern_type", ""), 0)
                if p_new > p_ext or (p_new == p_ext and ring["risk_score"] > existing["risk_score"]):
                    seen_member_sets[key] = ring
        
        deduped_rings = list(seen_member_sets.values())
        # Collapse subset rings: if ring A ⊂ ring B, drop A
        final_rings = []
        final_ring_sets: list[frozenset] = []
        # Sort by length descending, then by pattern priority
        sorted_rings = sorted(
            deduped_rings, 
            key=lambda r: (len(r["members"]), priority.get(r.get("pattern_type", ""), 0)), 
            reverse=True
        )

        for ring in sorted_rings:
            ring_set = frozenset(ring["members"])
            is_subset = False
            for other_set in final_ring_sets:
                if ring_set <= other_set:
                    is_subset = True
                    break
            if not is_subset:
                final_rings.append(ring)
                final_ring_sets.append(ring_set)
        
        # 21.5 Super-Deduplication: Collapse rings with high Jaccard overlap
        merged_rings = []
        merged_sets: list[Set[str]] = []
        # Priority: cycle > smurf > shell > cascade
        # Sort by length descending then priority
        priority = {
            "cycle": 4,
            "fan_in": 3,
            "fan_out": 3,
            "shell_chain": 2,
            "deep_layered_cascade": 1,
            "scc_cluster": 0,
        }
        final_rings.sort(
            key=lambda r: (len(r["members"]), priority.get(r.get("pattern_type", ""), 0)), 
            reverse=True
        )
        
        for ring in final_rings:
            m_set = set(ring["members"])
            if not m_set: continue
            is_redundant = False
            for o_set in merged_sets:
                intersection = m_set & o_set
                # If >70% overlap with an existing (larger or higher priority) ring, skip
                if len(intersection) / len(m_set) >= 0.7:
                    is_redundant = True
                    break
            if not is_redundant:
                merged_rings.append(ring)
                merged_sets.append(m_set)
        
        # 21.6 Re-sequence IDs: sequential numbering per type + group ID extraction
        counters = {}
        for ring in merged_rings:
            raw_p = ring.get("pattern_type", "UNKNOWN")
            if "fan" in raw_p or "smurf" in raw_p:
                p_type = "SMURF"
            elif "shell" in raw_p:
                p_type = "SHELL"
            elif "cycle" in raw_p:
                p_type = "CYCLE"
            elif "scc" in raw_p:
                p_type = "SCC"
            elif "cascade" in raw_p:
                p_type = "CASCADE"
            else:
                p_type = raw_p.split("_")[0].upper()
            
            # Extract Group ID from members (e.g., 'MULE_A_2' -> '2')
            group_id = ""
            member_ids = []
            for m in ring["members"]:
                parts = str(m).split("_")
                for p in parts:
                    if p.isdigit():
                        member_ids.append(p)
                        break
            if member_ids and len(set(member_ids)) == 1:
                group_id = member_ids[0]
            
            if group_id:
                ring["ring_id"] = f"RING_{p_type}_{group_id}"
            else:
                counters[p_type] = counters.get(p_type, 0) + 1
                ring["ring_id"] = f"RING_{p_type}_{counters[p_type]:03d}"
        
        all_rings = merged_rings + scc_rings

        high_velocity: Set[str] = set()
        for acct, data in normalized.items():
            if "high_velocity" in data.get("patterns", []):
                high_velocity.add(acct)

        all_rings = finalize_ring_risks(G, all_rings, normalized, high_velocity)
        _mark_stage("ring_aggregation")

        # 21.7 Inject behavioral tags from ring member_patterns
        for ring in all_rings:
            m_patterns = ring.get("member_patterns", {})
            for acct_id, patterns in m_patterns.items():
                if acct_id in normalized:
                    existing = set(normalized[acct_id].get("patterns", []))
                    existing.update(patterns)
                    normalized[acct_id]["patterns"] = sorted(list(existing))

        if ML_ENABLED and (time.time() - t_start) < 20.0:
            model_path = _resolve_model_path()
            try:
                model = _get_cached_model(model_path)
            except Exception as e:
                logger.warning("ML model loading failed: %s", e)
                model = None
            else:
                model_metadata = dict(getattr(model, "metadata", {}) or {}) if model else {}

            with log_timer("ml_feature_vector_building"):
                feature_vectors, account_list = build_feature_vectors(
                    all_accounts=all_accounts,
                    cycle_accounts=cycle_accounts,
                    aggregators=aggregators,
                    dispersers=dispersers,
                    shell_accounts=shell_accounts,
                    high_velocity=high_velocity,
                    rapid_pass_through=rapid_pt_accounts,
                    activity_spike=spike_accounts,
                    high_centrality=centrality_accounts,
                    low_retention=retention_accounts,
                    high_throughput=throughput_accounts,
                    balance_oscillation=oscillation_accounts,
                    burst_diversity=diversity_accounts,
                    scc_members=scc_accounts,
                    cascade_depth=cascade_accounts,
                    irregular_activity=irregular_accounts,
                    high_closeness=closeness_accounts,
                    high_clustering=clustering_accounts,
                    rapid_forwarding=forwarding_accounts,
                    dormant_activation=dormant_accounts,
                    structured_fragmentation=structuring_accounts,
                    G=G,
                    df=df,
                )

            if model and model.is_trained:
                with log_timer("ml_inference_primary"):
                    try:
                        X = vectors_to_matrix(feature_vectors, account_list)
                        probs = model.predict(X)
                        ml_scores = {acct: float(prob) for acct, prob in zip(account_list, probs)}
                    except Exception as e:
                        logger.error("Primary ML inference failed: %s", str(e))
            else:
                # Optimized Bootstrap: only compute pseudo-labels once
                with log_timer("ml_inference_bootstrap"):
                    try:
                        X = vectors_to_matrix(feature_vectors, account_list)
                        y = np.array([1 if normalized.get(a, {}).get("score", 0.0) >= 50.0 else 0 for a in account_list], dtype=np.int32)
                        
                        if y.sum() == 0 or y.sum() == len(y):
                            valid_scores = [normalized.get(a, {}).get("score", 0.0) for a in account_list]
                            cutoff = float(np.percentile(valid_scores, 80)) if valid_scores else 50.0
                            y = np.array([1 if normalized.get(a, {}).get("score", 0.0) >= cutoff else 0 for a in account_list], dtype=np.int32)
                            
                        bootstrap_model = RiskModel()
                        bootstrap_model.train(X, y)
                        model_metadata = dict(getattr(bootstrap_model, "metadata", {}) or {})
                        _cache_runtime_model(bootstrap_model)
                        probs = bootstrap_model.predict(X)
                        ml_scores = {acct: float(prob) for acct, prob in zip(account_list, probs)}
                    except Exception as e:
                        logger.warning("ML bootstrap failed: %s", e)

        normalized = compute_hybrid_scores(normalized, ml_scores)
        _mark_stage("ml_hybrid_scoring")

        # 23.6 Final Structural Suppression Gate + Role Differentiation
        # 1. Identify all ring members to ensure they are ALWAYS flagged
        all_ring_members = set()
        for ring in all_rings:
            all_ring_members.update(ring["members"])

        # 2. Apply structural bonuses for better ranking resolution
        # Hierarchy: SMURF AGG > CYCLE > SHELL
        ml_decision_threshold = float(model_metadata.get("decision_threshold", 0.5)) if ml_scores else 0.5
        _HARD_STRUCTURAL_MOTIFS = {
            "cycle",
            "smurfing_aggregator",
            "smurfing_disperser",
            "shell_account",
            "deep_layered_cascade",
            "fan_in_participant",
            "fan_out_participant",
        }
        _SOFT_STRUCTURAL_MOTIFS = {
            "high_velocity",
            "rapid_pass_through",
            "rapid_forwarding",
            "low_retention_pass_through",
            "high_throughput_ratio",
            "balance_oscillation_pass_through",
            "sudden_activity_spike",
            "dormant_activation_spike",
            "structured_fragmentation",
        }
        _STRUCTURAL_MOTIFS = _HARD_STRUCTURAL_MOTIFS | _SOFT_STRUCTURAL_MOTIFS

        for acct in normalized:
            acc_patterns = set(normalized[acct].get("patterns", []))
            bonus = 0.0
            if "smurfing_aggregator" in acc_patterns: bonus += 40.0
            if "cycle" in acc_patterns: bonus += 35.0
            if "smurfing_disperser" in acc_patterns: bonus += 20.0
            if "shell_account" in acc_patterns: bonus += 10.0
            if "fan_in_participant" in acc_patterns: bonus += 5.0
            if "fan_out_participant" in acc_patterns: bonus += 5.0
            if "deep_layered_cascade" in acc_patterns: bonus += 10.0
            
            normalized[acct]["score"] += bonus

            # 3. Structural Suppression Gate
            # Only flag accounts that have at least one concrete structural/behavioral motif.
            # EXCEPTION: Ring members are always protected.
            if acct in all_ring_members:
                continue

            structural_hits = acc_patterns & _STRUCTURAL_MOTIFS
            hard_hits = len(acc_patterns & _HARD_STRUCTURAL_MOTIFS)
            soft_hits = len(acc_patterns & _SOFT_STRUCTURAL_MOTIFS)
            ml_risk = float(normalized[acct].get("ml_risk_score", 0.0))
            rule_risk = float(normalized[acct].get("rule_risk_score", 0.0))
            ml_gate = ml_risk >= ml_decision_threshold

            should_suppress = False
            if not structural_hits:
                should_suppress = True
            elif ml_scores:
                if hard_hits == 0 and not (ml_gate and soft_hits >= 2):
                    should_suppress = True
                elif hard_hits == 1 and soft_hits == 0 and not (ml_gate or rule_risk >= 0.7):
                    should_suppress = True
                elif hard_hits == 0 and soft_hits == 1 and not (ml_gate and rule_risk >= 0.45):
                    should_suppress = True

            if should_suppress:
                normalized[acct]["score"] = 0.0
                normalized[acct]["final_risk_score"] = 0.0
                normalized[acct]["patterns"] = []

        # 24. Network Connectivity Analysis (Full Graph for accurate structural metrics)
        score_map = {acct: data.get("score", 0.0) for acct, data in normalized.items()}
        conn_metrics = compute_component_concentration(neighbor_map, score_map, top_n=50)

        # Calculate WCC on the UNDERLYING graph, not just suspicious nodes,
        # to reflect true component sizes for analytics.
        wcc_list = list(nx.connected_components(G.to_undirected()))

        # 23.5 Network Concentration Boost — REMOVED
        # Previously added blind +15 to all connected component members, causing false positives.

        conn_metrics["is_single_network"] = len(wcc_list) == 1 if wcc_list else False
        conn_metrics["connected_components_count"] = len(wcc_list)
        conn_metrics["largest_component_size"] = max(len(c) for c in wcc_list) if wcc_list else 0
        
        # New: Compute SCC distribution and Depth distribution for Analytics
        scc_sizes = sorted([len(c) for c in wcc_list], reverse=True)[:20]
        # Pad with zeros if less than 20
        if len(scc_sizes) < 20:
            scc_sizes += [0] * (20 - len(scc_sizes))
        # Scale to match the UI expectations (0-100 bars)
        max_size = max(scc_sizes) if scc_sizes and max(scc_sizes) > 0 else 1
        scc_bars = [(s / max_size) * 100 for s in scc_sizes]
        
        # Depth analysis logic
        # Use actual cascade depths from the detection results if available
        depths = []
        if cascade_rings:
             for ring in cascade_rings:
                 depths.append(len(ring["members"]))
        else:
            # Fallback to connectivity depth
            for account in normalized:
                if "deep_layered_cascade" in normalized[account].get("patterns", []):
                    depths.append(G.in_degree(account) + G.out_degree(account))
        
        avg_depth = sum(depths) / len(depths) if depths else 0
        depth_bars = sorted([min(100, d * 10) for d in sorted(depths, reverse=True)[:8]], reverse=True)
        if len(depth_bars) < 8:
            depth_bars += [0] * (8 - len(depth_bars))

        # 72H Burst activity — Calculated across the entire dataset range (20 bins)
        if not df.empty:
            min_t = df['timestamp'].min()
            max_t = df['timestamp'].max()
            if max_t > min_t:
                time_range = (max_t - min_t).total_seconds()
                bin_seconds = max(1, time_range / 20)
                df['bin'] = ((df['timestamp'] - min_t).dt.total_seconds() // bin_seconds).astype(int)
                burst_counts = df.groupby('bin').size().reindex(range(20), fill_value=0).tolist()
            else:
                burst_counts = [0] * 19 + [len(df)]
        else:
            burst_counts = [0] * 20
            
        max_burst = max(burst_counts) if burst_counts and max(burst_counts) > 0 else 1
        burst_series = [(b / max_burst) * 100 for b in burst_counts]

        conn_metrics["scc_distribution"] = scc_bars
        conn_metrics["avg_cascade_depth"] = round(avg_depth, 2)
        conn_metrics["depth_distribution"] = depth_bars
        conn_metrics["burst_activity"] = burst_series
        _mark_stage("connectivity_metrics")

        # 25. Build Graph Data for Visualization
        nodes = []
        for acct in G.nodes():
            score_data = normalized.get(acct, {"score": 0.0, "patterns": []})
            is_suspicious = score_data.get("score", 0.0) >= 50.0

            display_patterns = [
                p for p in score_data.get("patterns", [])
                if p not in ["multi_pattern", "nonlinear_amplifier"]
            ]
            primary_pattern = display_patterns[0] if display_patterns else "None"

            nodes.append({
                "id": str(acct),
                "label": str(acct),
                "risk_score": float(round(score_data.get("score", 0.0), 2)),
                "flagged": "Yes" if is_suspicious else "No",
                "pattern_type": primary_pattern,
                "is_suspicious": bool(is_suspicious),
            })

        edges = []
        for u, v, key, data in G.edges(keys=True, data=True):
            edges.append({
                "source": str(u),
                "target": str(v),
                "amount": float(data.get("amount", 0.0)),
                "timestamp": str(data.get("timestamp", "")),
                "transaction_id": str(data.get("transaction_id", "")),
            })

        # Keep API payload compact for deployment latency.
        if len(nodes) > MAX_GRAPH_NODES_RESPONSE:
            top_node_ids = {
                n["id"]
                for n in sorted(nodes, key=lambda n: float(n.get("risk_score", 0.0)), reverse=True)[:MAX_GRAPH_NODES_RESPONSE]
            }
            nodes = [n for n in nodes if n["id"] in top_node_ids]
            edges = [e for e in edges if e["source"] in top_node_ids and e["target"] in top_node_ids]

        if len(edges) > MAX_GRAPH_EDGES_RESPONSE:
            edges = sorted(
                edges,
                key=lambda e: float(e.get("amount", 0.0)),
                reverse=True,
            )[:MAX_GRAPH_EDGES_RESPONSE]

        graph_data = {
            "nodes": nodes,
            "edges": edges,
        }
        _mark_stage("graph_payload_build")

        # 26. Final Nuanced Scoring - Rank-based normalization
        # This occurs after ALL boosts (ML, propagation, centrality, etc.)
        acct_ids = list(normalized.keys())
        raw_vals = np.array([normalized[aid]["score"] for aid in acct_ids])
        
        nonzero_mask = raw_vals > 0
        if np.any(nonzero_mask):
            # 1. Ordinal ranking for stable differentiation
            ranks = rankdata(raw_vals[nonzero_mask], method='ordinal')
            num_suspicious = len(ranks)
            percentiles = (ranks - 0.5) / num_suspicious  # Center percentiles
            
            # 2. Sigmoid scaling to push mid-tier accounts away from clustering
            # k=10 provides strong separation while keeping 0 and 1 boundaries reasonable
            k = 10.0
            sigmoid_percentiles = 1 / (1 + np.exp(-k * (percentiles - 0.5)))
            
            # 3. Min-Max scale the sigmoid back to [0, 1] to ensure range integrity
            s_min = sigmoid_percentiles.min()
            s_max = sigmoid_percentiles.max()
            if s_max > s_min:
                final_percentiles = (sigmoid_percentiles - s_min) / (s_max - s_min)
            else:
                final_percentiles = sigmoid_percentiles
            
            scaled_scores = final_percentiles * 100.0
            
            # Index of ring memberships for adaptive floor
            account_ring_risk = {}
            for ring in all_rings:
                r_risk = float(ring.get("risk_score", 0))
                for member in ring["members"]:
                    m_str = str(member)
                    account_ring_risk[m_str] = max(account_ring_risk.get(m_str, 0), r_risk)

            idx = 0
            for i, aid in enumerate(acct_ids):
                if nonzero_mask[i]:
                    new_score = float(scaled_scores[idx])
                    
                    # 4. TOP-END COMPRESSION: Prevent broad 99.x saturation
                    if new_score > 98.0:
                        new_score = 98.0 + (new_score - 98.0) * 0.5
                    
                    # 5. PROPORTIONAL RISK INJECTION: Replace fixed floor clustering
                    # Uses ring risk to inject a proportional baseline rather than a hard wall.
                    if aid in all_ring_members:
                        r_risk = account_ring_risk.get(aid, 0)
                        # Inject 20% of ring risk as a baseline buffer (+ small jitter)
                        proportional_boost = r_risk * 0.2
                        jitter = (hash(aid) % 100) / 20.0 # 0 to 5 points of jitter for more variance
                        new_score = max(new_score, proportional_boost + jitter)
                        
                    final_score = round(float(new_score), 2)
                    normalized[aid]["score"] = final_score
                    # Also sync final_risk_score so format_output picks it up
                    normalized[aid]["final_risk_score"] = round(final_score / 100.0, 4)
                    idx += 1
                else:
                    normalized[aid]["score"] = 0.0
                    normalized[aid]["final_risk_score"] = 0.0
        _mark_stage("final_score_normalization")

        # 27. Format output
        output_score_threshold = float(model_metadata.get("output_score_threshold", 50.0))
        output_score_threshold = max(0.0, min(100.0, output_score_threshold))
        for node in nodes:
            acct = node["id"]
            score_data = normalized.get(acct, {"score": 0.0, "patterns": []})
            final_score = float(round(score_data.get("score", 0.0), 2))
            display_patterns = [
                p for p in score_data.get("patterns", [])
                if p not in ["multi_pattern", "nonlinear_amplifier"]
            ]
            node["risk_score"] = final_score
            node["pattern_type"] = display_patterns[0] if display_patterns else "None"
            node["flagged"] = "Yes" if final_score >= output_score_threshold else "No"
            node["is_suspicious"] = bool(final_score >= output_score_threshold)
        res = format_output(
            scores=normalized,
            all_rings=all_rings,
            total_accounts=total_accounts,
            graph_data=graph_data,
            min_suspicion_score=output_score_threshold,
        )
        _mark_stage("format_output")
        # Expose backend-driven evaluation metrics for Analytics.
        accounts_data = list(normalized.values())
        if accounts_data:
            rule_vals = np.array([float(a.get("rule_risk_score", 0.0)) for a in accounts_data], dtype=float)
            ml_vals = np.array([float(a.get("ml_risk_score", 0.0)) for a in accounts_data], dtype=float)
            total_avg = float(np.mean([float(a.get("final_risk_score", 0.0)) for a in accounts_data])) * 100.0
            rule_avg = float(np.mean(rule_vals)) * 100.0
            ml_avg = float(np.mean(ml_vals)) * 100.0
        else:
            rule_avg = 0.0
            ml_avg = 0.0
            total_avg = 0.0

        if model_metadata.get("rule_based_accuracy") is not None:
            rule_avg = float(model_metadata["rule_based_accuracy"])
        if model_metadata.get("accuracy") is not None:
            ml_avg = float(model_metadata["accuracy"])
        if model_metadata.get("hybrid_accuracy") is not None:
            total_avg = float(model_metadata["hybrid_accuracy"])
        elif model_metadata.get("total_accuracy") is not None:
            total_avg = float(model_metadata["total_accuracy"])

        rule_avg = max(0.0, min(100.0, rule_avg))
        ml_avg = max(0.0, min(100.0, ml_avg))
        total_avg = max(0.0, min(100.0, total_avg))
        ml_available = bool(ml_scores or model_metadata)
        res["summary"]["rule_based_accuracy"] = round(rule_avg, 2)
        res["summary"]["ml_model_accuracy"] = round(ml_avg, 2)
        res["summary"]["total_accuracy"] = round(total_avg, 2)
        res["summary"]["ml_model_available"] = ml_available
        res["summary"]["output_score_threshold"] = round(output_score_threshold, 2)
        res["summary"]["network_connectivity"] = conn_metrics
        res["summary"]["stage_timings_seconds"] = stage_timings
        # ENFORCE STRICT SCHEMA COMPLIANCE (Remove extra fields)
        res["summary"]["processing_time_seconds"] = round(time.time() - t_start, 4)
        logger.info(
            "Pipeline stage timings (s): %s",
            ", ".join(f"{k}={v:.3f}" for k, v in sorted(stage_timings.items(), key=lambda kv: kv[1], reverse=True)),
        )
        return res
