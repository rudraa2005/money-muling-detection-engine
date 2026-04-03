"""
Hybrid Scorer — Combines Rule-Based and ML Risk Scores.

Blends the existing rule-based suspicion score with the XGBoost ML
probability score using configurable weights:

    final_risk_score = rule_weight × normalized_rule_score + ml_weight × ml_risk_score

The rule score is normalized from [0, 100] → [0, 1] before blending.
If the ML model is unavailable or fails, falls back to the pure
normalized rule score.

Time Complexity: O(V)
Memory: O(V)
"""

import logging
from typing import Any, Dict, Optional

import numpy as np

from app.config import ML_MODEL_WEIGHT, ML_RULE_WEIGHT

logger = logging.getLogger(__name__)


def compute_hybrid_scores(
    normalized_scores: Dict[str, Dict[str, Any]],
    ml_scores: Optional[Dict[str, float]],
    rule_weight: float = ML_RULE_WEIGHT,
    ml_weight: float = ML_MODEL_WEIGHT,
) -> Dict[str, Dict[str, Any]]:
    """
    Blend rule-based and ML scores for each account.

    Args:
        normalized_scores: Existing scores dict from the pipeline.
            Each value has at least {"score": float, "patterns": [...]}.
            Score is on [0, 100] scale.
        ml_scores: Dict mapping account_id → ml probability [0, 1].
            If None, falls back to pure rule score.
        rule_weight: Weight for the normalized rule score (default 0.6).
        ml_weight: Weight for the ML score (default 0.4).

    Returns:
        Updated normalized_scores dict, with each account's data enriched:
            - "rule_risk_score": float [0, 1]
            - "ml_risk_score": float [0, 1]
            - "final_risk_score": float [0, 1]
            - "scoring_method": "hybrid" | "rule_only"
    """
    use_ml = ml_scores is not None and len(ml_scores) > 0

    if not use_ml:
        logger.info("ML scores unavailable — falling back to rule-only scoring")

    for account, data in normalized_scores.items():
        # Normalize rule score from [0, 100] → [0, 1]
        rule_normalized = max(0.0, min(1.0, round(data["score"] / 100.0, 4)))
        data["rule_risk_score"] = rule_normalized

        if use_ml and account in ml_scores:
            ml_score = max(0.0, min(1.0, float(ml_scores[account])))
            data["ml_risk_score"] = float(round(ml_score, 4))
            
            base_final = rule_weight * rule_normalized + ml_weight * ml_score

            # Precision-oriented blending:
            # require at least one engine to show moderate confidence, and only
            # add a small boost when both agree.
            if ml_score < 0.15 and rule_normalized < 0.6:
                final_score = base_final * 0.25
            elif ml_score < 0.3 and rule_normalized < 0.45:
                final_score = base_final * 0.5
            else:
                final_score = base_final
                if rule_normalized >= 0.55 and ml_score >= 0.55:
                    final_score = min(1.0, final_score + 0.08)
            
            data["final_risk_score"] = float(round(final_score, 4))
            data["score"] = float(round(final_score * 100.0, 2))
            data["scoring_method"] = "hybrid"
        else:
            # Fallback: pure rule score
            data["ml_risk_score"] = 0.0
            data["final_risk_score"] = float(round(rule_normalized, 4))
            data["scoring_method"] = "rule_only"

    return normalized_scores
