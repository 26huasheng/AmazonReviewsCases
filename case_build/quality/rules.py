from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from utils import sql_literal


DEFAULT_RULES: dict[str, Any] = {
    "min_competitors": 6,
    "max_competitors": 16,
    "min_gt1_users": 20,
    "min_history_product_count": 3,
    "max_days_since_last_event": 365,
    # Kept for compatibility; not used as default acceptance gates.
    "min_shelf_products": None,
    "min_selected_users": None,
    "min_market_positive_users": None,
    "max_none_rate": None,
    "min_focal_demand_count": None,
    "min_post90_rating_count": None,
    "min_market_pre_t0_review_count": None,
    "require_review_activity_truth": False,
    "max_gt1_users": None,
}


def load_quality_rules(path: Path | None) -> dict[str, Any]:
    rules = dict(DEFAULT_RULES)
    if path is None:
        return rules
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("quality rules must be a JSON object")
    unknown = set(value) - set(rules)
    if unknown:
        raise ValueError(f"unknown quality rule keys: {sorted(unknown)}")
    rules.update(value)
    return rules


def write_focal_quality_decisions(
    con: duckdb.DuckDBPyConnection,
    metrics: Path,
    destination: Path,
    copy_atomic,
    *,
    rules: dict[str, Any],
) -> None:
    src = sql_literal(str(metrics))
    min_c = int(rules["min_competitors"])
    max_c = int(rules["max_competitors"])
    min_u = int(rules["min_gt1_users"])

    reason_cases = [
        "CASE WHEN NOT coalesce(evaluation_window_complete, false) "
        "THEN 'evaluation_window_incomplete' END",
        f"CASE WHEN selected_competitor_count < {min_c} THEN 'competitors_below_{min_c}' END",
        f"CASE WHEN selected_competitor_count > {max_c} THEN 'competitors_above_{max_c}' END",
        "CASE WHEN NOT coalesce(focal_in_case_shelf, false) "
        "THEN 'focal_missing_from_shelf' END",
        "CASE WHEN coalesce(competitor_self_reference_count, 0) > 0 "
        "THEN 'competitor_self_reference' END",
        "CASE WHEN coalesce(duplicate_competitor_count, 0) > 0 "
        "THEN 'duplicate_competitor' END",
        "CASE WHEN coalesce(competitor_missing_from_shelf_count, 0) > 0 "
        "THEN 'competitor_missing_from_shelf' END",
        "CASE WHEN NOT coalesce(competitor_count_consistent, false) "
        "THEN 'competitor_count_inconsistent' END",
        "CASE WHEN NOT coalesce(competitor_time_eligibility_complete, false) "
        "THEN 'competitor_time_eligibility_failed' END",
        "CASE WHEN NOT coalesce(local_shelf_consistent, false) "
        "THEN 'local_shelf_inconsistent' END",
        "CASE WHEN NOT coalesce(competitor_relation_complete, false) "
        "THEN 'competitor_relation_incomplete' END",
        f"CASE WHEN gt1_user_count < {min_u} THEN 'gt1_users_below_{min_u}' END",
        "CASE WHEN NOT coalesce(gt1_unique_user_complete, false) "
        "THEN 'gt1_duplicate_choice' END",
        "CASE WHEN NOT coalesce(gt1_choice_membership_complete, false) "
        "THEN 'gt1_choice_outside_local_shelf' END",
        "CASE WHEN NOT coalesce(gt1_window_complete, false) "
        "THEN 'gt1_choice_outside_window' END",
        "CASE WHEN NOT coalesce(gt1_history_quality_complete, false) "
        "THEN 'gt1_history_quality_failed' END",
        "CASE WHEN NOT coalesce(gt1_reducer_unique, false) "
        "THEN 'gt1_reducer_not_unique' END",
        "CASE WHEN NOT coalesce(gt1_traceable_to_window_event, false) "
        "THEN 'gt1_not_traceable_to_window_event' END",
    ]
    reasons_expr = (
        "list_filter(list_value(" + ",".join(reason_cases) + "), x -> x IS NOT NULL)"
    )
    pass_expr = f"""
        coalesce(evaluation_window_complete, false)
        AND selected_competitor_count >= {min_c}
        AND selected_competitor_count <= {max_c}
        AND coalesce(focal_in_case_shelf, false)
        AND coalesce(competitor_relation_complete, false)
        AND coalesce(competitor_count_consistent, false)
        AND coalesce(competitor_time_eligibility_complete, false)
        AND coalesce(local_shelf_consistent, false)
        AND coalesce(gt1_unique_user_complete, false)
        AND coalesce(gt1_choice_membership_complete, false)
        AND coalesce(gt1_window_complete, false)
        AND coalesce(gt1_history_quality_complete, false)
        AND coalesce(gt1_reducer_unique, false)
        AND coalesce(gt1_traceable_to_window_event, false)
        AND gt1_user_count >= {min_u}
    """
    copy_atomic(f"""
        SELECT
            case_id,
            focal_id,
            ({pass_expr}) AS focal_quality_pass,
            CASE WHEN ({pass_expr}) THEN 'accepted' ELSE 'rejected' END
                AS focal_quality_status,
            {reasons_expr} AS focal_quality_rejection_reasons
        FROM read_parquet({src})
        ORDER BY case_id, focal_id
    """, destination)


def write_case_quality_decisions(
    con: duckdb.DuckDBPyConnection,
    metrics: Path,
    destination: Path,
    copy_atomic,
) -> None:
    src = sql_literal(str(metrics))
    copy_atomic(f"""
        SELECT
            case_id,
            original_focal_count,
            accepted_focal_count,
            rejected_focal_count,
            final_shelf_size,
            (accepted_focal_count >= 1) AS case_quality_pass,
            CASE WHEN accepted_focal_count >= 1 THEN 'accepted' ELSE 'rejected' END
                AS case_quality_status,
            CASE WHEN accepted_focal_count >= 1
                 THEN CAST([] AS VARCHAR[])
                 ELSE list_value('no_accepted_focal')
            END AS case_quality_rejection_reasons
        FROM read_parquet({src})
        ORDER BY case_id
    """, destination)
