from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


SUPPORTED_OUTCOME_POLICIES = {"first_observed_event"}


def write_positive_user_outcomes(
    con: duckdb.DuckDBPyConnection,
    future_events: Path,
    destination: Path,
    copy_atomic,
    *,
    outcome_policy: str = "first_observed_event",
) -> None:
    """把一用户多事件压成一个确定商品结果。

    当前实现只提供 first_observed_event。研究口径尚未冻结，因此 policy 显式写入产物，
    后续可以新增策略而不重扫原始事件。
    """
    if outcome_policy not in SUPPORTED_OUTCOME_POLICIES:
        raise ValueError(
            f"unsupported outcome_policy={outcome_policy}; "
            f"supported={sorted(SUPPORTED_OUTCOME_POLICIES)}"
        )
    src = sql_literal(str(future_events))
    copy_atomic(f"""
        WITH ranked AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY case_id, focal_id, user_id
                       ORDER BY event_timestamp, product_id
                   ) AS outcome_rank
            FROM read_parquet({src})
        )
        SELECT case_id,
               focal_id,
               market_id,
               source_partition,
               t0,
               user_id,
               product_id AS outcome_product_id,
               is_focal AS outcome_is_focal,
               event_timestamp,
               rating,
               verified_purchase,
               {sql_literal(outcome_policy)}::VARCHAR AS outcome_policy
        FROM ranked
        WHERE outcome_rank=1
        ORDER BY case_id, focal_id, user_id
    """, destination)
