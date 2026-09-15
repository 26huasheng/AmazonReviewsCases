from __future__ import annotations

from pathlib import Path
from typing import Sequence

import duckdb

from utils import sql_literal


def write_case_user_eligibility(
    con: duckdb.DuckDBPyConnection,
    features: Path,
    destination: Path,
    copy_atomic,
    *,
    min_history_products: int | None = None,
    max_days_since_last_event: int | None = None,
    min_category_products: int | None = None,
    min_market_products: int | None = None,
) -> None:
    """应用可配置的 pre-t0 用户资格规则。

    默认没有研究阈值写死；正式 benchmark 冻结阈值后通过参数固定。
    """
    src = sql_literal(str(features))
    clauses = ["TRUE"]
    reasons: list[str] = []
    if min_history_products is not None:
        clauses.append(f"history_product_count >= {int(min_history_products)}")
        reasons.append(
            f"WHEN history_product_count < {int(min_history_products)} "
            "THEN 'history_products_below_threshold'"
        )
    if max_days_since_last_event is not None:
        clauses.append(
            f"days_since_last_event IS NOT NULL AND days_since_last_event <= {int(max_days_since_last_event)}"
        )
        reasons.append(
            "WHEN days_since_last_event IS NULL "
            "THEN 'no_pre_t0_history'"
        )
        reasons.append(
            f"WHEN days_since_last_event > {int(max_days_since_last_event)} "
            "THEN 'recency_above_threshold'"
        )
    if min_category_products is not None:
        clauses.append(
            f"category_history_product_count >= {int(min_category_products)}"
        )
        reasons.append(
            f"WHEN category_history_product_count < {int(min_category_products)} "
            "THEN 'category_history_below_threshold'"
        )
    if min_market_products is not None:
        clauses.append(
            f"market_history_product_count >= {int(min_market_products)}"
        )
        reasons.append(
            f"WHEN market_history_product_count < {int(min_market_products)} "
            "THEN 'market_history_below_threshold'"
        )
    pass_expr = " AND ".join(f"({clause})" for clause in clauses)
    reason_expr = (
        "CASE " + " ".join(reasons) + " ELSE NULL END"
        if reasons else "NULL::VARCHAR"
    )
    copy_atomic(f"""
        SELECT *,
               ({pass_expr}) AS eligible_pre_cutoff,
               {reason_expr} AS ineligible_reason
        FROM read_parquet({src})
        ORDER BY case_id, user_id
    """, destination)


def write_threshold_scan(
    con: duckdb.DuckDBPyConnection,
    features: Path,
    destination: Path,
    copy_atomic,
    *,
    history_thresholds: Sequence[int] = (1, 3, 5, 10),
    recency_thresholds: Sequence[int] = (90, 180, 365, 730),
) -> None:
    """输出每个 Case 在候选阈值组合下会留下多少用户。"""
    con.execute("DROP TABLE IF EXISTS population_threshold_grid")
    con.execute("""
        CREATE TEMP TABLE population_threshold_grid(
            min_history_products BIGINT,
            max_days_since_last_event BIGINT
        )
    """)
    rows = [
        (int(history), int(recency))
        for history in history_thresholds
        for recency in recency_thresholds
    ]
    if rows:
        con.executemany("INSERT INTO population_threshold_grid VALUES (?, ?)", rows)
    src = sql_literal(str(features))
    copy_atomic(f"""
        SELECT f.case_id,
               g.min_history_products,
               g.max_days_since_last_event,
               count(*)::BIGINT AS population_count,
               count(*) FILTER (
                   f.history_product_count >= g.min_history_products
                   AND f.days_since_last_event IS NOT NULL
                   AND f.days_since_last_event <= g.max_days_since_last_event
               )::BIGINT AS eligible_count,
               count(*) FILTER (
                   f.history_product_count >= g.min_history_products
                   AND f.days_since_last_event IS NOT NULL
                   AND f.days_since_last_event <= g.max_days_since_last_event
                   AND f.market_history_product_count > 0
               )::BIGINT AS eligible_with_market_history,
               count(*) FILTER (
                   f.history_product_count >= g.min_history_products
                   AND f.days_since_last_event IS NOT NULL
                   AND f.days_since_last_event <= g.max_days_since_last_event
                   AND f.category_history_product_count > 0
               )::BIGINT AS eligible_with_category_history
        FROM read_parquet({src}) f
        CROSS JOIN population_threshold_grid g
        GROUP BY f.case_id,
                 g.min_history_products,
                 g.max_days_since_last_event
        ORDER BY f.case_id,
                 g.min_history_products,
                 g.max_days_since_last_event
    """, destination)
