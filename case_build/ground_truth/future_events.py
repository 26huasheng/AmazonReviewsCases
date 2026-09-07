from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


def write_gt1_shelf_events(
    con: duckdb.DuckDBPyConnection,
    cases: Path,
    case_focals: Path,
    focal_competitors: Path,
    canonical_user_events: Path,
    destination: Path,
    copy_atomic,
) -> None:
    """GT1 原始事件：不预先限制 case_users。

    对每个 focal，局部货架 = focal + selected competitors，
    窗口 [t0, t0+90) 内对货架任意商品的真实评分/选择记录。
    """
    c = sql_literal(str(cases))
    f = sql_literal(str(case_focals))
    comps = sql_literal(str(focal_competitors))
    e = sql_literal(str(canonical_user_events))
    copy_atomic(f"""
        WITH local_shelf AS (
            SELECT f.case_id,
                   f.focal_id,
                   c.market_id,
                   c.source_partition,
                   f.t0,
                   f.evaluation_start,
                   f.evaluation_end_exclusive,
                   f.focal_product_id AS product_id,
                   TRUE AS is_focal
            FROM read_parquet({f}) f
            JOIN read_parquet({c}) c USING(case_id)
            UNION ALL
            SELECT f.case_id,
                   f.focal_id,
                   c.market_id,
                   c.source_partition,
                   f.t0,
                   f.evaluation_start,
                   f.evaluation_end_exclusive,
                   fc.competitor_product_id,
                   FALSE
            FROM read_parquet({comps}) fc
            JOIN read_parquet({f}) f USING(case_id, focal_id)
            JOIN read_parquet({c}) c USING(case_id)
            WHERE fc.competitor_selected
        )
        SELECT s.case_id,
               s.focal_id,
               s.market_id,
               s.source_partition,
               s.t0,
               s.evaluation_start,
               s.evaluation_end_exclusive,
               e.user_id,
               s.product_id,
               s.is_focal,
               e.event_timestamp,
               e.event_date,
               e.rating,
               e.verified_purchase
        FROM local_shelf s
        JOIN read_parquet({e}) e
          ON s.source_partition=e.source_partition
         AND s.product_id=e.product_id
         AND e.user_id IS NOT NULL
         AND trim(e.user_id) <> ''
         AND e.event_timestamp >= CAST(s.evaluation_start AS TIMESTAMP)
         AND e.event_timestamp < CAST(s.evaluation_end_exclusive AS TIMESTAMP)
        ORDER BY s.case_id, s.focal_id, e.user_id, e.event_timestamp, s.product_id
    """, destination)


def filter_gt1_by_user_quality(
    con: duckdb.DuckDBPyConnection,
    gt1_outcomes: Path,
    user_history: Path,
    destination: Path,
    copy_atomic,
    *,
    min_history_products: int,
    max_days_since_last_event: int,
) -> None:
    """只保留 focal t0 前 history>=3 且 recency<=365 的 GT1 用户。"""
    src = sql_literal(str(gt1_outcomes))
    hist = sql_literal(str(user_history))
    copy_atomic(f"""
        WITH with_hist AS (
            SELECT o.*,
                   coalesce(g.cumulative_product_count, 0)::BIGINT AS history_product_count,
                   g.event_date AS last_event_date
            FROM read_parquet({src}) o
            ASOF LEFT JOIN read_parquet({hist}) g
              ON o.user_id=g.user_id
             AND g.event_date < o.t0
        )
        SELECT case_id,
               focal_id,
               market_id,
               source_partition,
               user_id,
               outcome_product_id,
               outcome_is_focal,
               event_timestamp,
               rating,
               verified_purchase,
               outcome_policy,
               history_product_count,
               CASE WHEN last_event_date IS NULL THEN NULL
                    ELSE date_diff('day', last_event_date, t0)::BIGINT
               END AS days_since_last_event,
               t0
        FROM with_hist
        WHERE history_product_count >= {int(min_history_products)}
          AND last_event_date IS NOT NULL
          AND date_diff('day', last_event_date, t0)
                <= {int(max_days_since_last_event)}
        ORDER BY case_id, focal_id, user_id
    """, destination)


def write_case_future_market_events(
    con: duckdb.DuckDBPyConnection,
    cases: Path,
    case_focals: Path,
    case_users: Path,
    focal_competitors: Path,
    canonical_user_events: Path,
    destination: Path,
    copy_atomic,
) -> None:
    """每个 focal 只用自己的局部货架（focal + selected competitors）扫 future。"""
    c = sql_literal(str(cases))
    f = sql_literal(str(case_focals))
    u = sql_literal(str(case_users))
    comps = sql_literal(str(focal_competitors))
    e = sql_literal(str(canonical_user_events))
    copy_atomic(f"""
        WITH local_shelf AS (
            SELECT focal_id, case_id, focal_product_id AS product_id, TRUE AS is_focal
            FROM read_parquet({f})
            UNION ALL
            SELECT focal_id, case_id, competitor_product_id AS product_id, FALSE AS is_focal
            FROM read_parquet({comps})
            WHERE competitor_selected
        )
        SELECT c.case_id,
               f.focal_id,
               c.market_id,
               c.source_partition,
               f.t0,
               f.evaluation_start,
               f.evaluation_end_exclusive,
               u.user_id,
               s.product_id,
               s.is_focal,
               e.event_timestamp,
               e.event_date,
               e.rating,
               e.verified_purchase
        FROM read_parquet({u}) u
        JOIN read_parquet({c}) c USING(case_id)
        JOIN read_parquet({f}) f USING(case_id)
        JOIN local_shelf s
          ON f.focal_id=s.focal_id
         AND f.case_id=s.case_id
        JOIN read_parquet({e}) e
          ON u.user_id=e.user_id
         AND c.source_partition=e.source_partition
         AND e.product_id=s.product_id
         AND e.event_timestamp >= CAST(f.evaluation_start AS TIMESTAMP)
         AND e.event_timestamp < CAST(f.evaluation_end_exclusive AS TIMESTAMP)
        ORDER BY c.case_id, f.focal_id, u.user_id, e.event_timestamp, e.product_id
    """, destination)


def write_review_activity_truth(
    con: duckdb.DuckDBPyConnection,
    cases: Path,
    case_focals: Path,
    focal_competitors: Path,
    rating_daily: Path,
    destination: Path,
    copy_atomic,
) -> None:
    """每个 focal 局部货架的未来评论量排名。"""
    c = sql_literal(str(cases))
    f = sql_literal(str(case_focals))
    comps = sql_literal(str(focal_competitors))
    d = sql_literal(str(rating_daily))
    copy_atomic(f"""
        WITH products AS (
            SELECT f.case_id, f.focal_id, c.source_partition,
                   f.evaluation_start, f.evaluation_end_exclusive,
                   f.focal_product_id AS product_id, TRUE AS is_focal
            FROM read_parquet({f}) f
            JOIN read_parquet({c}) c USING(case_id)
            UNION ALL
            SELECT fc.case_id, fc.focal_id, c.source_partition,
                   f.evaluation_start, f.evaluation_end_exclusive,
                   fc.competitor_product_id, FALSE
            FROM read_parquet({comps}) fc
            JOIN read_parquet({f}) f USING(case_id, focal_id)
            JOIN read_parquet({c}) c USING(case_id)
            WHERE fc.competitor_selected
        ), totals AS (
            SELECT p.case_id, p.focal_id, p.product_id, p.is_focal,
                   coalesce(sum(d.rating_count), 0)::BIGINT AS review_activity_count
            FROM products p
            LEFT JOIN read_parquet({d}) d
              ON p.source_partition=d.source_partition
             AND p.product_id=d.product_id
             AND d.event_date >= p.evaluation_start
             AND d.event_date < p.evaluation_end_exclusive
            GROUP BY p.case_id, p.focal_id, p.product_id, p.is_focal
        ), ranked AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY case_id, focal_id
                       ORDER BY review_activity_count DESC, product_id
                   )::BIGINT AS review_activity_rank,
                   sum(review_activity_count) OVER (
                       PARTITION BY case_id, focal_id
                   )::BIGINT AS focal_total
            FROM totals
        )
        SELECT case_id, focal_id, product_id, is_focal,
               review_activity_count,
               CASE WHEN focal_total > 0
                    THEN review_activity_count::DOUBLE / focal_total
                    ELSE 0.0 END AS review_activity_share,
               review_activity_rank
        FROM ranked
        ORDER BY case_id, focal_id, review_activity_rank, product_id
    """, destination)
