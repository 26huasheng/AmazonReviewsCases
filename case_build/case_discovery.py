from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from utils import sql_literal


def write_active_product_count_events(
    con: duckdb.DuckDBPyConnection,
    timeline_path: Path,
    events_path: Path,
    cumulative_path: Path,
    copy_atomic,
) -> int:
    """用区间事件线性计算每个 Market 在任意日期已有多少活跃商品。

    商品从 first_rating_date 的下一天开始计入“已有商品”，这样某商品在自己的
    首评日不会把自己算作 competitor；last_rating_date 当天仍视为活跃。
    """
    timeline = sql_literal(str(timeline_path))
    copy_atomic(f"""
        SELECT source_partition,
               market_id,
               event_date,
               sum(delta)::BIGINT AS delta
        FROM (
            SELECT source_partition,
                   market_id,
                   CAST(first_rating_date + INTERVAL 1 DAY AS DATE) AS event_date,
                   1 AS delta
            FROM read_parquet({timeline})
            WHERE first_rating_date IS NOT NULL
              AND last_rating_date IS NOT NULL
            UNION ALL
            SELECT source_partition,
                   market_id,
                   CAST(last_rating_date + INTERVAL 1 DAY AS DATE) AS event_date,
                   -1 AS delta
            FROM read_parquet({timeline})
            WHERE first_rating_date IS NOT NULL
              AND last_rating_date IS NOT NULL
        )
        GROUP BY source_partition, market_id, event_date
    """, events_path)

    copy_atomic(f"""
        SELECT source_partition,
               market_id,
               event_date,
               delta,
               sum(delta) OVER (
                   PARTITION BY source_partition, market_id
                   ORDER BY event_date
               )::BIGINT AS active_product_count
        FROM read_parquet({sql_literal(str(events_path))})
    """, cumulative_path)

    return int(con.execute(
        "SELECT count(*) FROM read_parquet(?)", [str(events_path)]
    ).fetchone()[0])


def attach_active_competitor_counts(
    con: duckdb.DuckDBPyConnection,
    timeline_path: Path,
    cumulative_path: Path,
) -> None:
    timeline = sql_literal(str(timeline_path))
    cumulative = sql_literal(str(cumulative_path))
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW timeline_with_active_competitor_count AS
        SELECT t.*,
               coalesce(c.active_product_count, 0)::BIGINT
                   AS active_competitor_count_at_t0
        FROM read_parquet({timeline}) t
        ASOF LEFT JOIN read_parquet({cumulative}) c
          ON t.source_partition = c.source_partition
         AND t.market_id = c.market_id
         AND c.event_date <= t.entry_date
    """)


def write_case_candidates(
    con: duckdb.DuckDBPyConnection,
    observation_end: date,
    destination: Path,
    copy_atomic,
    *,
    evaluation_days: int,
) -> None:
    """Market 内每个商品先形成一个候选新品进入事件。

    这里只做结构性检查：t0 是否存在、未来评测窗口是否完整。
    不使用未来评论量阈值，也不使用 competitor 数量阈值提前淘汰 Case。
    """
    if evaluation_days <= 0:
        raise ValueError("evaluation_days must be positive")
    end = sql_literal(observation_end.isoformat())
    copy_atomic(f"""
        SELECT
            case_candidate_id,
            case_candidate_id AS focal_candidate_id,
            source_partition,
            discovery_version,
            market_id,
            market_label,
            product_id AS focal_product_id,
            product_title AS focal_product_title,
            entry_date AS t0,
            entry_date_source,
            first_rating_date,
            last_rating_date,
            post90_rating_count,
            active_competitor_count_at_t0,
            entry_date AS evaluation_start,
            CAST(entry_date + INTERVAL {int(evaluation_days)} DAY AS DATE)
                AS evaluation_end_exclusive,
            {int(evaluation_days)}::BIGINT AS evaluation_days,
            (entry_date IS NOT NULL AND first_rating_date IS NOT NULL) AS valid_t0,
            (
                entry_date IS NOT NULL
                AND CAST(entry_date + INTERVAL {int(evaluation_days)} DAY AS DATE)
                    <= DATE {end} + INTERVAL 1 DAY
            ) AS evaluation_window_complete,
            CASE
                WHEN entry_date IS NULL OR first_rating_date IS NULL
                    THEN 'invalid_t0'
                WHEN CAST(entry_date + INTERVAL {int(evaluation_days)} DAY AS DATE)
                       > DATE {end} + INTERVAL 1 DAY
                    THEN 'incomplete_evaluation_window'
                ELSE NULL
            END AS structural_exclusion_reason
        FROM (
            SELECT
                'case_candidate_' || substr(
                    sha256(CAST(to_json(list_value(
                        source_partition,
                        market_id,
                        product_id,
                        CAST(entry_date AS VARCHAR)
                    )) AS VARCHAR)),
                    1,
                    20
                ) AS case_candidate_id,
                *
            FROM timeline_with_active_competitor_count
        ) timeline_candidates
    """, destination)
