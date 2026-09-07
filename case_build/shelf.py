from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


def _columns(con: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    return {
        str(row[0])
        for row in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
    }


def write_product_rating_cumulative(
    con: duckdb.DuckDBPyConnection,
    rating_daily: Path,
    destination: Path,
    copy_atomic,
) -> None:
    """一次性建立商品逐日累计评论量/评分和，后续所有 Case 用 ASOF 查询。"""
    columns = _columns(con, rating_daily)
    required = {"source_partition", "product_id", "event_date", "rating_count"}
    missing = required - columns
    if missing:
        raise ValueError(
            f"rating_daily_summary missing columns: {sorted(missing)}"
        )
    rating_sum_expr = (
        "sum(rating_sum)::DOUBLE" if "rating_sum" in columns
        else "NULL::DOUBLE"
    )
    daily = sql_literal(str(rating_daily))
    copy_atomic(f"""
        WITH daily AS (
            SELECT source_partition,
                   product_id,
                   event_date,
                   sum(rating_count)::BIGINT AS rating_count,
                   {rating_sum_expr} AS rating_sum
            FROM read_parquet({daily})
            GROUP BY source_partition, product_id, event_date
        )
        SELECT source_partition,
               product_id,
               event_date,
               sum(rating_count) OVER (
                   PARTITION BY source_partition, product_id
                   ORDER BY event_date
               )::BIGINT AS cumulative_rating_count,
               sum(rating_sum) OVER (
                   PARTITION BY source_partition, product_id
                   ORDER BY event_date
               )::DOUBLE AS cumulative_rating_sum
        FROM daily
    """, destination)


def write_focal_competitor_candidates(
    con: duckdb.DuckDBPyConnection,
    case_focals: Path,
    cases_path: Path,
    timeline_path: Path,
    destination: Path,
    copy_atomic,
) -> None:
    """每个 focal 单独找 t0 时仍活跃的同 Market competitor 池。"""
    focal_columns = _columns(con, case_focals)
    required = {
        "case_id",
        "focal_id",
        "focal_product_id",
        "t0",
    }
    missing = required - focal_columns
    if missing:
        raise ValueError(f"case_focals missing columns: {sorted(missing)}")

    timeline_columns = _columns(con, timeline_path)
    required_timeline = {
        "source_partition",
        "market_id",
        "product_id",
        "product_title",
        "first_rating_date",
        "last_rating_date",
        "metadata_snapshot_price",
    }
    missing_timeline = required_timeline - timeline_columns
    if missing_timeline:
        raise ValueError(
            f"market timeline missing columns: {sorted(missing_timeline)}"
        )

    duplicate_focals = con.execute("""
        SELECT case_id, focal_id, count(*)
        FROM read_parquet(?)
        GROUP BY case_id, focal_id
        HAVING count(*) > 1
        ORDER BY case_id, focal_id
        LIMIT 10
    """, [str(case_focals)]).fetchall()
    if duplicate_focals:
        raise ValueError(f"duplicate case_id/focal_id rows: {duplicate_focals}")

    focals = sql_literal(str(case_focals))
    cases = sql_literal(str(cases_path))
    timeline = sql_literal(str(timeline_path))

    missing_focal = con.execute(f"""
        SELECT f.case_id, f.focal_id, f.focal_product_id
        FROM read_parquet({focals}) f
        JOIN read_parquet({cases}) c USING(case_id)
        LEFT JOIN read_parquet({timeline}) t
          ON c.source_partition = t.source_partition
         AND c.market_id = t.market_id
         AND f.focal_product_id = t.product_id
        WHERE t.product_id IS NULL
        ORDER BY f.case_id, f.focal_id
        LIMIT 10
    """).fetchall()
    if missing_focal:
        raise ValueError(
            f"case focal missing from market timeline: {missing_focal}"
        )

    copy_atomic(f"""
        SELECT f.case_id,
               f.focal_id,
               f.focal_product_id,
               f.t0 AS focal_t0,
               c.source_partition,
               c.market_id,
               c.market_label,
               t.product_id AS competitor_product_id,
               t.product_title AS competitor_product_title,
               t.first_rating_date,
               t.last_rating_date,
               t.metadata_snapshot_price
        FROM read_parquet({focals}) f
        JOIN read_parquet({cases}) c USING(case_id)
        JOIN read_parquet({timeline}) t
          ON c.source_partition = t.source_partition
         AND c.market_id = t.market_id
        WHERE t.product_id <> f.focal_product_id
          AND t.first_rating_date < f.t0
          AND t.last_rating_date >= f.t0
    """, destination)


def write_focal_competitors(
    con: duckdb.DuckDBPyConnection,
    candidate_path: Path,
    cumulative_path: Path,
    destination: Path,
    copy_atomic,
    *,
    recent_window_days: int,
    max_competitors: int,
) -> None:
    if recent_window_days <= 0:
        raise ValueError("recent_window_days must be positive")
    if max_competitors <= 0:
        raise ValueError("max_competitors must be positive")
    members = sql_literal(str(candidate_path))
    cumulative = sql_literal(str(cumulative_path))
    copy_atomic(f"""
        WITH pre_t0 AS (
            SELECT s.*,
                   coalesce(c.cumulative_rating_count, 0)::BIGINT
                       AS pre_t0_review_count,
                   c.cumulative_rating_sum AS pre_t0_rating_sum
            FROM read_parquet({members}) s
            ASOF LEFT JOIN read_parquet({cumulative}) c
              ON s.source_partition = c.source_partition
             AND s.competitor_product_id = c.product_id
             AND c.event_date < s.focal_t0
        ), before_recent_window AS (
            SELECT p.*,
                   coalesce(c.cumulative_rating_count, 0)::BIGINT
                       AS review_count_before_recent_window
            FROM pre_t0 p
            ASOF LEFT JOIN read_parquet({cumulative}) c
              ON p.source_partition = c.source_partition
             AND p.competitor_product_id = c.product_id
             AND c.event_date < CAST(
                 p.focal_t0 - INTERVAL {int(recent_window_days)} DAY AS DATE
             )
        ), featured AS (
            SELECT *,
                   CASE
                       WHEN pre_t0_review_count > 0 AND pre_t0_rating_sum IS NOT NULL
                       THEN pre_t0_rating_sum / pre_t0_review_count
                   END AS pre_t0_rating_mean,
                   (pre_t0_review_count - review_count_before_recent_window)::BIGINT
                       AS pre_t0_recent_review_count
            FROM before_recent_window
        ), ranked AS (
            SELECT *,
                   count(*) OVER (PARTITION BY case_id, focal_id)::BIGINT
                       AS candidate_competitor_count,
                   row_number() OVER (
                       PARTITION BY case_id, focal_id
                       ORDER BY pre_t0_recent_review_count DESC,
                                pre_t0_review_count DESC,
                                competitor_product_id
                   )::BIGINT AS competitor_selection_rank
            FROM featured
        )
        SELECT case_id,
               focal_id,
               focal_product_id,
               focal_t0,
               competitor_product_id,
               competitor_product_title,
               candidate_competitor_count,
               pre_t0_review_count,
               pre_t0_recent_review_count,
               pre_t0_rating_mean,
               competitor_selection_rank,
               (competitor_selection_rank <= {int(max_competitors)}) AS competitor_selected,
               CASE
                   WHEN candidate_competitor_count <= {int(max_competitors)}
                   THEN 'all_kept_pool_le_16'
                   ELSE 'ranked_top16'
               END AS competitor_selection_reason,
               first_rating_date,
               last_rating_date,
               metadata_snapshot_price,
               source_partition,
               market_id,
               market_label
        FROM ranked
        ORDER BY case_id, focal_id, competitor_selection_rank
    """, destination)


def write_case_shelf(
    con: duckdb.DuckDBPyConnection,
    cases_path: Path,
    case_focals: Path,
    selected_competitors: Path,
    timeline_path: Path,
    destination: Path,
    copy_atomic,
) -> None:
    cases = sql_literal(str(cases_path))
    focals = sql_literal(str(case_focals))
    comps = sql_literal(str(selected_competitors))
    timeline = sql_literal(str(timeline_path))
    copy_atomic(f"""
        WITH focal_rows AS (
            SELECT f.case_id, c.source_partition, c.market_id, f.focal_product_id AS product_id,
                   TRUE AS is_focal, FALSE AS is_competitor
            FROM read_parquet({focals}) f
            JOIN read_parquet({cases}) c USING(case_id)
        ), competitor_rows AS (
            SELECT case_id, source_partition, market_id, competitor_product_id AS product_id,
                   FALSE AS is_focal, TRUE AS is_competitor
            FROM read_parquet({comps})
            WHERE competitor_selected
        ), unioned AS (
            SELECT case_id, source_partition, market_id, product_id,
                   max(is_focal) AS is_focal,
                   max(is_competitor) AS is_competitor
            FROM (
                SELECT * FROM focal_rows
                UNION ALL
                SELECT * FROM competitor_rows
            )
            GROUP BY case_id, source_partition, market_id, product_id
        )
        SELECT u.case_id,
               u.source_partition,
               u.market_id,
               u.product_id,
               t.product_title,
               u.is_focal,
               u.is_competitor,
               t.first_rating_date AS first_review_date,
               t.last_rating_date AS last_review_date,
               t.metadata_snapshot_price
        FROM unioned u
        JOIN read_parquet({timeline}) t
          ON u.source_partition=t.source_partition
         AND u.market_id=t.market_id
         AND u.product_id=t.product_id
        ORDER BY u.case_id, u.product_id
    """, destination)
