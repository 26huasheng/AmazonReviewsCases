from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


def _merge_parts(copy_atomic, parts: list[Path], destination: Path) -> None:
    if not parts:
        copy_atomic("""
            SELECT CAST(NULL AS VARCHAR) AS case_id WHERE FALSE
        """, destination)
        return
    listed = ", ".join(sql_literal(str(path)) for path in parts)
    copy_atomic(f"""
        SELECT * FROM read_parquet([{listed}], union_by_name=true)
        ORDER BY case_id, user_id
    """, destination)


def write_case_market_users(
    con: duckdb.DuckDBPyConnection,
    cases: Path,
    user_history: Path,
    user_market_history: Path,
    destination: Path,
    copy_atomic,
    *,
    work_dir: Path,
    min_history_products: int,
    max_days_since_last_event: int,
) -> None:
    """Find every quality Market-history user from the full cumulative index.

    This path never goes through a category random cap.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    markets = [
        str(row[0])
        for row in con.execute(
            "SELECT DISTINCT market_id FROM read_parquet(?) ORDER BY 1",
            [str(cases)],
        ).fetchall()
    ]
    cases_sql = sql_literal(str(cases))
    gh = sql_literal(str(user_history))
    mh = sql_literal(str(user_market_history))
    parts: list[Path] = []
    for i, market_id in enumerate(markets):
        mid = sql_literal(market_id)
        part = work_dir / f"case_market_users_{i:04d}.parquet"
        copy_atomic(f"""
            WITH cases_m AS (
                SELECT case_id, market_id, source_partition, population_cutoff,
                       population_cutoff_policy
                FROM read_parquet({cases_sql})
                WHERE market_id={mid}
            ), latest_mh AS (
                SELECT c.case_id,
                       c.market_id,
                       c.source_partition,
                       c.population_cutoff,
                       c.population_cutoff_policy,
                       h.user_id,
                       h.cumulative_product_count::BIGINT AS market_history_product_count,
                       h.cumulative_event_count::BIGINT AS market_history_event_count
                FROM cases_m c
                JOIN read_parquet({mh}) h
                  ON c.market_id=h.market_id
                 AND h.event_date < c.population_cutoff
                QUALIFY row_number() OVER (
                    PARTITION BY c.case_id, h.user_id
                    ORDER BY h.event_date DESC
                )=1
            ), with_global AS (
                SELECT m.*,
                       coalesce(g.cumulative_product_count, 0)::BIGINT AS history_product_count,
                       coalesce(g.cumulative_event_count, 0)::BIGINT AS history_event_count,
                       g.event_date AS last_event_date
                FROM latest_mh m
                ASOF LEFT JOIN read_parquet({gh}) g
                  ON m.user_id=g.user_id
                 AND g.event_date < m.population_cutoff
            )
            SELECT case_id,
                   user_id,
                   market_id,
                   source_partition,
                   population_cutoff,
                   population_cutoff_policy,
                   history_product_count,
                   history_event_count,
                   CASE WHEN last_event_date IS NULL THEN NULL
                        ELSE date_diff('day', last_event_date, population_cutoff)::BIGINT
                   END AS days_since_last_event,
                   market_history_product_count,
                   market_history_event_count,
                   'market_history'::VARCHAR AS user_group
            FROM with_global
            WHERE market_history_product_count > 0
              AND history_product_count >= {int(min_history_products)}
              AND last_event_date IS NOT NULL
              AND date_diff('day', last_event_date, population_cutoff)
                    <= {int(max_days_since_last_event)}
            ORDER BY case_id, user_id
        """, part)
        parts.append(part)
    _merge_parts(copy_atomic, parts, destination)


def write_case_background_users(
    con: duckdb.DuckDBPyConnection,
    cases: Path,
    user_summary: Path,
    user_history: Path,
    user_category_history: Path,
    user_market_history: Path,
    case_market_users: Path,
    destination: Path,
    copy_atomic,
    *,
    work_dir: Path,
    min_history_products: int,
    max_days_since_last_event: int,
    candidate_pool: int,
    target_users_per_case: int,
    seed: str,
) -> None:
    """Sample quality category users with no Market history before cutoff."""
    if candidate_pool <= 0 or target_users_per_case <= 0:
        raise ValueError("background pool sizes must be positive")
    work_dir.mkdir(parents=True, exist_ok=True)
    markets = con.execute("""
        SELECT DISTINCT market_id, source_partition
        FROM read_parquet(?)
        ORDER BY 1
    """, [str(cases)]).fetchall()
    cases_sql = sql_literal(str(cases))
    users = sql_literal(str(user_summary))
    gh = sql_literal(str(user_history))
    ch = sql_literal(str(user_category_history))
    mh = sql_literal(str(user_market_history))
    market_users = sql_literal(str(case_market_users))
    seed_sql = sql_literal(seed)
    parts: list[Path] = []
    for i, (market_id, source_partition) in enumerate(markets):
        mid = sql_literal(str(market_id))
        partn = sql_literal(str(source_partition))
        part = work_dir / f"case_background_users_{i:04d}.parquet"
        copy_atomic(f"""
            WITH cases_m AS (
                SELECT case_id, market_id, source_partition, population_cutoff,
                       population_cutoff_policy
                FROM read_parquet({cases_sql})
                WHERE market_id={mid}
            ), candidates AS (
                SELECT user_id
                FROM read_parquet({users})
                WHERE source_partition={partn}
                QUALIFY row_number() OVER (
                    ORDER BY sha256(CAST(to_json(list_value(
                        {seed_sql}, {mid}, user_id
                    )) AS VARCHAR)), user_id
                ) <= {int(candidate_pool)}
            ), base AS (
                SELECT c.case_id, c.market_id, c.source_partition, c.population_cutoff,
                       c.population_cutoff_policy, cand.user_id
                FROM cases_m c
                CROSS JOIN candidates cand
            ), with_global AS (
                SELECT b.*,
                       coalesce(g.cumulative_product_count, 0)::BIGINT AS history_product_count,
                       coalesce(g.cumulative_event_count, 0)::BIGINT AS history_event_count,
                       g.event_date AS last_event_date
                FROM base b
                ASOF LEFT JOIN read_parquet({gh}) g
                  ON b.user_id=g.user_id
                 AND g.event_date < b.population_cutoff
            ), with_category AS (
                SELECT b.*,
                       coalesce(g.cumulative_product_count, 0)::BIGINT
                           AS category_history_product_count
                FROM with_global b
                ASOF LEFT JOIN read_parquet({ch}) g
                  ON b.source_partition=g.source_partition
                 AND b.user_id=g.user_id
                 AND g.event_date < b.population_cutoff
            ), with_market AS (
                SELECT b.*,
                       coalesce(g.cumulative_product_count, 0)::BIGINT
                           AS market_history_product_count,
                       coalesce(g.cumulative_event_count, 0)::BIGINT
                           AS market_history_event_count
                FROM with_category b
                ASOF LEFT JOIN read_parquet({mh}) g
                  ON b.market_id=g.market_id
                 AND b.user_id=g.user_id
                 AND g.event_date < b.population_cutoff
            ), eligible AS (
                SELECT wm.*,
                       CASE WHEN wm.last_event_date IS NULL THEN NULL
                            ELSE date_diff('day', wm.last_event_date, wm.population_cutoff)::BIGINT
                       END AS days_since_last_event
                FROM with_market wm
                WHERE wm.history_product_count >= {int(min_history_products)}
                  AND wm.last_event_date IS NOT NULL
                  AND date_diff('day', wm.last_event_date, wm.population_cutoff)
                        <= {int(max_days_since_last_event)}
                  AND coalesce(wm.market_history_product_count, 0)=0
                  AND NOT EXISTS (
                      SELECT 1 FROM read_parquet({market_users}) mu
                      WHERE mu.case_id=wm.case_id
                        AND mu.user_id=wm.user_id
                  )
            ), ranked AS (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY case_id
                           ORDER BY sha256(CAST(to_json(list_value(
                               {seed_sql}, case_id, user_id
                           )) AS VARCHAR)), user_id
                       ) AS sample_rank
                FROM eligible
            )
            SELECT case_id,
                   user_id,
                   market_id,
                   source_partition,
                   population_cutoff,
                   population_cutoff_policy,
                   history_product_count,
                   history_event_count,
                   days_since_last_event,
                   market_history_product_count,
                   market_history_event_count,
                   'category_only'::VARCHAR AS user_group,
                   sample_rank::BIGINT
            FROM ranked
            WHERE sample_rank <= {int(target_users_per_case)}
            ORDER BY case_id, user_id
        """, part)
        parts.append(part)
    _merge_parts(copy_atomic, parts, destination)


def write_case_population_union(
    con: duckdb.DuckDBPyConnection,
    case_market_users: Path,
    case_background_users: Path,
    destination: Path,
    copy_atomic,
) -> None:
    market = sql_literal(str(case_market_users))
    background = sql_literal(str(case_background_users))
    copy_atomic(f"""
        SELECT case_id, user_id, market_id, source_partition, population_cutoff,
               population_cutoff_policy, history_product_count, history_event_count,
               days_since_last_event, market_history_product_count,
               market_history_event_count, user_group
        FROM read_parquet({market})
        UNION ALL
        SELECT case_id, user_id, market_id, source_partition, population_cutoff,
               population_cutoff_policy, history_product_count, history_event_count,
               days_since_last_event, market_history_product_count,
               market_history_event_count, user_group
        FROM read_parquet({background})
        ORDER BY case_id, user_group, user_id
    """, destination)
