from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


def _feature_sql(
    cases: Path,
    market_population: Path,
    user_history: Path,
    user_category_history: Path,
    user_market_history: Path,
    market_id: str | None = None,
) -> str:
    c = sql_literal(str(cases))
    mp = sql_literal(str(market_population))
    gh = sql_literal(str(user_history))
    ch = sql_literal(str(user_category_history))
    mh = sql_literal(str(user_market_history))
    market_filter = ""
    if market_id is not None:
        market_filter = f"WHERE c.market_id={sql_literal(market_id)}"
    return f"""
        WITH base AS (
            SELECT c.case_id,
                   c.market_id,
                   c.source_partition,
                   c.population_cutoff,
                   c.population_cutoff_policy,
                   p.user_id,
                   p.population_source,
                   p.sampling_rank AS market_population_rank
            FROM read_parquet({c}) c
            JOIN read_parquet({mp}) p
              ON c.market_id=p.market_id
            {market_filter}
        ), with_global AS (
            SELECT b.*,
                   coalesce(g.cumulative_event_count, 0)::BIGINT AS history_event_count,
                   coalesce(g.cumulative_product_count, 0)::BIGINT AS history_product_count,
                   g.event_date AS last_event_date
            FROM base b
            ASOF LEFT JOIN read_parquet({gh}) g
              ON b.user_id=g.user_id
             AND g.event_date < b.population_cutoff
        ), with_category AS (
            SELECT b.*,
                   coalesce(g.cumulative_event_count, 0)::BIGINT AS category_history_event_count,
                   coalesce(g.cumulative_product_count, 0)::BIGINT AS category_history_product_count
            FROM with_global b
            ASOF LEFT JOIN read_parquet({ch}) g
              ON b.source_partition=g.source_partition
             AND b.user_id=g.user_id
             AND g.event_date < b.population_cutoff
        ), with_market AS (
            SELECT b.*,
                   coalesce(g.cumulative_event_count, 0)::BIGINT AS market_history_event_count,
                   coalesce(g.cumulative_product_count, 0)::BIGINT AS market_history_product_count
            FROM with_category b
            ASOF LEFT JOIN read_parquet({mh}) g
              ON b.market_id=g.market_id
             AND b.user_id=g.user_id
             AND g.event_date < b.population_cutoff
        )
        SELECT *,
               CASE WHEN last_event_date IS NULL THEN NULL
                    ELSE date_diff('day', last_event_date, population_cutoff)::BIGINT END
                   AS days_since_last_event,
               CASE
                   WHEN market_history_product_count > 0 THEN 'market_history'
                   WHEN category_history_product_count > 0 THEN 'category_only'
                   ELSE 'outside_category'
               END AS relation_stratum
        FROM with_market
        ORDER BY case_id, user_id
    """


def write_case_user_features(
    con: duckdb.DuckDBPyConnection,
    cases: Path,
    market_population: Path,
    user_history: Path,
    user_category_history: Path,
    user_market_history: Path,
    destination: Path,
    copy_atomic,
    *,
    work_dir: Path | None = None,
) -> None:
    """给每个 Case×Market-user 计算只依赖 population_cutoff 以前数据的用户特征。"""
    markets = [
        str(row[0])
        for row in con.execute(
            "SELECT DISTINCT market_id FROM read_parquet(?) ORDER BY 1",
            [str(cases)],
        ).fetchall()
    ]
    if len(markets) <= 8:
        copy_atomic(
            _feature_sql(
                cases, market_population, user_history,
                user_category_history, user_market_history,
            ),
            destination,
        )
        return

    work = (work_dir or destination.parent / "_work")
    work.mkdir(parents=True, exist_ok=True)
    parts: list[Path] = []
    for i, market_id in enumerate(markets):
        part = work / f"case_user_features_{i:04d}.parquet"
        copy_atomic(
            _feature_sql(
                cases, market_population, user_history,
                user_category_history, user_market_history,
                market_id=market_id,
            ),
            part,
        )
        parts.append(part)
    listed = ", ".join(sql_literal(str(path)) for path in parts)
    copy_atomic(f"""
        SELECT * FROM read_parquet([{listed}], union_by_name=true)
        ORDER BY case_id, user_id
    """, destination)
