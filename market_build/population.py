from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


POPULATION_SOURCES = {"category", "global"}


def write_market_population(
    con: duckdb.DuckDBPyConnection,
    market_products: Path,
    user_summary: Path,
    destination: Path,
    copy_atomic,
    *,
    population_source: str,
    population_size: int | None = None,
    seed: str = "market_population_v1",
) -> None:
    """给每个 Market 固定一批共享候选用户。

    这一步只看 Market/source_partition 与大类用户池，不看任何 Case future outcome。
    """
    if population_source not in POPULATION_SOURCES:
        raise ValueError(
            f"population_source must be one of {sorted(POPULATION_SOURCES)}"
        )
    if population_size is not None and population_size <= 0:
        raise ValueError("population_size must be positive or None")
    products = sql_literal(str(market_products))
    users = sql_literal(str(user_summary))
    seed_sql = sql_literal(seed)

    markets = con.execute(f"""
        SELECT DISTINCT market_id, source_partition
        FROM read_parquet({products})
        ORDER BY source_partition, market_id
    """).fetchall()
    if not markets:
        copy_atomic("""
            SELECT CAST(NULL AS VARCHAR) AS market_id,
                   CAST(NULL AS VARCHAR) AS source_partition,
                   CAST(NULL AS VARCHAR) AS user_id,
                   CAST(NULL AS VARCHAR) AS population_source,
                   CAST(NULL AS BIGINT) AS sampling_rank
            WHERE FALSE
        """, destination)
        return

    if population_size is None:
        if population_source == "category":
            pool = f"""
                SELECT m.market_id, m.source_partition, u.user_id
                FROM (
                    SELECT DISTINCT market_id, source_partition
                    FROM read_parquet({products})
                ) m
                JOIN read_parquet({users}) u
                  ON m.source_partition=u.source_partition
            """
        else:
            pool = f"""
                SELECT m.market_id, m.source_partition, u.user_id
                FROM (
                    SELECT DISTINCT market_id, source_partition
                    FROM read_parquet({products})
                ) m
                CROSS JOIN (
                    SELECT DISTINCT user_id FROM read_parquet({users})
                ) u
            """
        copy_atomic(f"""
            WITH pool AS ({pool}), ranked AS (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY market_id
                           ORDER BY sha256(CAST(to_json(list_value(
                               {seed_sql}, market_id, user_id
                           )) AS VARCHAR)), user_id
                       ) AS sampling_rank
                FROM pool
            )
            SELECT market_id, source_partition, user_id,
                   {sql_literal(population_source)}::VARCHAR AS population_source,
                   sampling_rank::BIGINT AS sampling_rank
            FROM ranked
            ORDER BY market_id, sampling_rank
        """, destination)
        return

    con.execute("DROP TABLE IF EXISTS market_user_pool")
    con.execute(f"""
        CREATE TEMP TABLE market_user_pool AS
        SELECT source_partition, user_id
        FROM read_parquet({users})
    """)
    con.execute("DROP TABLE IF EXISTS market_population_sampled")
    con.execute("""
        CREATE TEMP TABLE market_population_sampled(
            market_id VARCHAR,
            source_partition VARCHAR,
            user_id VARCHAR,
            population_source VARCHAR,
            sampling_rank BIGINT
        )
    """)
    source_sql = sql_literal(population_source)
    size = int(population_size)
    for market_id, source_partition in markets:
        mid = sql_literal(str(market_id))
        part = sql_literal(str(source_partition))
        if population_source == "category":
            user_from = f"SELECT user_id FROM market_user_pool WHERE source_partition={part}"
        else:
            user_from = "SELECT DISTINCT user_id FROM market_user_pool"
        con.execute(f"""
            INSERT INTO market_population_sampled
            SELECT {mid}, {part}, user_id, {source_sql}, sampling_rank
            FROM (
                SELECT user_id,
                       row_number() OVER (
                           ORDER BY sha256(CAST(to_json(list_value(
                               {seed_sql}, {mid}, user_id
                           )) AS VARCHAR)), user_id
                       ) AS sampling_rank
                FROM ({user_from}) u
            )
            WHERE sampling_rank <= {size}
        """)
    copy_atomic("""
        SELECT market_id, source_partition, user_id, population_source, sampling_rank
        FROM market_population_sampled
        ORDER BY market_id, sampling_rank
    """, destination)
