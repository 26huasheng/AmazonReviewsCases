from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


EMPTY_DAILY_SELECT = """
SELECT CAST(NULL AS VARCHAR) AS source_partition,
       CAST(NULL AS VARCHAR) AS product_id,
       CAST(NULL AS DATE) AS event_date,
       CAST(NULL AS BIGINT) AS rating_count,
       CAST(NULL AS DOUBLE) AS rating_sum,
       CAST(NULL AS BIGINT) AS r1_count,
       CAST(NULL AS BIGINT) AS r2_count,
       CAST(NULL AS BIGINT) AS r3_count,
       CAST(NULL AS BIGINT) AS r4_count,
       CAST(NULL AS BIGINT) AS r5_count,
       CAST(NULL AS BIGINT) AS non_1_to_5_numeric_count
WHERE FALSE
"""


def write_bucket_daily(
    con: duckdb.DuckDBPyConnection,
    events_parquet: Path,
    source_partition: str,
    destination: Path,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    if part.exists():
        part.unlink()
    events = sql_literal(str(events_parquet))
    partition = sql_literal(source_partition)
    con.execute(f"""
        COPY (
            SELECT {partition} AS source_partition,
                   product_id,
                   try(to_timestamp(event_time_ms / 1000.0))::DATE AS event_date,
                   count(*) AS rating_count,
                   sum(rating_value) AS rating_sum,
                   count(*) FILTER (rating_value = 1) AS r1_count,
                   count(*) FILTER (rating_value = 2) AS r2_count,
                   count(*) FILTER (rating_value = 3) AS r3_count,
                   count(*) FILTER (rating_value = 4) AS r4_count,
                   count(*) FILTER (rating_value = 5) AS r5_count,
                   count(*) FILTER (rating_value NOT IN (1, 2, 3, 4, 5))
                       AS non_1_to_5_numeric_count
            FROM read_parquet({events}, hive_partitioning=false)
            WHERE product_id IS NOT NULL AND trim(product_id) <> ''
              AND event_time_ms IS NOT NULL
              AND rating_value IS NOT NULL
              AND try(to_timestamp(event_time_ms / 1000.0))::DATE IS NOT NULL
            GROUP BY product_id, event_date
        ) TO {sql_literal(str(part))} (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    part.replace(destination)


def merge_parquet_files(
    con: duckdb.DuckDBPyConnection,
    files: list[Path],
    destination: Path,
    empty_select_sql: str,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    if part.exists():
        part.unlink()
    existing = [path for path in files if path.is_file()]
    if not existing:
        con.execute(
            f"COPY ({empty_select_sql}) TO {sql_literal(str(part))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        part.replace(destination)
        return
    listed = ", ".join(sql_literal(str(path)) for path in existing)
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet([{listed}], union_by_name=true)
        ) TO {sql_literal(str(part))} (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    part.replace(destination)


def observation_range(con: duckdb.DuckDBPyConnection, daily_files: list[Path]) -> dict[str, str | None]:
    existing = [path for path in daily_files if path.is_file()]
    if not existing:
        return {"start_date": None, "end_date": None}
    listed = ", ".join(sql_literal(str(path)) for path in existing)
    start, end = con.execute(f"""
        SELECT min(event_date)::VARCHAR, max(event_date)::VARCHAR
        FROM read_parquet([{listed}], union_by_name=true)
    """).fetchone()
    return {"start_date": start, "end_date": end}
