from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


EMPTY_PRODUCT_TIME_SELECT = """
SELECT CAST(NULL AS VARCHAR) AS source_partition,
       CAST(NULL AS VARCHAR) AS product_id,
       CAST(NULL AS DATE) AS entry_date,
       CAST(NULL AS DATE) AS first_rating_date,
       CAST(NULL AS DATE) AS last_rating_date,
       CAST(NULL AS BIGINT) AS total_rating_count,
       CAST(NULL AS BIGINT) AS post90_rating_count
WHERE FALSE
"""


def write_bucket_product_time(
    con: duckdb.DuckDBPyConnection,
    bucket_daily: Path,
    destination: Path,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    if part.exists():
        part.unlink()
    daily = sql_literal(str(bucket_daily))
    con.execute("DROP TABLE IF EXISTS bucket_daily_src")
    con.execute("DROP TABLE IF EXISTS bucket_first_dates")
    con.execute(f"""
        CREATE TEMP TABLE bucket_daily_src AS
        SELECT source_partition, product_id, event_date, rating_count
        FROM read_parquet({daily})
    """)
    con.execute("""
        CREATE TEMP TABLE bucket_first_dates AS
        SELECT source_partition, product_id,
               min(event_date) AS first_rating_date,
               max(event_date) AS last_rating_date
        FROM bucket_daily_src
        GROUP BY source_partition, product_id
    """)
    con.execute(f"""
        COPY (
            SELECT d.source_partition,
                   d.product_id,
                   f.first_rating_date AS entry_date,
                   f.first_rating_date,
                   f.last_rating_date,
                   sum(d.rating_count) AS total_rating_count,
                   sum(d.rating_count) FILTER (
                       d.event_date >= f.first_rating_date
                       AND d.event_date < f.first_rating_date + INTERVAL 90 DAY
                   ) AS post90_rating_count
            FROM bucket_daily_src d
            INNER JOIN bucket_first_dates f
              ON d.source_partition = f.source_partition
             AND d.product_id = f.product_id
            GROUP BY d.source_partition, d.product_id, f.first_rating_date, f.last_rating_date
        ) TO {sql_literal(str(part))} (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.execute("DROP TABLE IF EXISTS bucket_daily_src")
    con.execute("DROP TABLE IF EXISTS bucket_first_dates")
    part.replace(destination)
