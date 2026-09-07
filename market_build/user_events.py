from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from utils import sql_literal, write_json


USER_COLUMNS = ("user_id", "consumer_id", "reviewer_id", "stable_user_key")
PRODUCT_COLUMNS = ("product_id", "parent_asin", "asin", "item_id")
TIME_COLUMNS = ("event_time_ms", "timestamp", "event_timestamp", "event_date")
RATING_COLUMNS = ("rating", "rating_value")
VERIFIED_COLUMNS = ("verified_purchase", "verified")


def _first(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    return next((name for name in candidates if name in columns), None)


def _source_relation(events: Path) -> tuple[str, str]:
    if events.is_dir():
        if list(events.glob("source_partition=*/bucket=*/events.parquet")):
            pattern = events / "source_partition=*" / "bucket=*" / "events.parquet"
            return (
                f"read_parquet({sql_literal(str(pattern))}, hive_partitioning=true)",
                "v5_rating_event_store",
            )
        if list(events.rglob("*.parquet")):
            pattern = events / "**" / "*.parquet"
            return (
                f"read_parquet({sql_literal(str(pattern))}, union_by_name=true, hive_partitioning=true)",
                "parquet_directory",
            )
        raise ValueError(f"no parquet files under {events}")
    if events.suffix.lower() == ".parquet":
        return f"read_parquet({sql_literal(str(events))})", "parquet"
    if events.suffix.lower() == ".jsonl":
        return (
            f"read_json({sql_literal(str(events))}, format='newline_delimited', "
            "ignore_errors=true, maximum_object_size=134217728)",
            "jsonl_reviews",
        )
    if events.suffix.lower() in {".csv", ".tsv"}:
        delim = "'\\t'" if events.suffix.lower() == ".tsv" else "','"
        return (
            f"read_csv_auto({sql_literal(str(events))}, delim={delim}, header=true, sample_size=-1)",
            "delimited_text",
        )
    raise ValueError(f"unsupported event source: {events}")


def register_canonical_user_events(
    con: duckdb.DuckDBPyConnection,
    events: Path,
    *,
    fallback_source_partition: str | None = None,
    view_name: str = "canonical_user_events",
) -> dict[str, Any]:
    """把 AmazonReviewrepo v5 或普通事件表统一成固定字段视图。"""
    relation, source_kind = _source_relation(events)
    columns = {
        str(row[0])
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }
    user_col = _first(columns, USER_COLUMNS)
    product_col = _first(columns, PRODUCT_COLUMNS)
    time_col = _first(columns, TIME_COLUMNS)
    rating_col = _first(columns, RATING_COLUMNS)
    verified_col = _first(columns, VERIFIED_COLUMNS)
    if not user_col or not product_col or not time_col:
        raise ValueError(
            "event source must contain user/product/time; "
            f"columns={sorted(columns)}"
        )
    if "source_partition" in columns:
        source_expr = "CAST(source_partition AS VARCHAR)"
    elif fallback_source_partition:
        source_expr = f"{sql_literal(fallback_source_partition)}::VARCHAR"
    else:
        raise ValueError("source_partition missing; provide fallback_source_partition")

    if time_col == "event_time_ms":
        ts_expr = f"try(to_timestamp(CAST({time_col} AS DOUBLE) / 1000.0))"
    elif time_col == "timestamp":
        ts_expr = (
            f"CASE WHEN try_cast({time_col} AS DOUBLE) IS NOT NULL THEN "
            f"try(to_timestamp(CASE WHEN try_cast({time_col} AS DOUBLE) > 100000000000 "
            f"THEN try_cast({time_col} AS DOUBLE) / 1000.0 "
            f"ELSE try_cast({time_col} AS DOUBLE) END)) "
            f"ELSE try_cast({time_col} AS TIMESTAMP) END"
        )
    else:
        ts_expr = f"try_cast({time_col} AS TIMESTAMP)"
    rating_expr = f"try_cast({rating_col} AS DOUBLE)" if rating_col else "NULL::DOUBLE"
    verified_expr = (
        f"try_cast({verified_col} AS BOOLEAN)" if verified_col else "NULL::BOOLEAN"
    )
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT {source_expr} AS source_partition,
               CAST({user_col} AS VARCHAR) AS user_id,
               CAST({product_col} AS VARCHAR) AS product_id,
               {ts_expr} AS event_timestamp,
               CAST({ts_expr} AS DATE) AS event_date,
               {rating_expr} AS rating,
               {verified_expr} AS verified_purchase
        FROM {relation}
        WHERE {user_col} IS NOT NULL
          AND trim(CAST({user_col} AS VARCHAR)) <> ''
          AND {product_col} IS NOT NULL
          AND trim(CAST({product_col} AS VARCHAR)) <> ''
          AND {ts_expr} IS NOT NULL
    """)
    return {
        "source_kind": source_kind,
        "source_columns": sorted(columns),
        "resolved_user_column": user_col,
        "resolved_product_column": product_col,
        "resolved_time_column": time_col,
        "resolved_rating_column": rating_col,
        "resolved_verified_column": verified_col,
    }


def write_canonical_user_events(
    con: duckdb.DuckDBPyConnection,
    events: Path,
    destination: Path,
    copy_atomic,
    *,
    fallback_source_partition: str | None = None,
) -> dict[str, Any]:
    meta = register_canonical_user_events(
        con,
        events,
        fallback_source_partition=fallback_source_partition,
    )
    copy_atomic("""
        SELECT source_partition, user_id, product_id,
               event_timestamp, event_date, rating, verified_purchase
        FROM canonical_user_events
        ORDER BY source_partition, user_id, event_timestamp, product_id
    """, destination)
    return meta


def write_user_event_store(
    con: duckdb.DuckDBPyConnection,
    canonical_events: Path,
    destination_dir: Path,
    *,
    bucket_count: int = 256,
) -> None:
    """按用户哈希分桶，供后续大量 user×t0 ASOF 查询。"""
    if bucket_count <= 0:
        raise ValueError("bucket_count must be positive")
    destination_dir.mkdir(parents=True, exist_ok=True)
    source = sql_literal(str(canonical_events))
    bucket = (
        "CAST(CAST('0x' || substr(sha256(user_id), 1, 16) AS UBIGINT) "
        f"% {int(bucket_count)} AS BIGINT)"
    )
    con.execute(f"""
        COPY (
            SELECT source_partition,
                   printf('%03d', {bucket}) AS user_bucket,
                   user_id, product_id, event_timestamp, event_date,
                   rating, verified_purchase
            FROM read_parquet({source})
        ) TO {sql_literal(str(destination_dir))}
          (FORMAT PARQUET, COMPRESSION ZSTD,
           PARTITION_BY (source_partition, user_bucket), OVERWRITE_OR_IGNORE true)
    """)


def write_user_event_store_manifest(
    path: Path,
    *,
    source: Path,
    bucket_count: int,
    source_metadata: dict[str, Any],
) -> None:
    write_json(path, {
        "schema_version": "user_event_store_v1",
        "source": str(source),
        "bucket_count": bucket_count,
        "canonical_columns": [
            "source_partition", "user_id", "product_id", "event_timestamp",
            "event_date", "rating", "verified_purchase",
        ],
        "source_metadata": source_metadata,
    })
