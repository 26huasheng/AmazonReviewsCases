from __future__ import annotations

import shutil
from pathlib import Path
from urllib.parse import unquote

import duckdb

from utils import sql_literal

from .compressed_storage import (
    bucket_directory_width,
    event_store_metadata,
    validate_bucket_count,
)


def write_rating_event_store(
    con: duckdb.DuckDBPyConnection,
    cache_dir: Path,
    tmp_dir: Path,
    bucket_count: int,
) -> Path:
    bucket_count = validate_bucket_count(bucket_count)
    event_store = cache_dir / "rating_event_store"
    width = bucket_directory_width(bucket_count)
    bucket_expression = (
        "CAST(CAST('0x' || substr(sha256(product_id), 1, 16) AS UBIGINT) "
        f"% {bucket_count} AS BIGINT)"
    )
    bucket_label = f"printf('%0{width}d', {bucket_expression})"
    build_root = tmp_dir / "rating_event_store_build"
    if build_root.exists():
        shutil.rmtree(build_root)
    staging = build_root / "staging"
    completed = build_root / "completed"
    completed.mkdir(parents=True)
    con.execute(f"""
        COPY (
            SELECT source_partition, {bucket_label} AS bucket,
                   product_id, consumer_id, rating_value, event_time_ms
            FROM rating_events
        ) TO {sql_literal(str(staging))}
          (FORMAT PARQUET, COMPRESSION ZSTD,
           PARTITION_BY (source_partition, bucket))
    """)

    previous_threads = int(con.execute("SELECT current_setting('threads')").fetchone()[0])
    previous_preserve_order = bool(
        con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]
    )
    try:
        con.execute("SET threads=1")
        con.execute("SET preserve_insertion_order=true")
        for bucket_dir in sorted(staging.glob("source_partition=*/bucket=*")):
            destination = completed / bucket_dir.relative_to(staging)
            destination.mkdir(parents=True, exist_ok=True)
            con.execute(f"""
                COPY (
                    SELECT product_id, consumer_id, rating_value, event_time_ms
                    FROM read_parquet(
                        {sql_literal(str(bucket_dir / '*.parquet'))},
                        hive_partitioning=false
                    )
                    ORDER BY product_id, event_time_ms
                ) TO {sql_literal(str(destination / 'events.parquet'))}
                  (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
    finally:
        con.execute(f"SET threads={previous_threads}")
        con.execute(f"SET preserve_insertion_order={str(previous_preserve_order).lower()}")
    if event_store.exists():
        shutil.rmtree(event_store)
    completed.replace(event_store)
    shutil.rmtree(build_root, ignore_errors=True)
    return event_store


def iter_event_store_buckets(event_store: Path) -> list[tuple[str, str, Path]]:
    rows: list[tuple[str, str, Path]] = []
    for path in sorted(event_store.glob("source_partition=*/bucket=*/events.parquet")):
        partition_dir, bucket_dir = path.parent.parent.name, path.parent.name
        source_partition = unquote(partition_dir.split("=", 1)[1])
        bucket = bucket_dir.split("=", 1)[1]
        rows.append((source_partition, bucket, path))
    return rows


def rating_event_store_metadata(bucket_count: int) -> dict[str, object]:
    return event_store_metadata(bucket_count)
