from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import duckdb

from utils import sql_literal, write_json

from .compressed_storage import DEFAULT_EVENT_STORE_BUCKET_COUNT
from .product_core import write_product_core
from .product_time_summary import EMPTY_PRODUCT_TIME_SELECT, write_bucket_product_time
from .rating_daily import EMPTY_DAILY_SELECT, merge_parquet_files, observation_range, write_bucket_daily
from .rating_event_store import (
    iter_event_store_buckets,
    rating_event_store_metadata,
    write_rating_event_store,
)
from .sources import category_source_paths


def _configure_connection(con: duckdb.DuckDBPyConnection, cache_dir: Path) -> Path:
    duckdb_tmp = cache_dir / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory={sql_literal(str(duckdb_tmp))}")
    con.execute("SET TimeZone='UTC'")
    con.execute("SET threads=4")
    return duckdb_tmp


def _prepare_tmp(cache_dir: Path) -> Path:
    tmp = cache_dir / ".build_inputs_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)
    return tmp


def register_amazon_views(con: duckdb.DuckDBPyConnection, category: str, data_root: Path) -> None:
    paths = category_source_paths(data_root, category)
    rating = sql_literal(str(paths["rating"]))
    metadata = sql_literal(str(paths["metadata"]))
    partition = sql_literal(category)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW rating_events AS
        SELECT {partition} AS source_partition,
               TRY_CAST(parent_asin AS VARCHAR) AS product_id,
               TRY_CAST(user_id AS VARCHAR) AS consumer_id,
               TRY_CAST(rating AS DOUBLE) AS rating_value,
               TRY_CAST(timestamp AS BIGINT) AS event_time_ms,
               try(to_timestamp(TRY_CAST(timestamp AS BIGINT) / 1000.0))::DATE AS event_date
        FROM read_csv(
            {rating},
            header=true,
            columns={{'user_id': 'VARCHAR', 'parent_asin': 'VARCHAR',
                      'rating': 'VARCHAR', 'timestamp': 'VARCHAR'}}
        )
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW product_catalog AS
        SELECT {partition} AS source_partition,
               TRY_CAST(parent_asin AS VARCHAR) AS product_id,
               TRY_CAST(title AS VARCHAR) AS product_title,
               TRY_CAST(main_category AS VARCHAR) AS raw_main_category,
               TRY_CAST(categories AS VARCHAR[]) AS category_path,
               TRY_CAST(price AS DOUBLE) AS snapshot_price,
               TRY_CAST(average_rating AS DOUBLE) AS metadata_rating_mean,
               TRY_CAST(rating_number AS BIGINT) AS metadata_rating_count,
               TRY_CAST(store AS VARCHAR) AS store_name,
               json_extract_string(TRY_CAST(details AS JSON), '$.Brand') AS brand_name,
               json_extract_string(TRY_CAST(details AS JSON), '$."Date First Available"')
                   AS first_available_date
        FROM read_json(
            {metadata},
            format='newline_delimited',
            maximum_object_size=134217728,
            ignore_errors=true,
            columns={{
                'parent_asin': 'VARCHAR',
                'title': 'VARCHAR',
                'main_category': 'VARCHAR',
                'categories': 'VARCHAR[]',
                'price': 'VARCHAR',
                'average_rating': 'DOUBLE',
                'rating_number': 'BIGINT',
                'store': 'VARCHAR',
                'details': 'JSON'
            }}
        )
    """)


def materialize_inputs(
    con: duckdb.DuckDBPyConnection,
    cache_dir: Path,
    event_store_bucket_count: int = DEFAULT_EVENT_STORE_BUCKET_COUNT,
) -> dict[str, Path]:
    cache = cache_dir.expanduser().resolve()
    cache.mkdir(parents=True, exist_ok=True)
    tmp = _prepare_tmp(cache)
    _configure_connection(con, cache)

    rated_products = tmp / "rated_products.parquet"
    con.execute(f"""
        COPY (SELECT DISTINCT source_partition, product_id FROM rating_events)
        TO {sql_literal(str(rated_products))} (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    catalog_parquet = tmp / "canonical_metadata.parquet"
    con.execute(f"""
        COPY (SELECT * FROM product_catalog)
        TO {sql_literal(str(catalog_parquet))} (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    paths = write_product_core(con, catalog_parquet, cache, rated_products=rated_products)
    catalog_parquet.unlink(missing_ok=True)
    rated_products.unlink(missing_ok=True)

    event_store = write_rating_event_store(con, cache, tmp, event_store_bucket_count)
    con.execute("DROP VIEW IF EXISTS rating_events")

    daily_dir = tmp / "bucket_daily"
    time_dir = tmp / "bucket_product_time"
    daily_dir.mkdir()
    time_dir.mkdir()
    daily_files: list[Path] = []
    time_files: list[Path] = []
    for source_partition, bucket, events_path in iter_event_store_buckets(event_store):
        daily_path = daily_dir / f"{source_partition}__{bucket}.parquet"
        write_bucket_daily(con, events_path, source_partition, daily_path)
        daily_files.append(daily_path)
        time_path = time_dir / f"{source_partition}__{bucket}.parquet"
        write_bucket_product_time(con, daily_path, time_path)
        time_files.append(time_path)

    rating_daily = cache / "rating_daily_summary.parquet"
    product_time = cache / "product_time_summary.parquet"
    merge_parquet_files(con, daily_files, rating_daily, EMPTY_DAILY_SELECT)
    merge_parquet_files(con, time_files, product_time, EMPTY_PRODUCT_TIME_SELECT)
    coverage = observation_range(con, daily_files)
    metadata_path = cache / "storage_metadata.json"
    write_json(metadata_path, {
        "rating_event_store": rating_event_store_metadata(event_store_bucket_count),
        "rating_observation": coverage,
    })
    shutil.rmtree(tmp, ignore_errors=True)
    shutil.rmtree(cache / ".duckdb_tmp", ignore_errors=True)
    paths.update({
        "rating_daily_summary": rating_daily,
        "product_time_summary": product_time,
        "rating_event_store": event_store,
        "storage_metadata": metadata_path,
    })
    return paths


def build_market_discovery_inputs(
    category: str,
    data_root: Path,
    output_cache: Path,
    event_store_bucket_count: int | None = None,
) -> dict[str, Any]:
    cache = output_cache.expanduser().resolve()
    cache.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    kwargs = {}
    if event_store_bucket_count is not None:
        kwargs["event_store_bucket_count"] = event_store_bucket_count
    try:
        register_amazon_views(con, category, data_root)
        return materialize_inputs(con, cache, **kwargs)
    finally:
        con.close()
