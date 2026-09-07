from __future__ import annotations

from pathlib import Path

import duckdb

from market_discovery.cross_path_merge import MIN_FINAL_MARKET_PRODUCT_COUNT
from utils import sql_literal


def _columns(con: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    return {
        str(row[0])
        for row in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
    }


def write_market_products(
    con: duckdb.DuckDBPyConnection,
    final_market: Path,
    product_core: Path,
    destination: Path,
    copy_atomic,
    *,
    product_time_summary: Path | None = None,
    min_product_count: int = MIN_FINAL_MARKET_PRODUCT_COUNT,
) -> None:
    """生成最终 Market 的长期商品 universe，一行一个 Market×product。"""
    if min_product_count < 0:
        raise ValueError("min_product_count must be >= 0")
    core_cols = _columns(con, product_core)
    required = {"source_partition", "product_id", "product_title"}
    missing = required - core_cols
    if missing:
        raise ValueError(f"product_core missing columns: {sorted(missing)}")

    category_expr = "p.category_path" if "category_path" in core_cols else "NULL::VARCHAR[]"
    available_expr = (
        "try_cast(p.first_available_date AS DATE)"
        if "first_available_date" in core_cols else "NULL::DATE"
    )
    store_expr = "CAST(p.store AS VARCHAR)" if "store" in core_cols else "NULL::VARCHAR"
    price_expr = (
        "try_cast(p.snapshot_price AS DOUBLE)"
        if "snapshot_price" in core_cols else "NULL::DOUBLE"
    )
    market = sql_literal(str(final_market))
    core = sql_literal(str(product_core))

    time_join = ""
    first_review_expr = "NULL::DATE"
    if product_time_summary is not None:
        time_cols = _columns(con, product_time_summary)
        needed = {"source_partition", "product_id", "first_rating_date"}
        missing_time = needed - time_cols
        if missing_time:
            raise ValueError(
                f"product_time_summary missing columns: {sorted(missing_time)}"
            )
        times = sql_literal(str(product_time_summary))
        time_join = (
            f"LEFT JOIN read_parquet({times}) t "
            "ON m.source_partition=t.source_partition AND m.product_id=t.product_id"
        )
        first_review_expr = "t.first_rating_date"

    copy_atomic(f"""
        WITH eligible AS (
            SELECT *
            FROM read_parquet({market})
            WHERE coalesce(product_count, len(product_ids)) >= {int(min_product_count)}
        ), expanded AS (
            SELECT discovery_version,
                   source_partition,
                   market_id,
                   market_label,
                   CAST(product_id AS VARCHAR) AS product_id
            FROM eligible, unnest(product_ids) u(product_id)
        ), m AS (
            SELECT * FROM expanded
        )
        SELECT m.discovery_version,
               m.source_partition,
               m.market_id,
               m.market_label,
               m.product_id,
               p.product_title AS title,
               {category_expr} AS category_path,
               {first_review_expr} AS first_review_date,
               {available_expr} AS first_available_date,
               {store_expr} AS store,
               TRUE AS metadata_available,
               {price_expr} AS metadata_snapshot_price
        FROM m
        JOIN read_parquet({core}) p
          ON m.source_partition=p.source_partition
         AND m.product_id=CAST(p.product_id AS VARCHAR)
        {time_join}
        ORDER BY m.source_partition, m.market_id, m.product_id
    """, destination)
