from __future__ import annotations

from pathlib import Path

import duckdb

from utils import PRODUCT_CORE_KEEP_PREDICATE, sql_literal, write_json


def write_product_core(
    con: duckdb.DuckDBPyConnection,
    catalog_parquet: Path,
    cache_dir: Path,
    rated_products: Path | None = None,
) -> dict[str, Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    source = sql_literal(str(catalog_parquet))
    rated_extra = ""
    if rated_products is not None:
        rated = sql_literal(str(rated_products))
        rated_extra = f"""
        ,
        count(*) FILTER (
            ({PRODUCT_CORE_KEEP_PREDICATE})
            AND NOT EXISTS (
                SELECT 1 FROM read_parquet({rated}) r
                WHERE r.source_partition = c.source_partition AND r.product_id = c.product_id
            )
        ) AS removed_unrated_product_count
"""
    quality_row = con.execute(f"""
        SELECT count(*) AS source_product_count,
               count(*) FILTER (product_id IS NULL OR trim(CAST(product_id AS VARCHAR)) = '')
                   AS missing_product_id_count,
               count(*) FILTER (product_title IS NULL OR trim(product_title) = '')
                   AS missing_product_title_count,
               count(*) FILTER (
                   first_available_date IS NULL
                   OR trim(CAST(first_available_date AS VARCHAR)) = ''
               ) AS missing_first_available_date_count,
               count(*) FILTER (category_path IS NULL OR len(category_path) = 0)
                   AS missing_category_path_count,
               count(*) FILTER (NOT ({PRODUCT_CORE_KEEP_PREDICATE}))
                   AS removed_product_count{rated_extra}
        FROM read_parquet({source}) c
    """).fetchone()
    quality = dict(zip([column[0] for column in con.description], quality_row))
    if rated_products is not None:
        quality["removed_product_count"] += quality["removed_unrated_product_count"]
    quality["usable_product_count"] = (
        quality["source_product_count"] - quality["removed_product_count"]
    )
    keep_predicate = f"({PRODUCT_CORE_KEEP_PREDICATE})"
    if rated_products is not None:
        rated = sql_literal(str(rated_products))
        keep_predicate += f"""
            AND EXISTS (
                SELECT 1 FROM read_parquet({rated}) r
                WHERE r.source_partition = c.source_partition AND r.product_id = c.product_id
            )"""
    product_core = cache_dir / "product_core.parquet"
    part = product_core.with_name(product_core.name + ".part")
    if part.exists():
        part.unlink()
    con.execute(f"""
        COPY (
            SELECT source_partition, product_id, product_title, category_path,
                   raw_main_category, brand_name, snapshot_price, first_available_date,
                   metadata_rating_mean, metadata_rating_count, store_name,
                   CAST(NULL AS VARCHAR) AS market_id,
                   CAST(NULL AS VARCHAR) AS market_segment_id
            FROM read_parquet({source}) c
            WHERE {keep_predicate}
        ) TO {sql_literal(str(part))} (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    part.replace(product_core)
    cleaning_path = cache_dir / "product_core_cleaning.json"
    write_json(cleaning_path, quality)
    return {"product_core": product_core, "product_core_cleaning": cleaning_path}
