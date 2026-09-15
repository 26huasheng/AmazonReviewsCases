from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


# Production behavior-graph thresholds (Electronics v1 focal diversity).
MIN_NODE_USERS = 100
MIN_SHARED_USERS = 5


def write_full_graph_edges(
    con: duckdb.DuckDBPyConnection,
    pair_full_counts: Path,
    product_user_totals: Path,
    destination: Path,
    copy_atomic,
    *,
    min_endpoint_users: int,
    min_shared_users: int,
) -> None:
    """Legacy audit graph edges. Not used by MarketBuildPipeline / Electronics v1.

    Production focal-diversity edges are write_strong_copreview_edges.
    """
    if min_endpoint_users <= 0 or min_shared_users <= 0:
        raise ValueError("graph thresholds must be positive")

    pairs = sql_literal(str(pair_full_counts))
    totals = sql_literal(str(product_user_totals))
    copy_atomic(f"""
        SELECT p.source_partition,
               p.market_id,
               p.market_label,
               p.leaf_category,
               p.product_a,
               p.product_b,
               a.n_users_full AS users_a_full,
               b.n_users_full AS users_b_full,
               p.shared_users_full,
               p.shared_users_full::DOUBLE /
                   nullif(a.n_users_full + b.n_users_full - p.shared_users_full, 0)
                   AS jaccard_full,
               p.shared_users_full::DOUBLE /
                   nullif(least(a.n_users_full, b.n_users_full), 0)
                   AS overlap_min_full
        FROM read_parquet({pairs}) p
        JOIN read_parquet({totals}) a
          ON p.source_partition = a.source_partition
         AND p.market_id = a.market_id
         AND p.product_a = a.product_id
        JOIN read_parquet({totals}) b
          ON p.source_partition = b.source_partition
         AND p.market_id = b.market_id
         AND p.product_b = b.product_id
        WHERE a.n_users_full >= {int(min_endpoint_users)}
          AND b.n_users_full >= {int(min_endpoint_users)}
          AND p.shared_users_full >= {int(min_shared_users)}
        ORDER BY p.source_partition, p.market_id, p.leaf_category,
                 p.product_a, p.product_b
    """, destination)


def write_strong_copreview_edges(
    con: duckdb.DuckDBPyConnection,
    market_products: Path,
    canonical_user_events: Path,
    destination: Path,
    copy_atomic,
    *,
    min_node_users: int = MIN_NODE_USERS,
    min_shared_users: int = MIN_SHARED_USERS,
) -> None:
    """Same Final Market + same leaf + n_users>=100 + shared_users>=5."""
    if min_node_users <= 0 or min_shared_users <= 0:
        raise ValueError("graph thresholds must be positive")
    products = sql_literal(str(market_products))
    events = sql_literal(str(canonical_user_events))
    copy_atomic(f"""
        WITH products AS (
            SELECT source_partition,
                   market_id,
                   market_label,
                   product_id,
                   CASE
                       WHEN category_path IS NULL OR len(category_path)=0 THEN NULL
                       ELSE category_path[len(category_path)]
                   END AS leaf_category
            FROM read_parquet({products})
        ), user_product AS (
            SELECT DISTINCT p.source_partition,
                   p.market_id,
                   p.market_label,
                   p.leaf_category,
                   p.product_id,
                   e.user_id
            FROM products p
            JOIN read_parquet({events}) e
              ON p.source_partition=e.source_partition
             AND p.product_id=e.product_id
            WHERE e.user_id IS NOT NULL
              AND trim(e.user_id) <> ''
        ), degree AS (
            SELECT source_partition, market_id, market_label, leaf_category,
                   product_id, count(*)::BIGINT AS n_users
            FROM user_product
            GROUP BY source_partition, market_id, market_label, leaf_category, product_id
        ), eligible AS (
            SELECT *
            FROM degree
            WHERE n_users >= {int(min_node_users)}
              AND leaf_category IS NOT NULL
              AND trim(leaf_category) <> ''
        )
        SELECT a.source_partition,
               a.market_id,
               a.market_label,
               a.leaf_category,
               least(ua.product_id, ub.product_id) AS product_id_a,
               greatest(ua.product_id, ub.product_id) AS product_id_b,
               da.n_users AS n_users_a,
               db.n_users AS n_users_b,
               count(*)::BIGINT AS shared_users
        FROM user_product ua
        JOIN eligible da
          ON ua.source_partition=da.source_partition
         AND ua.market_id=da.market_id
         AND ua.product_id=da.product_id
        JOIN user_product ub
          ON ua.source_partition=ub.source_partition
         AND ua.market_id=ub.market_id
         AND ua.leaf_category=ub.leaf_category
         AND ua.user_id=ub.user_id
         AND ua.product_id < ub.product_id
        JOIN eligible db
          ON ub.source_partition=db.source_partition
         AND ub.market_id=db.market_id
         AND ub.product_id=db.product_id
        JOIN eligible a
          ON ua.source_partition=a.source_partition
         AND ua.market_id=a.market_id
         AND ua.product_id=a.product_id
        GROUP BY a.source_partition, a.market_id, a.market_label, a.leaf_category,
                 least(ua.product_id, ub.product_id),
                 greatest(ua.product_id, ub.product_id),
                 da.n_users, db.n_users
        HAVING count(*) >= {int(min_shared_users)}
    """, destination)


def write_product_graph_degrees(
    con: duckdb.DuckDBPyConnection,
    market_products: Path,
    canonical_user_events: Path,
    destination: Path,
    copy_atomic,
) -> None:
    products = sql_literal(str(market_products))
    events = sql_literal(str(canonical_user_events))
    copy_atomic(f"""
        WITH products AS (
            SELECT source_partition,
                   market_id,
                   market_label,
                   product_id,
                   CASE
                       WHEN category_path IS NULL OR len(category_path)=0 THEN NULL
                       ELSE category_path[len(category_path)]
                   END AS leaf_category
            FROM read_parquet({products})
        ), user_product AS (
            SELECT DISTINCT p.source_partition, p.market_id, p.product_id, e.user_id
            FROM products p
            JOIN read_parquet({events}) e
              ON p.source_partition=e.source_partition
             AND p.product_id=e.product_id
            WHERE e.user_id IS NOT NULL AND trim(e.user_id) <> ''
        )
        SELECT p.source_partition,
               p.market_id,
               p.market_label,
               p.product_id,
               p.leaf_category,
               coalesce(d.n_users, 0)::BIGINT AS n_users
        FROM products p
        LEFT JOIN (
            SELECT source_partition, market_id, product_id, count(*)::BIGINT AS n_users
            FROM user_product
            GROUP BY source_partition, market_id, product_id
        ) d USING(source_partition, market_id, product_id)
    """, destination)
