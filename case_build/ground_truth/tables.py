from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


def write_population_truth(
    con: duckdb.DuckDBPyConnection,
    case_users: Path,
    case_focals: Path,
    positive_outcomes: Path,
    destination: Path,
    copy_atomic,
) -> None:
    users = sql_literal(str(case_users))
    focals = sql_literal(str(case_focals))
    outcomes = sql_literal(str(positive_outcomes))
    copy_atomic(f"""
        SELECT f.case_id,
               f.focal_id,
               u.user_id,
               u.user_group,
               o.outcome_product_id,
               o.event_timestamp,
               o.rating,
               o.verified_purchase,
               o.outcome_policy
        FROM read_parquet({users}) u
        JOIN read_parquet({focals}) f USING(case_id)
        LEFT JOIN read_parquet({outcomes}) o
          ON f.case_id=o.case_id
         AND f.focal_id=o.focal_id
         AND u.user_id=o.user_id
        ORDER BY f.case_id, f.focal_id, u.user_id
    """, destination)


def write_choice_truth(
    con: duckdb.DuckDBPyConnection,
    gt1_outcomes: Path,
    destination: Path,
    copy_atomic,
) -> None:
    src = sql_literal(str(gt1_outcomes))
    copy_atomic(f"""
        SELECT case_id,
               focal_id,
               user_id,
               outcome_product_id AS product_id,
               outcome_is_focal,
               event_timestamp,
               rating,
               verified_purchase,
               outcome_policy,
               history_product_count,
               days_since_last_event
        FROM read_parquet({src})
        ORDER BY case_id, focal_id, user_id
    """, destination)


def write_market_truth(
    con: duckdb.DuckDBPyConnection,
    case_focals: Path,
    focal_competitors: Path,
    population_truth: Path,
    destination: Path,
    copy_atomic,
) -> None:
    focals = sql_literal(str(case_focals))
    comps = sql_literal(str(focal_competitors))
    truth = sql_literal(str(population_truth))
    copy_atomic(f"""
        WITH products AS (
            SELECT case_id, focal_id, focal_product_id AS product_id
            FROM read_parquet({focals})
            UNION
            SELECT case_id, focal_id, competitor_product_id
            FROM read_parquet({comps})
            WHERE competitor_selected
        ), counts AS (
            SELECT case_id, focal_id, outcome_product_id AS product_id,
                   count(*)::BIGINT AS demand_count
            FROM read_parquet({truth})
            WHERE outcome_product_id IS NOT NULL
            GROUP BY case_id, focal_id, outcome_product_id
        ), totals AS (
            SELECT case_id, focal_id,
                   count(*) FILTER (outcome_product_id IS NOT NULL)::BIGINT
                       AS market_positive_count,
                   count(*)::BIGINT AS population_count
            FROM read_parquet({truth})
            GROUP BY case_id, focal_id
        ), joined AS (
            SELECT p.case_id,
                   p.focal_id,
                   p.product_id,
                   coalesce(c.demand_count, 0)::BIGINT AS demand_count,
                   coalesce(t.market_positive_count, 0)::BIGINT AS market_positive_count,
                   coalesce(t.population_count, 0)::BIGINT AS population_count
            FROM products p
            LEFT JOIN counts c USING(case_id, focal_id, product_id)
            LEFT JOIN totals t USING(case_id, focal_id)
        ), ranked AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY case_id, focal_id
                       ORDER BY demand_count DESC, product_id
                   )::BIGINT AS rank
            FROM joined
        )
        SELECT case_id,
               focal_id,
               product_id,
               demand_count,
               CASE WHEN market_positive_count > 0
                    THEN demand_count::DOUBLE / market_positive_count
                    ELSE 0.0 END AS demand_share,
               rank,
               population_count,
               market_positive_count,
               (population_count - market_positive_count)::BIGINT AS none_count,
               CASE WHEN population_count > 0
                    THEN market_positive_count::DOUBLE / population_count
                    ELSE 0.0 END AS market_entry_rate
        FROM ranked
        ORDER BY case_id, focal_id, rank, product_id
    """, destination)
