from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


def validate_long_tables(
    con: duckdb.DuckDBPyConnection,
    accepted_cases: Path,
    case_shelf: Path,
    case_users: Path,
    choice_truth: Path,
    population_truth: Path,
    market_truth: Path,
    split_assignments: Path,
) -> dict[str, Any]:
    """在物化最终目录前检查核心跨表契约。"""
    errors: list[str] = []

    duplicate_cases = con.execute("""
        SELECT count(*) FROM (
            SELECT case_candidate_id, count(*) n
            FROM read_parquet(?) GROUP BY case_candidate_id HAVING n>1
        )
    """, [str(accepted_cases)]).fetchone()[0]
    if duplicate_cases:
        errors.append(f"duplicate accepted case ids: {duplicate_cases}")

    bad_focal = con.execute("""
        SELECT count(*) FROM (
            SELECT c.case_candidate_id,
                   count(*) FILTER (s.role='focal' AND s.product_id=c.focal_product_id) AS n
            FROM read_parquet(?) c
            LEFT JOIN read_parquet(?) s USING(case_candidate_id)
            GROUP BY c.case_candidate_id
            HAVING n<>1
        )
    """, [str(accepted_cases), str(case_shelf)]).fetchone()[0]
    if bad_focal:
        errors.append(f"cases without exactly one focal shelf row: {bad_focal}")

    bad_gt2_coverage = con.execute("""
        WITH u AS (
            SELECT case_candidate_id, count(*) n FROM read_parquet(?) GROUP BY 1
        ), g AS (
            SELECT case_candidate_id, count(*) n FROM read_parquet(?) GROUP BY 1
        )
        SELECT count(*) FROM u FULL JOIN g USING(case_candidate_id)
        WHERE coalesce(u.n,0)<>coalesce(g.n,0)
    """, [str(case_users), str(population_truth)]).fetchone()[0]
    if bad_gt2_coverage:
        errors.append(f"GT2 coverage mismatch cases: {bad_gt2_coverage}")

    bad_targets = con.execute("""
        SELECT count(*)
        FROM read_parquet(?) g
        LEFT JOIN read_parquet(?) s
          ON g.case_candidate_id=s.case_candidate_id
         AND g.outcome_product_id=s.product_id
        WHERE g.outcome_product_id IS NOT NULL
          AND s.product_id IS NULL
    """, [str(population_truth), str(case_shelf)]).fetchone()[0]
    if bad_targets:
        errors.append(f"GT2 outcomes outside shelf: {bad_targets}")

    gt1_mismatch = con.execute("""
        WITH positives AS (
            SELECT case_candidate_id, user_id, outcome_product_id
            FROM read_parquet(?) WHERE outcome_product_id IS NOT NULL
        ), gt1 AS (
            SELECT case_candidate_id, user_id, target_product_id
            FROM read_parquet(?)
        )
        SELECT count(*) FROM (
            SELECT p.case_candidate_id, p.user_id
            FROM positives p LEFT JOIN gt1 g USING(case_candidate_id,user_id)
            WHERE g.target_product_id IS NULL OR g.target_product_id<>p.outcome_product_id
            UNION ALL
            SELECT g.case_candidate_id, g.user_id
            FROM gt1 g LEFT JOIN positives p USING(case_candidate_id,user_id)
            WHERE p.outcome_product_id IS NULL
        )
    """, [str(population_truth), str(choice_truth)]).fetchone()[0]
    if gt1_mismatch:
        errors.append(f"GT1 != GT2 positives rows: {gt1_mismatch}")

    bad_market_truth = con.execute("""
        WITH expected AS (
            SELECT case_candidate_id, outcome_product_id AS product_id, count(*)::BIGINT n
            FROM read_parquet(?)
            WHERE outcome_product_id IS NOT NULL
            GROUP BY 1,2
        )
        SELECT count(*)
        FROM read_parquet(?) m
        LEFT JOIN expected e USING(case_candidate_id,product_id)
        WHERE m.demand_count<>coalesce(e.n,0)
    """, [str(population_truth), str(market_truth)]).fetchone()[0]
    if bad_market_truth:
        errors.append(f"market_truth demand mismatch rows: {bad_market_truth}")

    split_mismatch = con.execute("""
        WITH a AS (SELECT DISTINCT case_candidate_id FROM read_parquet(?)),
             s AS (SELECT case_candidate_id, count(*) n FROM read_parquet(?) GROUP BY 1)
        SELECT count(*) FROM a FULL JOIN s USING(case_candidate_id)
        WHERE a.case_candidate_id IS NULL OR s.case_candidate_id IS NULL OR s.n<>1
    """, [str(accepted_cases), str(split_assignments)]).fetchone()[0]
    if split_mismatch:
        errors.append(f"split coverage mismatch cases: {split_mismatch}")

    return {
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "error_count": len(errors),
    }
