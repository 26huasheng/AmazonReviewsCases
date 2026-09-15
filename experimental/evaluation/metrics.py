from __future__ import annotations

import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb

from utils import sql_literal


def _columns(con: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    return {
        str(row[0])
        for row in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
    }


def register_predictions(
    con: duckdb.DuckDBPyConnection,
    predictions: Path,
) -> None:
    cols = _columns(con, predictions)
    required = {"case_candidate_id", "user_id"}
    missing = required - cols
    if missing:
        raise ValueError(f"individual predictions missing columns: {sorted(missing)}")
    if "predicted_outcome_product_id" in cols:
        product = "predicted_outcome_product_id"
    elif "predicted_product_id" in cols:
        product = "predicted_product_id"
    else:
        raise ValueError(
            "individual predictions require predicted_outcome_product_id or predicted_product_id"
        )
    src = sql_literal(str(predictions))
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW normalized_individual_predictions AS
        SELECT CAST(case_candidate_id AS VARCHAR) AS case_candidate_id,
               CAST(user_id AS VARCHAR) AS user_id,
               CASE WHEN {product} IS NULL OR trim(CAST({product} AS VARCHAR))='' THEN NULL
                    ELSE CAST({product} AS VARCHAR) END AS predicted_outcome_product_id
        FROM read_parquet({src})
    """)


def write_individual_metrics(
    con: duckdb.DuckDBPyConnection,
    population_truth: Path,
    choice_truth: Path,
    destination: Path,
    copy_atomic,
) -> None:
    g2 = sql_literal(str(population_truth))
    g1 = sql_literal(str(choice_truth))
    copy_atomic(f"""
        WITH joined AS (
            SELECT g.case_candidate_id,
                   g.user_id,
                   g.outcome_product_id,
                   p.predicted_outcome_product_id,
                   (g.outcome_product_id IS NOT NULL) AS actual_entry,
                   (p.predicted_outcome_product_id IS NOT NULL) AS predicted_entry,
                   (p.user_id IS NOT NULL) AS prediction_present
            FROM read_parquet({g2}) g
            LEFT JOIN normalized_individual_predictions p
              ON g.case_candidate_id=p.case_candidate_id
             AND g.user_id=p.user_id
        ), gt1_users AS (
            SELECT case_candidate_id, user_id, target_product_id
            FROM read_parquet({g1})
        )
        SELECT j.case_candidate_id,
               count(*)::BIGINT AS n_population,
               count(*) FILTER (prediction_present)::BIGINT AS n_predictions,
               avg(CASE WHEN prediction_present
                         AND predicted_outcome_product_id IS NOT DISTINCT FROM outcome_product_id
                        THEN 1.0 ELSE 0.0 END)::DOUBLE AS gt2_outcome_accuracy,
               avg(CASE WHEN prediction_present AND actual_entry=predicted_entry
                        THEN 1.0 ELSE 0.0 END)::DOUBLE AS market_entry_accuracy,
               count(*) FILTER (actual_entry)::BIGINT AS actual_market_positive,
               count(*) FILTER (predicted_entry)::BIGINT AS predicted_market_positive,
               abs(count(*) FILTER (predicted_entry) - count(*) FILTER (actual_entry))::BIGINT
                   AS market_positive_count_abs_error,
               count(*) FILTER (g1.user_id IS NOT NULL)::BIGINT AS n_gt1,
               avg(CASE WHEN g1.user_id IS NULL THEN NULL
                        WHEN prediction_present
                         AND predicted_outcome_product_id=g1.target_product_id
                        THEN 1.0 ELSE 0.0 END)::DOUBLE AS gt1_choice_accuracy
        FROM joined j
        LEFT JOIN gt1_users g1
          ON j.case_candidate_id=g1.case_candidate_id
         AND j.user_id=g1.user_id
        GROUP BY j.case_candidate_id
        ORDER BY j.case_candidate_id
    """, destination)


def _kendall_tau(order_true: list[str], order_pred: list[str]) -> float | None:
    if len(order_true) < 2 or set(order_true) != set(order_pred):
        return None
    pos_true = {item: i for i, item in enumerate(order_true)}
    pos_pred = {item: i for i, item in enumerate(order_pred)}
    concordant = 0
    discordant = 0
    items = order_true
    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            a, b = items[i], items[j]
            sign_true = pos_true[a] - pos_true[b]
            sign_pred = pos_pred[a] - pos_pred[b]
            if sign_true * sign_pred > 0:
                concordant += 1
            else:
                discordant += 1
    total = concordant + discordant
    return (concordant - discordant) / total if total else None


def _ndcg(true_relevance: dict[str, float], predicted_order: list[str]) -> float | None:
    if not true_relevance:
        return None
    ideal = sorted(true_relevance, key=lambda p: (-true_relevance[p], p))

    def dcg(order: list[str]) -> float:
        return sum(
            true_relevance.get(product_id, 0.0) / math.log2(index + 2)
            for index, product_id in enumerate(order)
        )

    ideal_dcg = dcg(ideal)
    return dcg(predicted_order) / ideal_dcg if ideal_dcg > 0 else None


def build_market_metric_rows(
    con: duckdb.DuckDBPyConnection,
    market_truth: Path,
    market_predictions: Path | None,
) -> list[dict[str, Any]]:
    truth_rows = con.execute("""
        SELECT case_candidate_id, product_id, demand_count, rank
        FROM read_parquet(?) ORDER BY case_candidate_id, rank, product_id
    """, [str(market_truth)]).fetchall()
    truth: dict[str, list[tuple[str, float, int]]] = defaultdict(list)
    for case_id, product_id, demand, rank in truth_rows:
        truth[str(case_id)].append((str(product_id), float(demand), int(rank)))

    pred: dict[str, list[tuple[str, float]]] = defaultdict(list)
    if market_predictions is not None:
        cols = _columns(con, market_predictions)
        if "predicted_demand_count" in cols:
            score = "predicted_demand_count"
        elif "predicted_demand_score" in cols:
            score = "predicted_demand_score"
        elif "predicted_rank" in cols:
            score = "-try_cast(predicted_rank AS DOUBLE)"
        else:
            raise ValueError(
                "market predictions require predicted_demand_count, predicted_demand_score or predicted_rank"
            )
        rows = con.execute(f"""
            SELECT case_candidate_id, product_id, {score} AS score
            FROM read_parquet({sql_literal(str(market_predictions))})
        """).fetchall()
        for case_id, product_id, score_value in rows:
            pred[str(case_id)].append((str(product_id), float(score_value or 0.0)))
    else:
        rows = con.execute("""
            SELECT case_candidate_id, predicted_outcome_product_id AS product_id,
                   count(*)::DOUBLE AS score
            FROM normalized_individual_predictions
            WHERE predicted_outcome_product_id IS NOT NULL
            GROUP BY case_candidate_id, predicted_outcome_product_id
        """).fetchall()
        for case_id, product_id, score_value in rows:
            pred[str(case_id)].append((str(product_id), float(score_value)))

    output = []
    for case_id, true_rows in truth.items():
        relevance = {product: demand for product, demand, _ in true_rows}
        true_order = [product for product, _, _ in sorted(true_rows, key=lambda x: x[2])]
        score_map = {product: score for product, score in pred.get(case_id, [])}
        pred_order = sorted(relevance, key=lambda p: (-score_map.get(p, 0.0), p))
        true_total = sum(relevance.values())
        pred_total = sum(max(score_map.get(p, 0.0), 0.0) for p in relevance)
        output.append({
            "case_candidate_id": case_id,
            "kendall_tau": _kendall_tau(true_order, pred_order),
            "ndcg": _ndcg(relevance, pred_order),
            "true_demand_total": true_total,
            "predicted_demand_total": pred_total,
            "demand_total_abs_error": abs(pred_total - true_total),
        })
    return output
