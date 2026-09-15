from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from utils import sql_literal, write_json
from .metrics import (
    build_market_metric_rows,
    register_predictions,
    write_individual_metrics,
)


class EvaluationPipeline:
    """评估用户侧选择与商品侧需求 / 排名。"""

    def __init__(
        self,
        population_truth: Path,
        choice_truth: Path,
        market_truth: Path,
        individual_predictions: Path,
        output_dir: Path,
        *,
        market_predictions: Path | None = None,
        split_assignments: Path | None = None,
    ) -> None:
        self.population_truth = population_truth.expanduser().resolve()
        self.choice_truth = choice_truth.expanduser().resolve()
        self.market_truth = market_truth.expanduser().resolve()
        self.individual_predictions = individual_predictions.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.market_predictions = (
            market_predictions.expanduser().resolve() if market_predictions else None
        )
        self.split_assignments = (
            split_assignments.expanduser().resolve() if split_assignments else None
        )
        for path in (
            self.population_truth, self.choice_truth,
            self.market_truth, self.individual_predictions,
        ):
            if not path.is_file():
                raise FileNotFoundError(path)
        for path in (self.market_predictions, self.split_assignments):
            if path is not None and not path.is_file():
                raise FileNotFoundError(path)
        self.individual_metrics_path = self.output_dir / "individual_metrics.parquet"
        self.market_metrics_path = self.output_dir / "market_metrics.parquet"
        self.case_metrics_path = self.output_dir / "case_metrics.parquet"
        self.summary_path = self.output_dir / "evaluation_summary.json"
        self.con = duckdb.connect()
        self.con.execute("SET preserve_insertion_order=false")

    def close(self) -> None:
        self.con.close()

    def _copy(self, query: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        part = path.with_suffix(path.suffix + ".part")
        part.unlink(missing_ok=True)
        self.con.execute(
            f"COPY ({query}) TO {sql_literal(str(part))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        os.replace(part, path)

    def _write_market_metrics(self, rows: list[dict[str, Any]]) -> None:
        self.con.execute("DROP TABLE IF EXISTS market_metric_rows")
        self.con.execute("""
            CREATE TEMP TABLE market_metric_rows(
                case_candidate_id VARCHAR,
                kendall_tau DOUBLE,
                ndcg DOUBLE,
                true_demand_total DOUBLE,
                predicted_demand_total DOUBLE,
                demand_total_abs_error DOUBLE
            )
        """)
        if rows:
            self.con.executemany(
                "INSERT INTO market_metric_rows VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (
                        row["case_candidate_id"], row["kendall_tau"], row["ndcg"],
                        row["true_demand_total"], row["predicted_demand_total"],
                        row["demand_total_abs_error"],
                    )
                    for row in rows
                ],
            )
        self._copy(
            "SELECT * FROM market_metric_rows ORDER BY case_candidate_id",
            self.market_metrics_path,
        )

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        register_predictions(self.con, self.individual_predictions)
        write_individual_metrics(
            self.con,
            self.population_truth,
            self.choice_truth,
            self.individual_metrics_path,
            self._copy,
        )
        market_rows = build_market_metric_rows(
            self.con, self.market_truth, self.market_predictions
        )
        self._write_market_metrics(market_rows)

        split_join = ""
        split_select = ""
        if self.split_assignments is not None:
            split_join = (
                f"LEFT JOIN read_parquet({sql_literal(str(self.split_assignments))}) s "
                "USING(case_candidate_id)"
            )
            split_select = ", s.split_name, s.evaluation_regime"
        self._copy(f"""
            SELECT i.*, m.kendall_tau, m.ndcg,
                   m.true_demand_total, m.predicted_demand_total,
                   m.demand_total_abs_error
                   {split_select}
            FROM read_parquet({sql_literal(str(self.individual_metrics_path))}) i
            LEFT JOIN read_parquet({sql_literal(str(self.market_metrics_path))}) m
              USING(case_candidate_id)
            {split_join}
            ORDER BY i.case_candidate_id
        """, self.case_metrics_path)

        aggregate = self.con.execute("""
            SELECT avg(gt1_choice_accuracy),
                   avg(gt2_outcome_accuracy),
                   avg(market_entry_accuracy),
                   avg(kendall_tau),
                   avg(ndcg),
                   avg(demand_total_abs_error),
                   sum(n_population),
                   sum(n_gt1)
            FROM read_parquet(?)
        """, [str(self.case_metrics_path)]).fetchone()
        payload = {
            "status": "COMPLETE",
            "schema_version": "evaluation_v1",
            "case_count": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.case_metrics_path)]
            ).fetchone()[0]),
            "metrics": {
                "mean_gt1_choice_accuracy": aggregate[0],
                "mean_gt2_outcome_accuracy": aggregate[1],
                "mean_market_entry_accuracy": aggregate[2],
                "mean_kendall_tau": aggregate[3],
                "mean_ndcg": aggregate[4],
                "mean_demand_total_abs_error": aggregate[5],
                "population_rows": aggregate[6],
                "gt1_rows": aggregate[7],
            },
            "market_prediction_source": (
                "explicit_market_predictions"
                if self.market_predictions is not None
                else "aggregated_individual_predictions"
            ),
        }
        write_json(self.summary_path, payload)
        return payload
