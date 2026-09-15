from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb

from utils import sql_literal, write_json
from .split import (
    hybrid_split,
    market_holdout_split,
    read_cases,
    temporal_within_market_split,
)


class BenchmarkSplitPipeline:
    def __init__(
        self,
        accepted_cases: Path,
        output_dir: Path,
        *,
        strategy: str = "hybrid",
        seed: str = "benchmark_split_v1",
        learning_fraction: float = 0.7,
        validation_fraction: float = 0.1,
        unseen_market_fraction: float = 0.2,
    ) -> None:
        self.accepted_cases = accepted_cases.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        if not self.accepted_cases.is_file():
            raise FileNotFoundError(self.accepted_cases)
        if strategy not in {"market_holdout", "temporal_within_market", "hybrid"}:
            raise ValueError("unknown split strategy")
        self.strategy = strategy
        self.seed = seed
        self.learning_fraction = learning_fraction
        self.validation_fraction = validation_fraction
        self.unseen_market_fraction = unseen_market_fraction
        self.assignments_path = self.output_dir / "split_assignments.parquet"
        self.summary_path = self.output_dir / "split_summary.json"
        self.con = duckdb.connect()

    def close(self) -> None:
        self.con.close()

    def _write_assignments(self, rows: list[dict[str, Any]]) -> None:
        self.con.execute("DROP TABLE IF EXISTS split_rows")
        self.con.execute("""
            CREATE TEMP TABLE split_rows(
                case_candidate_id VARCHAR,
                market_id VARCHAR,
                t0 DATE,
                split_name VARCHAR,
                evaluation_regime VARCHAR
            )
        """)
        if rows:
            self.con.executemany(
                "INSERT INTO split_rows VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        row["case_candidate_id"], row["market_id"], row["t0"],
                        row["split_name"], row.get("evaluation_regime"),
                    )
                    for row in rows
                ],
            )
        self.output_dir.mkdir(parents=True, exist_ok=True)
        part = self.assignments_path.with_suffix(".parquet.part")
        part.unlink(missing_ok=True)
        self.con.execute(
            f"COPY (SELECT * FROM split_rows ORDER BY market_id,t0,case_candidate_id) "
            f"TO {sql_literal(str(part))} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        os.replace(part, self.assignments_path)

    def _write_split_json(self, rows: list[dict[str, Any]], split_name: str) -> None:
        grouped: dict[str, list[str]] = defaultdict(list)
        regimes: dict[str, str | None] = {}
        for row in rows:
            if row["split_name"] != split_name:
                continue
            grouped[row["market_id"]].append(row["case_candidate_id"])
            if row.get("evaluation_regime"):
                regimes[row["market_id"]] = row["evaluation_regime"]
        markets = []
        for market_id in sorted(grouped):
            item: dict[str, Any] = {
                "market_id": market_id,
                "case_ids": grouped[market_id],
            }
            if market_id in regimes:
                item["evaluation_regime"] = regimes[market_id]
            markets.append(item)
        write_json(self.output_dir / f"{split_name}.json", {
            "split_name": split_name,
            "strategy": self.strategy,
            "markets": markets,
        })

    def run(self) -> dict[str, Any]:
        cases = read_cases(self.con, self.accepted_cases)
        if self.strategy == "market_holdout":
            rows = market_holdout_split(
                cases,
                learning_fraction=self.learning_fraction,
                validation_fraction=self.validation_fraction,
                seed=self.seed,
            )
        elif self.strategy == "temporal_within_market":
            rows = temporal_within_market_split(
                cases,
                learning_fraction=self.learning_fraction,
                validation_fraction=self.validation_fraction,
            )
        else:
            rows = hybrid_split(
                cases,
                unseen_market_fraction=self.unseen_market_fraction,
                seen_learning_fraction=self.learning_fraction,
                seen_validation_fraction=self.validation_fraction,
                seed=self.seed,
            )
        self._write_assignments(rows)
        for name in ("learning", "validation", "evaluation"):
            self._write_split_json(rows, name)
        counts = {
            name: sum(row["split_name"] == name for row in rows)
            for name in ("learning", "validation", "evaluation")
        }
        payload = {
            "status": "COMPLETE",
            "schema_version": "benchmark_split_v1",
            "strategy": self.strategy,
            "seed": self.seed,
            "case_counts": counts,
            "market_count": len({row["market_id"] for row in rows}),
            "uses_ground_truth_for_split": False,
            "unseen_market_fraction": self.unseen_market_fraction,
            "learning_fraction": self.learning_fraction,
            "validation_fraction": self.validation_fraction,
        }
        write_json(self.summary_path, payload)
        return payload
