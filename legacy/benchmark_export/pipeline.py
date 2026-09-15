from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb

from utils import json_safe, sql_literal, write_json
from .validate import validate_long_tables


class BenchmarkExportPipeline:
    """把长表构建产物物化成 SCHEMA.md 定义的 Market → Cases 目录。"""

    def __init__(
        self,
        final_market: Path,
        market_products: Path,
        market_population: Path,
        canonical_user_events: Path,
        accepted_cases: Path,
        case_shelf: Path,
        case_users: Path,
        choice_truth: Path,
        population_truth: Path,
        market_truth: Path,
        split_assignments: Path,
        output_dir: Path,
    ) -> None:
        self.final_market = final_market.expanduser().resolve()
        self.market_products = market_products.expanduser().resolve()
        self.market_population = market_population.expanduser().resolve()
        self.canonical_user_events = canonical_user_events.expanduser().resolve()
        self.accepted_cases = accepted_cases.expanduser().resolve()
        self.case_shelf = case_shelf.expanduser().resolve()
        self.case_users = case_users.expanduser().resolve()
        self.choice_truth = choice_truth.expanduser().resolve()
        self.population_truth = population_truth.expanduser().resolve()
        self.market_truth = market_truth.expanduser().resolve()
        self.split_assignments = split_assignments.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        for path in (
            self.final_market, self.market_products, self.market_population,
            self.canonical_user_events, self.accepted_cases, self.case_shelf,
            self.case_users, self.choice_truth, self.population_truth,
            self.market_truth, self.split_assignments,
        ):
            if not path.is_file():
                raise FileNotFoundError(path)
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        self.con.execute("SET preserve_insertion_order=false")
        temp = self.output_dir / ".duckdb_tmp"
        temp.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"SET temp_directory={sql_literal(str(temp))}")

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

    def _write_splits(self) -> None:
        rows = self.con.execute("""
            SELECT split_name, market_id, case_candidate_id, evaluation_regime
            FROM read_parquet(?)
            ORDER BY split_name, market_id, t0, case_candidate_id
        """, [str(self.split_assignments)]).fetchall()
        by_split: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
        regimes: dict[tuple[str, str], str] = {}
        for split_name, market_id, case_id, regime in rows:
            by_split[str(split_name)][str(market_id)].append(str(case_id))
            if regime:
                regimes[(str(split_name), str(market_id))] = str(regime)
        for split_name in ("learning", "validation", "evaluation"):
            markets = []
            for market_id in sorted(by_split.get(split_name, {})):
                item: dict[str, Any] = {
                    "market_id": market_id,
                    "case_ids": by_split[split_name][market_id],
                }
                regime = regimes.get((split_name, market_id))
                if regime:
                    item["evaluation_regime"] = regime
                markets.append(item)
            write_json(self.output_dir / "splits" / f"{split_name}.json", {
                "split_name": split_name,
                "markets": markets,
            })

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        validation = validate_long_tables(
            self.con,
            self.accepted_cases,
            self.case_shelf,
            self.case_users,
            self.choice_truth,
            self.population_truth,
            self.market_truth,
            self.split_assignments,
        )
        write_json(self.output_dir / "validation_report.json", validation)
        if validation["status"] != "PASS":
            raise ValueError(f"benchmark validation failed: {validation['errors']}")

        market_rows = self.con.execute("""
            SELECT DISTINCT market_id FROM read_parquet(?) ORDER BY market_id
        """, [str(self.accepted_cases)]).fetchall()
        exported_cases = 0
        for (market_id_raw,) in market_rows:
            market_id = str(market_id_raw)
            market_sql = sql_literal(market_id)
            market_dir = self.output_dir / "markets" / market_id
            population_dir = market_dir / "population"
            cases_dir = market_dir / "cases"
            market_dir.mkdir(parents=True, exist_ok=True)

            final_row = self.con.execute("""
                SELECT market_label, source_partition, source_market_ids,
                       source_category_paths, product_count
                FROM read_parquet(?) WHERE market_id=?
            """, [str(self.final_market), market_id]).fetchone()
            if final_row is None:
                raise ValueError(f"accepted market missing from final_market: {market_id}")
            market_label, source_partition, source_market_ids, source_paths, product_count = final_row

            self._copy(f"""
                SELECT product_id, title, source_partition, category_path,
                       first_review_date, first_available_date, store,
                       metadata_available, metadata_snapshot_price
                FROM read_parquet({sql_literal(str(self.market_products))})
                WHERE market_id={market_sql}
                ORDER BY product_id
            """, market_dir / "products.parquet")
            self._copy(f"""
                SELECT user_id
                FROM read_parquet({sql_literal(str(self.market_population))})
                WHERE market_id={market_sql}
                ORDER BY user_id
            """, population_dir / "users.parquet")
            self._copy(f"""
                WITH users AS (
                    SELECT DISTINCT user_id
                    FROM read_parquet({sql_literal(str(self.market_population))})
                    WHERE market_id={market_sql}
                )
                SELECT e.user_id, e.product_id, e.event_timestamp AS timestamp,
                       e.rating, e.source_partition, e.verified_purchase
                FROM read_parquet({sql_literal(str(self.canonical_user_events))}) e
                JOIN users USING(user_id)
                ORDER BY e.user_id, e.event_timestamp, e.product_id
            """, population_dir / "interactions.parquet")

            case_rows = self.con.execute("""
                SELECT case_candidate_id, focal_product_id, t0,
                       evaluation_start, evaluation_end_exclusive, evaluation_days,
                       quality_status
                FROM read_parquet(?)
                WHERE market_id=?
                ORDER BY t0, case_candidate_id
            """, [str(self.accepted_cases), market_id]).fetchall()
            case_ids: list[str] = []
            for row in case_rows:
                case_id = str(row[0])
                case_ids.append(case_id)
                case_sql = sql_literal(case_id)
                case_dir = cases_dir / case_id
                gt_dir = case_dir / "ground_truth"
                self._copy(f"""
                    SELECT product_id, role, pre_t0_review_count,
                           pre_t0_rating_mean, price_at_t0,
                           pre_t0_recent_review_count, metadata_snapshot_price
                    FROM read_parquet({sql_literal(str(self.case_shelf))})
                    WHERE case_candidate_id={case_sql}
                    ORDER BY CASE WHEN role='focal' THEN 0 ELSE 1 END, product_id
                """, case_dir / "shelf.parquet")
                self._copy(f"""
                    SELECT user_id
                    FROM read_parquet({sql_literal(str(self.case_users))})
                    WHERE case_candidate_id={case_sql}
                    ORDER BY user_id
                """, case_dir / "users.parquet")
                self._copy(f"""
                    SELECT user_id, target_product_id, event_timestamp
                    FROM read_parquet({sql_literal(str(self.choice_truth))})
                    WHERE case_candidate_id={case_sql}
                    ORDER BY user_id
                """, gt_dir / "choice_truth.parquet")
                self._copy(f"""
                    SELECT user_id, outcome_product_id, event_timestamp
                    FROM read_parquet({sql_literal(str(self.population_truth))})
                    WHERE case_candidate_id={case_sql}
                    ORDER BY user_id
                """, gt_dir / "population_truth.parquet")
                self._copy(f"""
                    SELECT product_id, demand_count, demand_share, rank
                    FROM read_parquet({sql_literal(str(self.market_truth))})
                    WHERE case_candidate_id={case_sql}
                    ORDER BY rank, product_id
                """, gt_dir / "market_truth.parquet")
                n_shelf = int(self.con.execute(
                    "SELECT count(*) FROM read_parquet(?) WHERE case_candidate_id=?",
                    [str(self.case_shelf), case_id],
                ).fetchone()[0])
                n_users = int(self.con.execute(
                    "SELECT count(*) FROM read_parquet(?) WHERE case_candidate_id=?",
                    [str(self.case_users), case_id],
                ).fetchone()[0])
                write_json(case_dir / "case_manifest.json", json_safe({
                    "case_id": case_id,
                    "market_id": market_id,
                    "focal_product_id": row[1],
                    "t0": row[2],
                    "evaluation": {
                        "start": row[3],
                        "end_exclusive": row[4],
                        "days": row[5],
                    },
                    "n_shelf_products": n_shelf,
                    "n_selected_users": n_users,
                    "quality_status": row[6],
                }))
                exported_cases += 1

            n_population = int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?) WHERE market_id=?",
                [str(self.market_population), market_id],
            ).fetchone()[0])
            write_json(market_dir / "market_manifest.json", json_safe({
                "market_id": market_id,
                "market_name": market_label,
                "source_partition": source_partition,
                "source_market_ids": source_market_ids,
                "source_category_paths": source_paths,
                "n_products": product_count,
                "n_population_users": n_population,
                "case_ids": case_ids,
            }))

        self._write_splits()
        payload = {
            "status": "COMPLETE",
            "schema_version": "benchmark_export_v1",
            "market_count": len(market_rows),
            "case_count": exported_cases,
            "validation": validation,
            "layout": "Market -> Cases",
        }
        write_json(self.output_dir / "benchmark_manifest.json", payload)
        return payload
