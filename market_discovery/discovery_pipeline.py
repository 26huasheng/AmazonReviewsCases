from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable

import duckdb

from .cross_path_merge import (
    MIN_FINAL_MARKET_PRODUCT_COUNT,
    merge_exact_normalized_markets,
)
from .discovery_rules import (
    DISCOVERY_SEED,
    ROUND2_MAX_TITLES,
    MarketRule,
    adaptive_sample_size,
    classify_title,
    deterministic_rank,
    select_round2_sample,
    stable_local_market_id,
    stable_path_id,
)
from .market_llm import LLMResult, MarketLLMClient
from .prompts import (
    ARBITRATION_PROMPT_VERSION,
    MARKET_PROMPT_VERSION,
    prompt_hash,
    render_prompt_pair,
)
from utils import read_json, sql_literal, write_json


LOGGER = logging.getLogger(__name__)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]], mode: str = "w") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(mode, encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _json_columns(columns: dict[str, str]) -> str:
    return "{" + ", ".join(
        f"{sql_literal(name)}: {sql_literal(dtype)}" for name, dtype in columns.items()
    ) + "}"


class MarketDiscoveryPipeline:
    """Discover path-local markets and materialize deterministic cross-path final markets.

    Path-local market definition keeps the v5 two-round LLM + deterministic
    title matcher. Cross-path market merging is deliberately narrow: markets
    are merged only when their labels are identical after formatting
    normalization. Cross-path merging never calls an LLM.
    """

    def __init__(
        self,
        product_core: Path,
        output_root: Path,
        discovery_version: str,
        source_partition: str | None = None,
        product_core_cleaning: Path | None = None,
        min_final_market_product_count: int = MIN_FINAL_MARKET_PRODUCT_COUNT,
    ) -> None:
        self.product_core = product_core.expanduser().resolve()
        self.output_dir = output_root.expanduser().resolve() / discovery_version
        self.discovery_version = discovery_version
        self.source_partition = source_partition
        self.product_core_cleaning = (
            product_core_cleaning.expanduser().resolve() if product_core_cleaning else None
        )
        if min_final_market_product_count < 0:
            raise ValueError("min_final_market_product_count must be >= 0")
        self.min_final_market_product_count = min_final_market_product_count
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect()
        self.con.execute("SET threads=4")
        self.con.execute("SET preserve_insertion_order=false")
        duckdb_tmp = self.output_dir / ".duckdb_tmp"
        duckdb_tmp.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"SET temp_directory={sql_literal(str(duckdb_tmp))}")
        self.paths: list[dict[str, Any]] = []

    def close(self) -> None:
        self.con.close()

    def _copy(self, query: str, path: Path, csv: bool = False) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        options = "FORMAT CSV, HEADER true" if csv else "FORMAT PARQUET, COMPRESSION ZSTD"
        self.con.execute(f"COPY ({query}) TO {sql_literal(str(path))} ({options})")

    def prepare_local_evidence(self) -> dict[str, Any]:
        partition_filter = ""
        if self.source_partition:
            partition_filter = f"WHERE source_partition={sql_literal(self.source_partition)}"
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW product_input AS
            SELECT * FROM read_parquet({sql_literal(str(self.product_core))})
            {partition_filter}
        """)
        self.con.execute("""
            CREATE OR REPLACE TEMP VIEW products_valid AS
            SELECT * FROM product_input
            WHERE product_id IS NOT NULL AND trim(CAST(product_id AS VARCHAR)) <> ''
              AND product_title IS NOT NULL AND trim(product_title) <> ''
              AND category_path IS NOT NULL AND len(category_path) > 0
        """)
        audit = self._missing_path_audit()
        write_json(self.output_dir / "missing_path_audit.json", audit)

        grouped = self.con.execute("""
            SELECT source_partition, category_path, count(*) AS path_product_count
            FROM products_valid
            GROUP BY source_partition, category_path
            ORDER BY source_partition, category_path
        """).fetchall()
        self.paths = [{
            "source_partition": row[0],
            "category_path": row[1],
            "path_product_count": row[2],
            "path_id": stable_path_id(row[0], row[1]),
            "sample_target": adaptive_sample_size(row[2]),
            "round1_decision": None,
            "round2_used": False,
            "n_local_markets": None,
            "assigned_count": None,
            "ambiguous_count": None,
            "unmatched_count": None,
            "assigned_rate": None,
            "ambiguous_rate": None,
            "unmatched_rate": None,
            "status": "EVIDENCE_READY",
        } for row in grouped]
        self._register_paths()
        self._write_round1_samples()
        self._write_path_summary()
        summary = {
            "discovery_version": self.discovery_version,
            "product_core": str(self.product_core),
            "product_core_product_count": audit["product_core_product_count"],
            "product_core_invalid_product_id_count": audit["product_core_invalid_product_id_count"],
            "product_core_invalid_product_title_count": audit["product_core_invalid_product_title_count"],
            "product_core_missing_path_count": audit["product_core_missing_path_count"],
            "discovery_path_count": len(self.paths),
            "discovery_product_count": sum(row["path_product_count"] for row in self.paths),
            "round1_sample_title_count": self.con.execute(
                "SELECT count(*) FROM samples_round1"
            ).fetchone()[0],
            "estimated_round1_api_calls": len(self.paths),
            "paths_by_source_partition": dict(Counter(
                row["source_partition"] for row in self.paths
            )),
        }
        if self.product_core_cleaning and self.product_core_cleaning.exists():
            summary["product_core_build_cleaning"] = read_json(self.product_core_cleaning)
        write_json(self.output_dir / "dry_run_summary.json", summary)
        self._ensure_pending_outputs()
        return summary

    def _ensure_pending_outputs(self) -> None:
        ai_dir = self.output_dir / "ai_runs"
        ai_dir.mkdir(parents=True, exist_ok=True)
        for name in ("round1.jsonl", "round2.jsonl", "arbitration.jsonl"):
            (ai_dir / name).touch(exist_ok=True)
        empty_specs = {
            "samples_round2.parquet": """
                SELECT NULL::VARCHAR path_id, NULL::VARCHAR product_id,
                       NULL::VARCHAR product_title, NULL::VARCHAR provisional_group
                WHERE false
            """,
            "local_market_definitions.parquet": """
                SELECT NULL::VARCHAR discovery_version, NULL::VARCHAR source_partition,
                       NULL::VARCHAR path_id, NULL::VARCHAR[] category_path,
                       NULL::VARCHAR local_market_id, NULL::VARCHAR market_label,
                       NULL::VARCHAR center_term, NULL::VARCHAR[] equivalent_terms,
                       NULL::VARCHAR[] support_terms, NULL::BIGINT round_finalized,
                       NULL::DOUBLE confidence, NULL::VARCHAR status WHERE false
            """,
            "market_assignment.parquet": """
                SELECT NULL::VARCHAR discovery_version, NULL::VARCHAR source_partition,
                       NULL::VARCHAR product_id, NULL::VARCHAR path_id,
                       NULL::VARCHAR local_market_id, NULL::VARCHAR market_label,
                       NULL::VARCHAR assignment_status,
                       NULL::VARCHAR[] candidate_local_market_ids,
                       NULL::VARCHAR[] candidate_market_labels,
                       NULL::BIGINT top_score, NULL::BIGINT second_score,
                       NULL::VARCHAR matched_center_term,
                       NULL::VARCHAR[] matched_equivalent_terms,
                       NULL::VARCHAR[] matched_support_terms,
                       NULL::BOOLEAN ambiguous_llm_used,
                       NULL::VARCHAR ambiguous_llm_selected_market WHERE false
            """,
        }
        for name, query in empty_specs.items():
            path = self.output_dir / name
            if not path.exists():
                self._copy(query, path)
        definitions_csv = self.output_dir / "local_market_definitions.csv"
        if not definitions_csv.exists():
            self._copy(empty_specs["local_market_definitions.parquet"], definitions_csv, csv=True)
        overlap_csv = self.output_dir / "cross_path_market_overlap.csv"
        if not overlap_csv.exists():
            self._copy("""
                SELECT NULL::VARCHAR source_partition,
                       NULL::VARCHAR normalized_market_label,
                       NULL::BIGINT n_paths,
                       NULL::BIGINT n_local_markets,
                       NULL::BIGINT affected_assigned_products,
                       NULL::VARCHAR[] path_ids,
                       NULL::VARCHAR[] local_market_ids WHERE false
            """, overlap_csv, csv=True)
        overlap_summary = self.output_dir / "cross_path_market_overlap_summary.json"
        if not overlap_summary.exists():
            write_json(overlap_summary, {"status": "waiting_for_discovery_results"})

    def _missing_path_audit(self) -> dict[str, Any]:
        counts = self.con.execute("""
            SELECT count(*) AS total,
                   count(*) FILTER (product_id IS NULL OR trim(CAST(product_id AS VARCHAR)) = '') AS invalid_id,
                   count(*) FILTER (product_title IS NULL OR trim(product_title) = '') AS invalid_title,
                   count(*) FILTER (category_path IS NULL OR len(category_path) = 0) AS missing_path
            FROM product_input
        """).fetchone()
        per_partition_rows = self.con.execute("""
            SELECT source_partition, count(*) product_count,
                   count(*) FILTER (category_path IS NULL OR len(category_path)=0) missing_path_count
            FROM product_input GROUP BY source_partition ORDER BY source_partition
        """).fetchall()
        return {
            "product_core_product_count": counts[0],
            "product_core_invalid_product_id_count": counts[1],
            "product_core_invalid_product_title_count": counts[2],
            "product_core_missing_path_count": counts[3],
            "product_core_missing_path_rate": counts[3] / counts[0] if counts[0] else 0.0,
            "by_source_partition": [{
                "source_partition": row[0],
                "product_count": row[1],
                "missing_path_count": row[2],
                "missing_path_rate": row[2] / row[1] if row[1] else 0.0,
            } for row in per_partition_rows],
        }

    def _register_paths(self) -> None:
        self.con.execute("""
            CREATE OR REPLACE TEMP TABLE discovery_paths(
                source_partition VARCHAR, path_id VARCHAR, category_path VARCHAR[],
                path_product_count BIGINT, sample_target BIGINT
            )
        """)
        if self.paths:
            self.con.executemany("INSERT INTO discovery_paths VALUES (?, ?, ?, ?, ?)", [
                (row["source_partition"], row["path_id"], row["category_path"],
                 row["path_product_count"], row["sample_target"])
                for row in self.paths
            ])
        self.con.execute("""
            CREATE OR REPLACE TEMP VIEW product_paths AS
            SELECT p.source_partition, p.product_id, p.product_title, d.path_id,
                   d.category_path, d.path_product_count, d.sample_target
            FROM products_valid p
            JOIN discovery_paths d USING (source_partition, category_path)
        """)

    def _write_round1_samples(self) -> None:
        seed = sql_literal(DISCOVERY_SEED)
        version = sql_literal(self.discovery_version)
        query = f"""
            WITH ranked AS (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY path_id
                           ORDER BY sha256(to_json(list_value(
                               {seed}, {version}, path_id, product_id)))
                       ) AS sample_rank
                FROM product_paths
            )
            SELECT path_id, product_id, product_title, 'sample' AS sample_role
            FROM ranked
            WHERE sample_rank <= sample_target
        """
        path = self.output_dir / "samples_round1.parquet"
        self._copy(query, path)
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW samples_round1 AS
            SELECT * FROM read_parquet({sql_literal(str(path))})
        """)
        evidence_path = self.output_dir / "evidence_round1.jsonl"
        rows: list[dict[str, Any]] = []
        for path_row in self.paths:
            samples = self.con.execute("""
                SELECT product_id, product_title, sample_role FROM samples_round1
                WHERE path_id=?
            """, [path_row["path_id"]]).fetchall()
            rows.append({
                "path_id": path_row["path_id"],
                "source_partition": path_row["source_partition"],
                "category_path": path_row["category_path"],
                "path_product_count": path_row["path_product_count"],
                "samples": [
                    {"product_id": row[0], "title": row[1], "sample_role": row[2]}
                    for row in samples
                ],
            })
        _write_jsonl(evidence_path, rows)

    def _write_path_summary(self) -> None:
        jsonl = self.output_dir / ".path_summary.jsonl"
        _write_jsonl(jsonl, self.paths)
        columns = {
            "source_partition": "VARCHAR",
            "path_id": "VARCHAR",
            "category_path": "VARCHAR[]",
            "path_product_count": "BIGINT",
            "sample_target": "BIGINT",
            "round1_decision": "VARCHAR",
            "round2_used": "BOOLEAN",
            "n_local_markets": "BIGINT",
            "assigned_count": "BIGINT",
            "ambiguous_count": "BIGINT",
            "unmatched_count": "BIGINT",
            "assigned_rate": "DOUBLE",
            "ambiguous_rate": "DOUBLE",
            "unmatched_rate": "DOUBLE",
            "status": "VARCHAR",
        }
        source = (
            f"read_json({sql_literal(str(jsonl))}, format='newline_delimited', "
            f"columns={_json_columns(columns)})"
        )
        self._copy(
            f"SELECT * FROM {source} ORDER BY source_partition, path_id",
            self.output_dir / "path_summary.parquet",
        )
        self._copy(
            f"SELECT * FROM {source} ORDER BY source_partition, path_id",
            self.output_dir / "path_summary.csv",
            csv=True,
        )
        jsonl.unlink()

    def run_discovery(
        self,
        client: MarketLLMClient,
        max_paths: int | None,
        resume: bool,
        llm_workers: int = 1,
    ) -> dict[str, Any]:
        round1_log = self.output_dir / "ai_runs/round1.jsonl"
        round2_log = self.output_dir / "ai_runs/round2.jsonl"
        if not resume and any(
            path.exists() and path.stat().st_size > 0 for path in (round1_log, round2_log)
        ):
            raise RuntimeError("AI run logs already exist; use --resume or a new discovery-version")

        successful1 = self._successful_runs(round1_log) if resume else {}
        successful2 = self._successful_runs(round2_log) if resume else {}
        successful_arbitrations = self._successful_arbitrations() if resume else {}
        evidence = {
            row["path_id"]: row
            for row in _read_jsonl(self.output_dir / "evidence_round1.jsonl")
        }
        selected = self.paths[:max_paths] if max_paths is not None else self.paths
        assignment_jsonl = self.output_dir / ".market_assignment.jsonl"
        definitions: list[dict[str, Any]] = []
        round2_samples: list[dict[str, Any]] = []
        assignment_jsonl.write_text("", encoding="utf-8")

        pending = [
            row for row in selected
            if row["path_id"] not in successful1 and row["path_id"] in evidence
        ]
        round1_results: dict[str, LLMResult] = {}
        round1_errors: dict[str, Exception] = {}

        if pending and llm_workers > 1:
            def _fetch(row: dict[str, Any]) -> tuple[str, LLMResult | None, Exception | None]:
                pid = row["path_id"]
                try:
                    return pid, client.call(evidence[pid], 1), None
                except Exception as exc:  # noqa: BLE001
                    return pid, None, exc

            with ThreadPoolExecutor(max_workers=llm_workers) as pool:
                for pid, result, error in pool.map(_fetch, pending):
                    if error is not None:
                        round1_errors[pid] = error
                    elif result is not None:
                        round1_results[pid] = result
        else:
            for row in pending:
                pid = row["path_id"]
                try:
                    round1_results[pid] = client.call(evidence[pid], 1)
                except Exception as exc:  # noqa: BLE001
                    round1_errors[pid] = exc

        for path_row in selected:
            path_id = path_row["path_id"]
            error_log = round1_log
            error_evidence = evidence[path_id]
            try:
                response1 = successful1.get(path_id)
                if response1 is None:
                    if path_id in round1_errors:
                        raise round1_errors[path_id]
                    result = round1_results.get(path_id) or client.call(evidence[path_id], 1)
                    response1 = result.parsed_response
                    self._log_success(round1_log, path_row, evidence[path_id], result)

                path_row["round1_decision"] = response1["decision"]
                if response1["decision"] == "REVIEW":
                    self._write_review_assignments(path_row, assignment_jsonl)
                    definitions.append(self._review_definition(path_row, 1))
                    path_row["status"] = "REVIEW"
                    continue

                rules1 = self._rules(path_id, response1)
                provisional_fd, provisional_name = tempfile.mkstemp(
                    prefix="provisional_", suffix=".jsonl", dir=self.output_dir
                )
                os.close(provisional_fd)
                provisional_path = Path(provisional_name)
                counts, groups = self._match_path(
                    path_row,
                    rules1,
                    provisional_path,
                    client=client,
                    arbitrate=(response1["decision"] == "KEEP"),
                    successful_arbitrations=successful_arbitrations,
                )
                final_response = response1
                final_rules = rules1
                final_round = 1

                if response1["decision"] == "SPLIT":
                    path_row["round2_used"] = True
                    samples2 = select_round2_sample(
                        groups, self.discovery_version, path_id, ROUND2_MAX_TITLES
                    )
                    round2_samples.extend({
                        "path_id": path_id,
                        "product_id": row["product_id"],
                        "product_title": row["product_title"],
                        "provisional_group": row["provisional_group"],
                    } for row in samples2)
                    evidence2 = {
                        "path_id": path_id,
                        "source_partition": path_row["source_partition"],
                        "category_path": path_row["category_path"],
                        "path_product_count": path_row["path_product_count"],
                        "round1_markets": response1["markets"],
                        "match_counts": counts,
                        "samples": samples2,
                    }
                    error_log = round2_log
                    error_evidence = evidence2
                    response2 = successful2.get(path_id)
                    if response2 is None:
                        result2 = client.call(evidence2, 2)
                        response2 = result2.parsed_response
                        self._log_success(round2_log, path_row, evidence2, result2)
                    if response2["decision"] == "REVIEW":
                        provisional_path.unlink(missing_ok=True)
                        self._write_review_assignments(path_row, assignment_jsonl)
                        definitions.append(self._review_definition(path_row, 2))
                        path_row["status"] = "REVIEW"
                        continue

                    final_response = response2
                    final_rules = self._rules(path_id, response2)
                    final_round = 2
                    provisional_path.unlink(missing_ok=True)
                    final_fd, final_name = tempfile.mkstemp(
                        prefix="final_", suffix=".jsonl", dir=self.output_dir
                    )
                    os.close(final_fd)
                    final_path = Path(final_name)
                    counts, _ = self._match_path(
                        path_row,
                        final_rules,
                        final_path,
                        client=client,
                        arbitrate=True,
                        successful_arbitrations=successful_arbitrations,
                    )
                else:
                    final_path = provisional_path

                with final_path.open(encoding="utf-8") as source, assignment_jsonl.open(
                    "a", encoding="utf-8"
                ) as target:
                    for line in source:
                        target.write(line)
                final_path.unlink(missing_ok=True)

                definitions.extend(self._definitions(path_row, final_response, final_round))
                self._update_counts(path_row, counts, len(final_rules))
                path_row["status"] = "FINAL"
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Market discovery failed for %s", path_id)
                self._log_error(error_log, path_row, error_evidence, exc, client)
                self._write_review_assignments(path_row, assignment_jsonl)
                definitions.append(self._review_definition(path_row, 1))
                path_row["status"] = "ERROR"

        _write_jsonl(self.output_dir / ".definitions.jsonl", definitions)
        _write_jsonl(self.output_dir / ".samples_round2.jsonl", round2_samples)
        self._materialize_discovery_outputs(assignment_jsonl)
        self._write_path_summary()
        self._write_first_market()
        cross_audit = self._write_cross_path_audit()
        exact_merge = merge_exact_normalized_markets(
            self.output_dir,
            self.con,
            min_product_count=self.min_final_market_product_count,
        )
        return {
            "processed_paths": len(selected),
            "cross_path_audit": cross_audit,
            "exact_name_merge": exact_merge,
        }

    def _successful_runs(self, path: Path) -> dict[str, dict[str, Any]]:
        successful: dict[str, dict[str, Any]] = {}
        for row in _read_jsonl(path):
            if row.get("call_status") != "SUCCESS":
                continue
            if row.get("prompt_version") != MARKET_PROMPT_VERSION:
                raise RuntimeError(
                    f"STALE_PROMPT_RESULT path_id={row.get('path_id')} "
                    f"stored={row.get('prompt_version')} current={MARKET_PROMPT_VERSION}"
                )
            successful[row["path_id"]] = row["parsed_response"]
        return successful

    def _log_success(
        self,
        log: Path,
        path_row: dict[str, Any],
        evidence: dict[str, Any],
        result: Any,
    ) -> None:
        system_prompt, user_prompt = render_prompt_pair(
            evidence, 2 if "round1_markets" in evidence else 1
        )
        _write_jsonl(log, [{
            "run_id": str(uuid.uuid4()),
            "discovery_version": self.discovery_version,
            "path_id": path_row["path_id"],
            "model": result.model,
            "provider": result.provider,
            "prompt_version": MARKET_PROMPT_VERSION,
            "system_prompt_sha256": prompt_hash(system_prompt),
            "user_prompt_sha256": prompt_hash(user_prompt),
            "sample_product_ids": [row["product_id"] for row in evidence.get("samples", [])],
            "parsed_response": result.parsed_response,
            "call_status": "SUCCESS",
            "error": None,
            "input_tokens": result.input_tokens,
            "output_tokens": result.output_tokens,
            "total_tokens": result.total_tokens,
        }], mode="a")

    def _log_error(
        self,
        log: Path,
        path_row: dict[str, Any],
        evidence: dict[str, Any],
        exc: Exception,
        client: Any,
    ) -> None:
        system_prompt, user_prompt = render_prompt_pair(
            evidence, 2 if "round1_markets" in evidence else 1
        )
        _write_jsonl(log, [{
            "run_id": str(uuid.uuid4()),
            "discovery_version": self.discovery_version,
            "path_id": path_row["path_id"],
            "model": getattr(client, "model", None),
            "provider": getattr(client, "provider", None),
            "sample_product_ids": [row["product_id"] for row in evidence.get("samples", [])],
            "parsed_response": None,
            "call_status": "ERROR",
            "prompt_version": MARKET_PROMPT_VERSION,
            "system_prompt_sha256": prompt_hash(system_prompt),
            "user_prompt_sha256": prompt_hash(user_prompt),
            "error": f"{type(exc).__name__}: {exc}",
            "input_tokens": None,
            "output_tokens": None,
            "total_tokens": None,
        }], mode="a")

    def _rules(self, path_id: str, response: dict[str, Any]) -> list[MarketRule]:
        return [
            MarketRule(
                stable_local_market_id(path_id, market["market_label"]),
                market["market_label"],
                market["center_term"],
                tuple(market["equivalent_terms"]),
                tuple(market["support_terms"]),
                market["confidence"],
            )
            for market in response["markets"]
        ]

    def _match_path(
        self,
        path_row: dict[str, Any],
        rules: list[MarketRule],
        output: Path,
        client: Any = None,
        arbitrate: bool = False,
        successful_arbitrations: dict[str, str | None] | None = None,
    ) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
        successful_arbitrations = successful_arbitrations or {}
        cursor = self.con.execute("""
            SELECT product_id, product_title FROM product_paths
            WHERE path_id=? ORDER BY product_id
        """, [path_row["path_id"]])
        counts: Counter[str] = Counter()
        market_counts: Counter[str] = Counter()
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)

        with output.open("w", encoding="utf-8") as handle:
            while True:
                batch = cursor.fetchmany(10_000)
                if not batch:
                    break
                for product_id, title in batch:
                    result = classify_title(title, rules)
                    if result["assignment_status"] == "AMBIGUOUS" and arbitrate and client is not None:
                        result = self._arbitrate_product(
                            path_row,
                            product_id,
                            title,
                            result,
                            client,
                            successful_arbitrations,
                        )

                    counts[result["assignment_status"]] += 1
                    if result["assignment_status"] == "ASSIGNED":
                        market_counts[result["market_label"]] += 1

                    group = (
                        result["market_label"]
                        if result["assignment_status"] == "ASSIGNED"
                        else result["assignment_status"]
                    )
                    sample_row = {
                        "product_id": product_id,
                        "product_title": title,
                        "_rank": deterministic_rank(
                            self.discovery_version,
                            path_row["path_id"] + ":round2:" + group,
                            product_id,
                        ),
                    }
                    groups[group].append(sample_row)
                    groups[group].sort(key=lambda row: row["_rank"])
                    if len(groups[group]) > ROUND2_MAX_TITLES:
                        groups[group].pop()

                    record = {
                        "discovery_version": self.discovery_version,
                        "source_partition": path_row["source_partition"],
                        "product_id": product_id,
                        "path_id": path_row["path_id"],
                        **result,
                    }
                    handle.write(json.dumps(record, ensure_ascii=False) + "\n")

        total = path_row["path_product_count"]
        statistics: dict[str, Any] = dict(counts)
        statistics["provisional_markets"] = [{
            "market_label": rule.market_label,
            "local_market_id": rule.local_market_id,
            "assigned_count": market_counts.get(rule.market_label, 0),
            "assigned_rate": market_counts.get(rule.market_label, 0) / total if total else 0.0,
        } for rule in rules]
        statistics["ambiguous_count"] = counts.get("AMBIGUOUS", 0)
        statistics["ambiguous_rate"] = counts.get("AMBIGUOUS", 0) / total if total else 0.0
        statistics["unmatched_count"] = counts.get("UNMATCHED", 0)
        statistics["unmatched_rate"] = counts.get("UNMATCHED", 0) / total if total else 0.0
        return statistics, {
            group: [
                {key: value for key, value in row.items() if key != "_rank"}
                for row in rows
            ]
            for group, rows in groups.items()
        }

    def _arbitrate_product(
        self,
        path_row: dict[str, Any],
        product_id: str,
        title: str,
        result: dict[str, Any],
        client: Any,
        successful_arbitrations: dict[str, str | None],
    ) -> dict[str, Any]:
        candidates = list(zip(
            result["candidate_local_market_ids"],
            result["candidate_market_labels"],
        ))
        if not candidates:
            return {
                **result,
                "ambiguous_llm_used": True,
                "ambiguous_llm_selected_market": None,
            }
        labels = [label for _, label in candidates]
        if product_id in successful_arbitrations:
            selected = successful_arbitrations[product_id]
            self._log_arbitration(product_id, title, labels, selected, reused=True)
        else:
            try:
                selected = client.arbitrate(title, labels, product_id=product_id)
            except Exception as exc:  # noqa: BLE001
                LOGGER.info("Arbitration failed for %s: %s", product_id, exc)
                selected = None
            self._log_arbitration(product_id, title, labels, selected)
        market_id = next(
            (candidate_id for candidate_id, label in candidates if label == selected),
            None,
        )
        if selected is not None and market_id is not None:
            return {
                **result,
                "assignment_status": "ASSIGNED",
                "local_market_id": market_id,
                "market_label": selected,
                "ambiguous_llm_used": True,
                "ambiguous_llm_selected_market": selected,
            }
        return {
            **result,
            "ambiguous_llm_used": True,
            "ambiguous_llm_selected_market": None,
        }

    def _successful_arbitrations(self) -> dict[str, str | None]:
        log = self.output_dir / "ai_runs/arbitration.jsonl"
        successful: dict[str, str | None] = {}
        for row in _read_jsonl(log):
            if row.get("call_status") != "SUCCESS":
                continue
            if row.get("prompt_version") != ARBITRATION_PROMPT_VERSION:
                raise RuntimeError(
                    f"STALE_ARBITRATION_RESULT product_id={row.get('product_id')} "
                    f"stored={row.get('prompt_version')} current={ARBITRATION_PROMPT_VERSION}"
                )
            successful[str(row["product_id"])] = row.get("selected_market")
        return successful

    def _log_arbitration(
        self,
        product_id: str,
        title: str,
        candidates: list[str],
        selected: str | None,
        reused: bool = False,
    ) -> None:
        log = self.output_dir / "ai_runs/arbitration.jsonl"
        _write_jsonl(log, [{
            "run_id": str(uuid.uuid4()),
            "discovery_version": self.discovery_version,
            "product_id": str(product_id),
            "title": title,
            "candidates": candidates,
            "selected_market": selected,
            "prompt_version": ARBITRATION_PROMPT_VERSION,
            "call_status": "SUCCESS" if selected is not None else "UNRESOLVED",
            "reused": reused,
        }], mode="a")

    def _write_review_assignments(self, path_row: dict[str, Any], output: Path) -> None:
        cursor = self.con.execute(
            "SELECT product_id FROM product_paths WHERE path_id=? ORDER BY product_id",
            [path_row["path_id"]],
        )
        with output.open("a", encoding="utf-8") as handle:
            while True:
                batch = cursor.fetchmany(10_000)
                if not batch:
                    break
                for (product_id,) in batch:
                    handle.write(json.dumps({
                        "discovery_version": self.discovery_version,
                        "source_partition": path_row["source_partition"],
                        "product_id": product_id,
                        "path_id": path_row["path_id"],
                        "local_market_id": None,
                        "market_label": None,
                        "assignment_status": "REVIEW",
                        "candidate_local_market_ids": [],
                        "candidate_market_labels": [],
                        "top_score": None,
                        "second_score": None,
                        "matched_center_term": None,
                        "matched_equivalent_terms": [],
                        "matched_support_terms": [],
                        "ambiguous_llm_used": False,
                        "ambiguous_llm_selected_market": None,
                    }, ensure_ascii=False) + "\n")

    def _definitions(
        self,
        path_row: dict[str, Any],
        response: dict[str, Any],
        round_number: int,
    ) -> list[dict[str, Any]]:
        return [{
            "discovery_version": self.discovery_version,
            "source_partition": path_row["source_partition"],
            "path_id": path_row["path_id"],
            "category_path": path_row["category_path"],
            "local_market_id": stable_local_market_id(
                path_row["path_id"], market["market_label"]
            ),
            **market,
            "round_finalized": round_number,
            "status": "FINAL",
        } for market in response["markets"]]

    def _review_definition(self, path_row: dict[str, Any], round_number: int) -> dict[str, Any]:
        return {
            "discovery_version": self.discovery_version,
            "source_partition": path_row["source_partition"],
            "path_id": path_row["path_id"],
            "category_path": path_row["category_path"],
            "local_market_id": None,
            "market_label": None,
            "center_term": None,
            "equivalent_terms": [],
            "support_terms": [],
            "round_finalized": round_number,
            "confidence": None,
            "status": "REVIEW",
        }

    def _update_counts(
        self,
        path_row: dict[str, Any],
        counts: dict[str, Any],
        markets: int,
    ) -> None:
        total = path_row["path_product_count"]
        path_row["n_local_markets"] = markets
        for key, field in (
            ("ASSIGNED", "assigned"),
            ("AMBIGUOUS", "ambiguous"),
            ("UNMATCHED", "unmatched"),
        ):
            value = counts.get(key, 0)
            path_row[field + "_count"] = value
            path_row[field + "_rate"] = value / total if total else 0.0

    def _materialize_discovery_outputs(self, assignment_jsonl: Path) -> None:
        assignment_columns = {
            "discovery_version": "VARCHAR",
            "source_partition": "VARCHAR",
            "product_id": "VARCHAR",
            "path_id": "VARCHAR",
            "local_market_id": "VARCHAR",
            "market_label": "VARCHAR",
            "assignment_status": "VARCHAR",
            "candidate_local_market_ids": "VARCHAR[]",
            "candidate_market_labels": "VARCHAR[]",
            "top_score": "BIGINT",
            "second_score": "BIGINT",
            "matched_center_term": "VARCHAR",
            "matched_equivalent_terms": "VARCHAR[]",
            "matched_support_terms": "VARCHAR[]",
            "ambiguous_llm_used": "BOOLEAN",
            "ambiguous_llm_selected_market": "VARCHAR",
        }
        source = (
            f"read_json({sql_literal(str(assignment_jsonl))}, format='newline_delimited', "
            f"columns={_json_columns(assignment_columns)})"
        )
        self._copy(
            f"SELECT * FROM {source} ORDER BY source_partition, path_id, product_id",
            self.output_dir / "market_assignment.parquet",
        )

        definition_columns = {
            "discovery_version": "VARCHAR",
            "source_partition": "VARCHAR",
            "path_id": "VARCHAR",
            "category_path": "VARCHAR[]",
            "local_market_id": "VARCHAR",
            "market_label": "VARCHAR",
            "center_term": "VARCHAR",
            "equivalent_terms": "VARCHAR[]",
            "support_terms": "VARCHAR[]",
            "round_finalized": "BIGINT",
            "confidence": "DOUBLE",
            "status": "VARCHAR",
        }
        definitions_jsonl = self.output_dir / ".definitions.jsonl"
        definition_source = (
            f"read_json({sql_literal(str(definitions_jsonl))}, format='newline_delimited', "
            f"columns={_json_columns(definition_columns)})"
        )
        self._copy(
            f"SELECT * FROM {definition_source} ORDER BY source_partition, path_id, market_label",
            self.output_dir / "local_market_definitions.parquet",
        )
        self._copy(
            f"SELECT * FROM {definition_source} ORDER BY source_partition, path_id, market_label",
            self.output_dir / "local_market_definitions.csv",
            csv=True,
        )

        sample_columns = {
            "path_id": "VARCHAR",
            "product_id": "VARCHAR",
            "product_title": "VARCHAR",
            "provisional_group": "VARCHAR",
        }
        samples_jsonl = self.output_dir / ".samples_round2.jsonl"
        sample_source = (
            f"read_json({sql_literal(str(samples_jsonl))}, format='newline_delimited', "
            f"columns={_json_columns(sample_columns)})"
        )
        self._copy(
            f"SELECT * FROM {sample_source} ORDER BY path_id, provisional_group, product_id",
            self.output_dir / "samples_round2.parquet",
        )

    def _write_first_market(self) -> None:
        from .market_io import parquet_to_csv, write_market_parquet_from_relation

        definitions = sql_literal(str(self.output_dir / "local_market_definitions.parquet"))
        assignments = sql_literal(str(self.output_dir / "market_assignment.parquet"))
        query = f"""
            SELECT d.discovery_version,
                   d.source_partition,
                   d.local_market_id AS market_id,
                   d.market_label,
                   list_value(d.local_market_id) AS source_market_ids,
                   list_value(d.path_id) AS source_path_ids,
                   list_value(d.category_path) AS source_category_paths,
                   list_filter(list(a.product_id), x -> x IS NOT NULL) AS product_ids,
                   count(DISTINCT a.product_id)::BIGINT AS product_count
            FROM read_parquet({definitions}) d
            LEFT JOIN read_parquet({assignments}) a
              ON a.local_market_id = d.local_market_id
             AND a.source_partition = d.source_partition
            WHERE d.status='FINAL' AND d.local_market_id IS NOT NULL
            GROUP BY d.discovery_version, d.source_partition, d.local_market_id, d.market_label,
                     d.path_id, d.category_path
        """
        parquet = self.output_dir / "first_market.parquet"
        write_market_parquet_from_relation(self.con, query, parquet)
        parquet_to_csv(self.con, parquet, self.output_dir / "first_market.csv")

    def _write_cross_path_audit(self) -> dict[str, Any]:
        definitions = self.output_dir / "local_market_definitions.parquet"
        assignments = self.output_dir / "market_assignment.parquet"
        if not definitions.exists() or not assignments.exists():
            return {"status": "waiting_for_discovery_results"}

        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW final_definitions AS
            SELECT *, lower(trim(regexp_replace(market_label, '[^a-zA-Z0-9]+', '_', 'g')))
                AS normalized_market_label
            FROM read_parquet({sql_literal(str(definitions))})
            WHERE status='FINAL' AND market_label IS NOT NULL
        """)
        overlap_query = f"""
            WITH duplicated AS (
                SELECT source_partition, normalized_market_label,
                       count(DISTINCT path_id) AS n_paths,
                       count(*) AS n_local_markets,
                       list(path_id ORDER BY path_id) AS path_ids,
                       list(local_market_id ORDER BY local_market_id) AS local_market_ids
                FROM final_definitions
                GROUP BY source_partition, normalized_market_label
                HAVING count(DISTINCT path_id) >= 2
            ), assigned AS (
                SELECT d.source_partition, d.normalized_market_label,
                       count(*) FILTER (a.assignment_status='ASSIGNED') AS affected_assigned_products
                FROM final_definitions d
                JOIN read_parquet({sql_literal(str(assignments))}) a
                  ON a.local_market_id=d.local_market_id
                 AND a.source_partition=d.source_partition
                GROUP BY d.source_partition, d.normalized_market_label
            )
            SELECT d.source_partition, d.normalized_market_label,
                   d.n_paths, d.n_local_markets,
                   coalesce(a.affected_assigned_products,0) AS affected_assigned_products,
                   d.path_ids, d.local_market_ids
            FROM duplicated d
            LEFT JOIN assigned a
              USING(source_partition, normalized_market_label)
            ORDER BY affected_assigned_products DESC,
                     source_partition, normalized_market_label
        """
        csv_path = self.output_dir / "cross_path_market_overlap.csv"
        self._copy(overlap_query, csv_path, csv=True)

        total_markets, unique_labels = self.con.execute("""
            SELECT count(*),
                   count(DISTINCT source_partition || chr(31) || normalized_market_label)
            FROM final_definitions
        """).fetchone()
        rows = self.con.execute(overlap_query).fetchall()
        n_final = sum(row["status"] == "FINAL" for row in self.paths)
        n_review = sum(row["status"] == "REVIEW" for row in self.paths)
        n_error = sum(row["status"] == "ERROR" for row in self.paths)
        completed_paths = n_final + n_review
        total_paths = len(self.paths)
        error_rate = n_error / total_paths if total_paths else 0.0
        acceptable = error_rate < 0.05
        summary = {
            "status": "complete" if n_error == 0 or acceptable else "partial_discovery_results",
            "discovery_path_count": total_paths,
            "paths_with_results": completed_paths,
            "n_final": n_final,
            "n_review": n_review,
            "n_error": n_error,
            "error_rate": round(error_rate, 4),
            "local_market_count": total_markets,
            "unique_normalized_market_label_count": unique_labels,
            "cross_path_duplicate_label_group_count": len(rows),
            "affected_local_market_count": sum(row[3] for row in rows),
            "affected_path_count": len({path for row in rows for path in row[5]}),
            "affected_assigned_product_count": sum(row[4] for row in rows),
        }
        write_json(self.output_dir / "cross_path_market_overlap_summary.json", summary)
        return summary
