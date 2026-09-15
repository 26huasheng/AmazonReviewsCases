#!/usr/bin/env python3
"""Apply a frozen manual clean-market registry onto Quality accepted tables.

Does not judge whether a Market is clean. Replay rule is name-based:
accepted_cases.market_label must appear as a CLEAN registry market_name.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils import sql_literal, write_json


QUALITY_TABLES = {
    "accepted_cases": "accepted_cases.parquet",
    "accepted_focals": "accepted_focals.parquet",
    "accepted_comps": "accepted_focal_competitors.parquet",
    "accepted_shelf": "accepted_case_shelf.parquet",
    "focal_metrics": "quality_focal_metrics.parquet",
    "case_decisions": "quality_case_decisions.parquet",
    "case_metrics": "quality_case_metrics.parquet",
    "focal_decisions": "quality_focal_decisions.parquet",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_atomic(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    part.unlink(missing_ok=True)
    con.execute(
        f"COPY ({query}) TO {sql_literal(str(part))} (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    os.replace(part, path)


def load_registry(con: duckdb.DuckDBPyConnection, registry: Path) -> dict[str, Any]:
    src = sql_literal(str(registry))
    con.execute(f"""
        CREATE OR REPLACE TABLE registry AS
        SELECT
            CAST(source_partition AS VARCHAR) AS source_partition,
            CAST(market_id AS VARCHAR) AS market_id,
            CAST(market_name AS VARCHAR) AS market_name,
            CAST(audit_status AS VARCHAR) AS audit_status,
            CAST(provenance AS VARCHAR) AS provenance,
            TRY_CAST(product_count AS BIGINT) AS product_count,
            TRY_CAST(n_paths AS BIGINT) AS n_paths,
            TRY_CAST(n_source_markets AS BIGINT) AS n_source_markets
        FROM read_csv_auto({src}, header=true)
        WHERE audit_status = 'CLEAN'
    """)
    n = int(con.execute("SELECT count(*) FROM registry").fetchone()[0])
    n_names = int(con.execute("SELECT count(DISTINCT market_name) FROM registry").fetchone()[0])
    dupes = con.execute("""
        SELECT market_name, count(*) AS n
        FROM registry
        GROUP BY market_name
        HAVING count(*) > 1
        ORDER BY market_name
    """).fetchall()
    return {"row_count": n, "distinct_names": n_names, "duplicate_names": dupes}


def apply_registry(quality_dir: Path, registry: Path, output_dir: Path) -> dict[str, Any]:
    quality_dir = quality_dir.expanduser().resolve()
    registry = registry.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    missing = [quality_dir / name for name in QUALITY_TABLES.values() if not (quality_dir / name).is_file()]
    if missing:
        raise FileNotFoundError("missing quality inputs:\n" + "\n".join(str(p) for p in missing))
    if not registry.is_file():
        raise FileNotFoundError(registry)

    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    try:
        meta = load_registry(con, registry)
        if meta["duplicate_names"]:
            raise SystemExit(
                "registry has duplicate market_name values; name-based replay is ambiguous:\n"
                + json.dumps(
                    [{"market_name": r[0], "n": int(r[1])} for r in meta["duplicate_names"]],
                    indent=2,
                )
            )

        cases = sql_literal(str(quality_dir / QUALITY_TABLES["accepted_cases"]))
        focals = sql_literal(str(quality_dir / QUALITY_TABLES["accepted_focals"]))
        comps = sql_literal(str(quality_dir / QUALITY_TABLES["accepted_comps"]))
        shelf = sql_literal(str(quality_dir / QUALITY_TABLES["accepted_shelf"]))
        focal_metrics = sql_literal(str(quality_dir / QUALITY_TABLES["focal_metrics"]))
        case_decisions = sql_literal(str(quality_dir / QUALITY_TABLES["case_decisions"]))
        case_metrics = sql_literal(str(quality_dir / QUALITY_TABLES["case_metrics"]))
        focal_decisions = sql_literal(str(quality_dir / QUALITY_TABLES["focal_decisions"]))

        copy_atomic(
            con,
            """
            SELECT market_id, market_name AS market_label, product_count, n_paths, n_source_markets
            FROM registry
            ORDER BY market_id
            """,
            output_dir / "clean_markets.parquet",
        )
        con.execute(f"""
            COPY (
                SELECT market_id, market_name AS market_label, product_count, n_paths, n_source_markets
                FROM registry
                ORDER BY market_id
            ) TO {sql_literal(str(output_dir / "clean_markets.csv"))} (HEADER, DELIMITER ',')
        """)

        copy_atomic(
            con,
            f"""
            SELECT c.*
            FROM read_parquet({cases}) c
            WHERE c.market_label IN (SELECT market_name FROM registry)
            ORDER BY c.case_id
            """,
            output_dir / QUALITY_TABLES["accepted_cases"],
        )
        copy_atomic(
            con,
            f"""
            SELECT f.*
            FROM read_parquet({focals}) f
            WHERE f.case_id IN (SELECT case_id FROM read_parquet({sql_literal(str(output_dir / QUALITY_TABLES['accepted_cases']))}))
            ORDER BY f.case_id, f.focal_id
            """,
            output_dir / QUALITY_TABLES["accepted_focals"],
        )
        surviving_cases = sql_literal(str(output_dir / QUALITY_TABLES["accepted_cases"]))
        surviving_focals = sql_literal(str(output_dir / QUALITY_TABLES["accepted_focals"]))
        copy_atomic(
            con,
            f"""
            SELECT fc.*
            FROM read_parquet({comps}) fc
            WHERE (fc.case_id, fc.focal_id) IN (
                SELECT case_id, focal_id FROM read_parquet({surviving_focals})
            )
            ORDER BY fc.case_id, fc.focal_id, fc.competitor_product_id
            """,
            output_dir / QUALITY_TABLES["accepted_comps"],
        )
        copy_atomic(
            con,
            f"""
            SELECT s.*
            FROM read_parquet({shelf}) s
            WHERE s.case_id IN (SELECT case_id FROM read_parquet({surviving_cases}))
            ORDER BY s.case_id, s.product_id
            """,
            output_dir / QUALITY_TABLES["accepted_shelf"],
        )
        copy_atomic(
            con,
            f"""
            SELECT m.*
            FROM read_parquet({focal_metrics}) m
            WHERE (m.case_id, m.focal_id) IN (
                SELECT case_id, focal_id FROM read_parquet({surviving_focals})
            )
            ORDER BY m.case_id, m.focal_id
            """,
            output_dir / QUALITY_TABLES["focal_metrics"],
        )
        copy_atomic(
            con,
            f"""
            SELECT d.*
            FROM read_parquet({case_decisions}) d
            WHERE d.case_id IN (SELECT case_id FROM read_parquet({surviving_cases}))
            ORDER BY d.case_id
            """,
            output_dir / QUALITY_TABLES["case_decisions"],
        )
        copy_atomic(
            con,
            f"""
            SELECT m.*
            FROM read_parquet({case_metrics}) m
            WHERE m.case_id IN (SELECT case_id FROM read_parquet({surviving_cases}))
            ORDER BY m.case_id
            """,
            output_dir / QUALITY_TABLES["case_metrics"],
        )
        copy_atomic(
            con,
            f"""
            SELECT d.*
            FROM read_parquet({focal_decisions}) d
            WHERE (d.case_id, d.focal_id) IN (
                SELECT case_id, focal_id FROM read_parquet({surviving_focals})
            )
            ORDER BY d.case_id, d.focal_id
            """,
            output_dir / QUALITY_TABLES["focal_decisions"],
        )

        counts = {
            "clean_registry_rows": meta["row_count"],
            "accepted_market_count": int(con.execute(
                f"SELECT count(DISTINCT market_id) FROM read_parquet({surviving_cases})"
            ).fetchone()[0]),
            "accepted_case_count": int(con.execute(
                f"SELECT count(*) FROM read_parquet({surviving_cases})"
            ).fetchone()[0]),
            "accepted_focal_count": int(con.execute(
                f"SELECT count(*) FROM read_parquet({surviving_focals})"
            ).fetchone()[0]),
            "accepted_case_shelf_rows": int(con.execute(
                f"SELECT count(*) FROM read_parquet({sql_literal(str(output_dir / QUALITY_TABLES['accepted_shelf']))})"
            ).fetchone()[0]),
            "accepted_focal_competitor_rows": int(con.execute(
                f"SELECT count(*) FROM read_parquet({sql_literal(str(output_dir / QUALITY_TABLES['accepted_comps']))})"
            ).fetchone()[0]),
        }
        source_partition = con.execute(
            "SELECT any_value(source_partition) FROM registry"
        ).fetchone()[0]
        summary = {
            "status": "COMPLETE",
            "source_partition": source_partition,
            "curation_type": "manual_audit",
            "filter": "name_only_conservative_clean_markets",
            "match_key": "market_name",
            "criterion": (
                "label is clear; no obvious generic/umbrella wording; "
                "no obvious synonym/duplicate merge conflict visible from names alone"
            ),
            "registry_path": str(registry),
            "registry_sha256": sha256_file(registry),
            "registry_row_count": meta["row_count"],
            "input_quality_dir": str(quality_dir),
            "source_quality_dir": str(quality_dir),
            "clean_market_count": meta["row_count"],
            "accepted_market_count": counts["accepted_market_count"],
            "accepted_case_count": counts["accepted_case_count"],
            "accepted_focal_count": counts["accepted_focal_count"],
            "accepted_case_shelf_rows": counts["accepted_case_shelf_rows"],
            "accepted_focal_competitor_rows": counts["accepted_focal_competitor_rows"],
        }
        write_json(output_dir / "subset_summary.json", summary)
        return summary
    finally:
        con.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Apply frozen manual clean-market registry to Quality accepted tables"
    )
    p.add_argument("--quality-dir", type=Path, required=True)
    p.add_argument("--registry", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    return p


def main() -> None:
    args = build_parser().parse_args()
    summary = apply_registry(args.quality_dir, args.registry, args.output_dir)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
