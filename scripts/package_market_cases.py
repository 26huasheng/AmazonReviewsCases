#!/usr/bin/env python3
"""Package existing Quality + clean-market results into Market/Case JSON(L) trees.

Read-only on production parquet. Does not recompute discovery/select/shelf/GT1/quality.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb


UNSAFE_FS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def sql_lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def folder_name(label: str) -> str:
    name = UNSAFE_FS.sub("_", (label or "").strip())
    name = name.rstrip(" .")
    if not name:
        raise ValueError("empty market folder name")
    return name


def json_ready(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return value
    if isinstance(value, (date, datetime)):
        if isinstance(value, datetime):
            return value.replace(microsecond=0).isoformat()
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [json_ready(x) for x in value]
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if hasattr(value, "as_py"):
        return json_ready(value.as_py())
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def copy_jsonl(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    part.unlink(missing_ok=True)
    con.execute(f"COPY ({query}) TO {sql_lit(str(part))} (FORMAT JSON)")
    os.replace(part, path)
    n = 0
    with path.open("rb") as handle:
        for _ in handle:
            n += 1
    return n


def cast_json_select(con: duckdb.DuckDBPyConnection, from_sql: str) -> str:
    cols = con.execute(f"DESCRIBE SELECT * FROM ({from_sql}) _pkg_src").fetchall()
    pieces = []
    for name, typ, *_ in cols:
        t = str(typ).upper()
        ident = f'"{name}"'
        if t.startswith("DATE") and "TIME" not in t:
            expr = f"strftime({ident}, '%Y-%m-%d')"
        elif "TIMESTAMP" in t:
            expr = f"strftime({ident}, '%Y-%m-%dT%H:%M:%S')"
        elif t.startswith("FLOAT") or t.startswith("DOUBLE") or t.startswith("DECIMAL"):
            expr = (
                f"CASE WHEN {ident} IS NULL THEN NULL "
                f"WHEN typeof({ident}) IN ('FLOAT','DOUBLE') AND (isnan({ident}) OR isinf({ident})) "
                f"THEN NULL ELSE {ident} END"
            )
        else:
            expr = ident
        pieces.append(f"{expr} AS {ident}")
    return "SELECT " + ", ".join(pieces) + f" FROM ({from_sql}) _pkg_src"


class MarketCasePackager:
    def __init__(
        self,
        electronics_root: Path,
        clean_quality_dir: Path,
        output_dir: Path,
        overwrite: bool,
    ) -> None:
        self.root = electronics_root.expanduser().resolve()
        self.clean_dir = clean_quality_dir.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.overwrite = overwrite
        self.reviews_jsonl = None
        self.staging = self.output_dir / "_staging"

        self.paths = {
            "clean_markets": self.clean_dir / "clean_markets.parquet",
            "accepted_cases": self.clean_dir / "accepted_cases.parquet",
            "accepted_focals": self.clean_dir / "accepted_focals.parquet",
            "accepted_comps": self.clean_dir / "accepted_focal_competitors.parquet",
            "accepted_shelf": self.clean_dir / "accepted_case_shelf.parquet",
            "focal_metrics": self.clean_dir / "quality_focal_metrics.parquet",
            "gt1_users": self.root / "case_build" / "ground_truth_gt1" / "gt1_users.parquet",
            "choice_truth": self.root / "case_build" / "ground_truth_gt1" / "choice_truth.parquet",
            "market_products": self.root / "market_build" / "market_products.parquet",
            "canonical_events": self.root / "market_build" / "canonical_user_events.parquet",
            "hist_cum": self.root / "market_build" / "user_history_cumulative.parquet",
            "cat_cum": self.root / "market_build" / "user_category_history_cumulative.parquet",
            "mkt_cum": self.root / "market_build" / "user_market_history_cumulative.parquet",
        }
        missing = [str(p) for p in self.paths.values() if not p.is_file()]
        if missing:
            raise FileNotFoundError("missing inputs:\n" + "\n".join(missing))

        self.con = duckdb.connect()
        self.con.execute("SET preserve_insertion_order=false")
        self.con.execute("SET memory_limit='200GB'")

    def close(self) -> None:
        self.con.close()

    def _prepare_output(self) -> None:
        if self.output_dir.exists():
            nonempty = any(self.output_dir.iterdir())
            if nonempty and not self.overwrite:
                raise SystemExit(
                    f"refusing to write into existing directory {self.output_dir}; "
                    "pass --overwrite"
                )
            if nonempty and self.overwrite:
                for child in self.output_dir.iterdir():
                    if child.is_dir():
                        shutil.rmtree(child)
                    else:
                        child.unlink()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.staging.mkdir(parents=True, exist_ok=True)
        temp = self.output_dir / ".duckdb_tmp"
        temp.mkdir(exist_ok=True)
        self.con.execute(f"SET temp_directory={sql_lit(str(temp))}")
        self.con.execute("SET max_temp_directory_size='80GiB'")

    def _stage(self) -> dict[str, Any]:
        p = {k: sql_lit(str(v)) for k, v in self.paths.items()}
        conflicts = self.con.execute(f"""
            SELECT market_label, list(market_id ORDER BY market_id) AS market_ids, count(*) n
            FROM read_parquet({p['clean_markets']})
            GROUP BY market_label
            HAVING count(*) > 1
            ORDER BY market_label
        """).fetchall()
        if conflicts:
            detail = [
                {"market_name": row[0], "market_ids": list(row[1]), "n": int(row[2])}
                for row in conflicts
            ]
            raise SystemExit("market name conflict, refusing to merge:\n" + json.dumps(detail, indent=2))

        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_markets AS
            SELECT
                m.market_id,
                m.market_label AS market_name,
                prod.source_partition,
                count(prod.product_id)::BIGINT AS n_products
            FROM read_parquet({p['clean_markets']}) m
            JOIN (
                SELECT DISTINCT market_id
                FROM read_parquet({p['accepted_cases']})
            ) acc ON acc.market_id = m.market_id
            LEFT JOIN read_parquet({p['market_products']}) prod
              ON prod.market_id = m.market_id
            GROUP BY m.market_id, m.market_label, prod.source_partition
        """)
        folder_rows = self.con.execute(
            "SELECT market_id, market_name FROM pkg_markets ORDER BY market_name, market_id"
        ).fetchall()
        folders: dict[str, str] = {}
        used: dict[str, str] = {}
        for market_id, name in folder_rows:
            folder = folder_name(str(name))
            if folder in used and used[folder] != market_id:
                raise SystemExit(
                    f"folder name conflict: {folder!r} for {used[folder]} and {market_id}"
                )
            used[folder] = market_id
            folders[market_id] = folder
        self.con.execute("CREATE OR REPLACE TABLE pkg_folders (market_id VARCHAR, market_folder_name VARCHAR)")
        self.con.executemany("INSERT INTO pkg_folders VALUES (?, ?)", list(folders.items()))

        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_cases AS
            SELECT c.*
            FROM read_parquet({p['accepted_cases']}) c
            JOIN pkg_markets m ON c.market_id = m.market_id
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_focals AS
            SELECT
                f.*,
                met.selected_competitor_count,
                met.gt1_user_count
            FROM read_parquet({p['accepted_focals']}) f
            JOIN pkg_cases c ON f.case_id = c.case_id
            LEFT JOIN read_parquet({p['focal_metrics']}) met
              ON f.case_id = met.case_id AND f.focal_id = met.focal_id
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_comps AS
            SELECT fc.*
            FROM read_parquet({p['accepted_comps']}) fc
            JOIN pkg_focals f ON fc.case_id = f.case_id AND fc.focal_id = f.focal_id
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_shelf AS
            SELECT s.*
            FROM read_parquet({p['accepted_shelf']}) s
            JOIN pkg_cases c ON s.case_id = c.case_id
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_gt1_users AS
            SELECT g.*
            FROM read_parquet({p['gt1_users']}) g
            JOIN pkg_focals f ON g.case_id = f.case_id AND g.focal_id = f.focal_id
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_choice AS
            SELECT g.*
            FROM read_parquet({p['choice_truth']}) g
            JOIN pkg_focals f ON g.case_id = f.case_id AND g.focal_id = f.focal_id
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_products AS
            SELECT p.*
            FROM read_parquet({p['market_products']}) p
            JOIN pkg_markets m ON p.market_id = m.market_id
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_market_users AS
            SELECT DISTINCT c.market_id, g.user_id
            FROM pkg_gt1_users g
            JOIN pkg_cases c ON g.case_id = c.case_id
        """)

        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_hist_summary AS
            SELECT
                g.case_id,
                g.focal_id,
                g.user_id,
                g.t0,
                h.cumulative_event_count AS history_event_count,
                g.history_product_count,
                CASE WHEN g.days_since_last_event IS NULL THEN NULL
                     ELSE g.t0 - CAST(g.days_since_last_event AS INTEGER)
                END AS last_event_date,
                g.days_since_last_event,
                cat.cumulative_event_count AS category_history_event_count,
                cat.cumulative_product_count AS category_history_product_count,
                mkt.cumulative_event_count AS market_history_event_count,
                mkt.cumulative_product_count AS market_history_product_count
            FROM pkg_gt1_users g
            JOIN pkg_cases c ON g.case_id = c.case_id
            ASOF LEFT JOIN read_parquet({p['hist_cum']}) h
              ON g.user_id = h.user_id AND h.event_date < g.t0
            ASOF LEFT JOIN read_parquet({p['cat_cum']}) cat
              ON g.user_id = cat.user_id
             AND cat.source_partition = g.source_partition
             AND cat.event_date < g.t0
            ASOF LEFT JOIN read_parquet({p['mkt_cum']}) mkt
              ON g.user_id = mkt.user_id
             AND mkt.market_id = g.market_id
             AND mkt.event_date < g.t0
        """)

        self.con.execute(f"""
            CREATE OR REPLACE TABLE pkg_hist_events AS
            SELECT
                g.case_id,
                g.focal_id,
                e.user_id,
                e.event_date,
                e.event_timestamp,
                e.product_id,
                e.rating,
                e.verified_purchase,
                e.source_partition
            FROM pkg_gt1_users g
            JOIN read_parquet({p['canonical_events']}) e
              ON e.user_id = g.user_id
             AND e.event_timestamp < CAST(g.t0 AS TIMESTAMP)
        """)

        counts = {
            "markets": int(self.con.execute("SELECT count(*) FROM pkg_markets").fetchone()[0]),
            "cases": int(self.con.execute("SELECT count(*) FROM pkg_cases").fetchone()[0]),
            "focals": int(self.con.execute("SELECT count(*) FROM pkg_focals").fetchone()[0]),
            "gt1_users_rows": int(self.con.execute("SELECT count(*) FROM pkg_gt1_users").fetchone()[0]),
            "choice_rows": int(self.con.execute("SELECT count(*) FROM pkg_choice").fetchone()[0]),
            "market_users": int(self.con.execute("SELECT count(*) FROM pkg_market_users").fetchone()[0]),
            "products": int(self.con.execute("SELECT count(*) FROM pkg_products").fetchone()[0]),
            "hist_summary_rows": int(self.con.execute("SELECT count(*) FROM pkg_hist_summary").fetchone()[0]),
            "hist_event_rows": int(self.con.execute("SELECT count(*) FROM pkg_hist_events").fetchone()[0]),
        }
        return {"folders": folders, "counts": counts}

    def _validate_in_memory(self) -> dict[str, Any]:
        issues: dict[str, Any] = {}
        issues["choice_outside_local_shelf"] = int(self.con.execute("""
            SELECT count(*)
            FROM pkg_choice ch
            JOIN pkg_focals f ON ch.case_id = f.case_id AND ch.focal_id = f.focal_id
            LEFT JOIN (
                SELECT case_id, focal_id, focal_product_id AS product_id FROM pkg_focals
                UNION
                SELECT case_id, focal_id, competitor_product_id FROM pkg_comps
            ) loc
              ON loc.case_id = ch.case_id
             AND loc.focal_id = ch.focal_id
             AND loc.product_id = ch.product_id
            WHERE loc.product_id IS NULL
        """).fetchone()[0])
        issues["history_event_on_or_after_t0"] = int(self.con.execute("""
            SELECT count(*)
            FROM pkg_hist_events e
            JOIN pkg_focals f ON e.case_id = f.case_id AND e.focal_id = f.focal_id
            WHERE e.event_timestamp >= CAST(f.t0 AS TIMESTAMP)
        """).fetchone()[0])
        issues["competitor_missing_from_shelf"] = int(self.con.execute("""
            SELECT count(*)
            FROM pkg_comps fc
            LEFT JOIN pkg_shelf s
              ON s.case_id = fc.case_id AND s.product_id = fc.competitor_product_id
            WHERE s.product_id IS NULL
        """).fetchone()[0])
        issues["focal_missing_from_shelf"] = int(self.con.execute("""
            SELECT count(*)
            FROM pkg_focals f
            LEFT JOIN pkg_shelf s
              ON s.case_id = f.case_id AND s.product_id = f.focal_product_id
            WHERE s.product_id IS NULL
        """).fetchone()[0])
        issues["gt1_user_missing_from_market_users"] = int(self.con.execute("""
            SELECT count(*)
            FROM pkg_gt1_users g
            JOIN pkg_cases c ON g.case_id = c.case_id
            LEFT JOIN pkg_market_users u
              ON u.market_id = c.market_id AND u.user_id = g.user_id
            WHERE u.user_id IS NULL
        """).fetchone()[0])
        return issues

    def _write_markets(self, folders: dict[str, str]) -> dict[str, int]:
        written = {
            "users_jsonl_rows": 0,
            "summary_jsonl_rows": 0,
            "events_jsonl_rows": 0,
            "products_jsonl_rows": 0,
            "gt1_users_jsonl_rows": 0,
            "choice_jsonl_rows": 0,
            "empty_time_boxes": 0,
        }
        markets = self.con.execute("""
            SELECT
                m.market_id,
                m.market_name,
                coalesce(m.source_partition, any_value(c.source_partition)) AS source_partition,
                m.n_products,
                f.market_folder_name
            FROM pkg_markets m
            JOIN pkg_folders f ON m.market_id = f.market_id
            LEFT JOIN pkg_cases c ON c.market_id = m.market_id
            GROUP BY m.market_id, m.market_name, m.source_partition, m.n_products, f.market_folder_name
            ORDER BY m.market_name, m.market_id
        """).fetchall()

        for market_id, market_name, source_partition, n_products, folder in markets:
            mdir = self.output_dir / folder
            cases = self.con.execute("""
                SELECT case_id, time_box_id, time_box_start, time_box_end,
                       population_cutoff, population_cutoff_policy,
                       accepted_focal_count, source_partition, market_id, market_label
                FROM pkg_cases
                WHERE market_id = ?
                ORDER BY time_box_id, case_id
            """, [market_id]).fetchall()
            focals_by_case: dict[str, list[str]] = {}
            time_boxes = sorted({row[1] for row in cases})
            n_focals = int(self.con.execute(
                "SELECT count(*) FROM pkg_focals f JOIN pkg_cases c ON f.case_id=c.case_id WHERE c.market_id=?",
                [market_id],
            ).fetchone()[0])
            write_json(mdir / "market.json", {
                "market_id": market_id,
                "market_name": market_name,
                "market_folder_name": folder,
                "source_partition": source_partition,
                "n_products": int(n_products or 0),
                "n_cases": len(cases),
                "n_focals": n_focals,
                "available_time_boxes": time_boxes,
            })
            written["products_jsonl_rows"] += copy_jsonl(
                self.con,
                cast_json_select(
                    self.con,
                    f"SELECT * FROM pkg_products WHERE market_id = {sql_lit(market_id)}",
                ),
                mdir / "products" / "products.jsonl",
            )
            written["users_jsonl_rows"] += copy_jsonl(
                self.con,
                f"SELECT user_id FROM pkg_market_users WHERE market_id = {sql_lit(market_id)} ORDER BY user_id",
                mdir / "users" / "users.jsonl",
            )
            written["summary_jsonl_rows"] += copy_jsonl(
                self.con,
                cast_json_select(
                    self.con,
                    f"""SELECT * FROM pkg_hist_summary
                    WHERE case_id IN (SELECT case_id FROM pkg_cases WHERE market_id = {sql_lit(market_id)})""",
                ),
                mdir / "users" / "histories" / "summary.jsonl",
            )
            written["events_jsonl_rows"] += copy_jsonl(
                self.con,
                cast_json_select(
                    self.con,
                    f"""SELECT * FROM pkg_hist_events
                    WHERE case_id IN (SELECT case_id FROM pkg_cases WHERE market_id = {sql_lit(market_id)})""",
                ),
                mdir / "users" / "histories" / "events.jsonl",
            )

            for case in cases:
                (
                    case_id, time_box_id, time_box_start, time_box_end,
                    population_cutoff, population_cutoff_policy,
                    accepted_focal_count, case_partition, case_market_id, case_label,
                ) = case
                focal_ids = [
                    r[0] for r in self.con.execute(
                        "SELECT focal_id FROM pkg_focals WHERE case_id=? ORDER BY t0, focal_id",
                        [case_id],
                    ).fetchall()
                ]
                focals_by_case[case_id] = focal_ids
                cdir = mdir / "cases" / str(time_box_id) / str(case_id)
                write_json(cdir / "case.json", {
                    "case_id": case_id,
                    "market_id": case_market_id,
                    "market_name": case_label,
                    "time_box_id": time_box_id,
                    "time_box_start": json_ready(time_box_start),
                    "time_box_end": json_ready(time_box_end),
                    "population_cutoff": json_ready(population_cutoff),
                    "population_cutoff_policy": population_cutoff_policy,
                    "n_focals": len(focal_ids),
                    "focal_ids": focal_ids,
                })
                copy_jsonl(
                    self.con,
                    cast_json_select(
                        self.con, f"SELECT * FROM pkg_focals WHERE case_id = {sql_lit(case_id)}"
                    ),
                    cdir / "focals.jsonl",
                )
                copy_jsonl(
                    self.con,
                    cast_json_select(
                        self.con, f"SELECT * FROM pkg_shelf WHERE case_id = {sql_lit(case_id)}"
                    ),
                    cdir / "shelf.jsonl",
                )
                copy_jsonl(
                    self.con,
                    cast_json_select(
                        self.con, f"SELECT * FROM pkg_comps WHERE case_id = {sql_lit(case_id)}"
                    ),
                    cdir / "focal_competitors.jsonl",
                )
                written["gt1_users_jsonl_rows"] += copy_jsonl(
                    self.con,
                    cast_json_select(
                        self.con, f"SELECT * FROM pkg_gt1_users WHERE case_id = {sql_lit(case_id)}"
                    ),
                    cdir / "ground_truth" / "gt1_users.jsonl",
                )
                written["choice_jsonl_rows"] += copy_jsonl(
                    self.con,
                    cast_json_select(
                        self.con, f"SELECT * FROM pkg_choice WHERE case_id = {sql_lit(case_id)}"
                    ),
                    cdir / "ground_truth" / "choice_truth.jsonl",
                )

            empty_boxes = [
                p.name for p in (mdir / "cases").glob("*")
                if p.is_dir() and not any(p.iterdir())
            ] if (mdir / "cases").exists() else []
            written["empty_time_boxes"] += len(empty_boxes)

        return written

    def run(self) -> dict[str, Any]:
        self._prepare_output()
        staged = self._stage()
        issues = self._validate_in_memory()
        written = self._write_markets(staged["folders"])
        payload = {
            "status": "COMPLETE",
            "output_dir": str(self.output_dir),
            "source_clean_quality_dir": str(self.clean_dir),
            "counts": staged["counts"],
            "written": written,
            "integrity_issues": issues,
        }
        write_json(self.output_dir / "package_summary.json", payload)
        if getattr(self, "reviews_jsonl", None) is not None:
            from scripts.attach_history_review_text import attach_review_text

            payload["review_text"] = attach_review_text(self.output_dir, self.reviews_jsonl)
            write_json(self.output_dir / "package_summary.json", payload)
        return payload


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Package accepted Market/Case results as JSON/JSONL")
    p.add_argument("--electronics-root", type=Path, required=True)
    p.add_argument("--clean-quality-dir", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument(
        "--reviews-jsonl",
        type=Path,
        help="Amazon Reviews 2023 category jsonl; review text is attached to histories/events.jsonl only",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    worker = MarketCasePackager(
        args.electronics_root,
        args.clean_quality_dir,
        args.output_dir,
        args.overwrite,
    )
    worker.reviews_jsonl = args.reviews_jsonl
    try:
        result = worker.run()
    finally:
        worker.close()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
