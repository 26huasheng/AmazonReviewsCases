from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from utils import sql_literal, write_json
from .metrics import write_focal_quality_metrics
from .rules import (
    load_quality_rules,
    write_case_quality_decisions,
    write_focal_quality_decisions,
)


class CaseQualityPipeline:
    """Focal-level Quality, then rebuild Case content from accepted focals."""

    def __init__(
        self,
        cases: Path,
        case_shelf: Path,
        choice_truth: Path,
        output_dir: Path,
        *,
        case_focals: Path,
        focal_competitors: Path,
        rules_json: Path | None = None,
        gt1_users: Path | None = None,
        gt1_raw_outcomes: Path | None = None,
        gt1_shelf_events: Path | None = None,
        timeline: Path | None = None,
        review_activity_truth: Path | None = None,
        case_users: Path | None = None,
        population_truth: Path | None = None,
        market_truth: Path | None = None,
        external_signals: Path | None = None,
    ) -> None:
        self.cases = cases.expanduser().resolve()
        self.case_shelf = case_shelf.expanduser().resolve()
        self.choice_truth = choice_truth.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.case_focals = case_focals.expanduser().resolve()
        self.focal_competitors = focal_competitors.expanduser().resolve()
        self.rules_json = rules_json.expanduser().resolve() if rules_json else None
        self.gt1_users = gt1_users.expanduser().resolve() if gt1_users else None
        self.gt1_raw_outcomes = (
            gt1_raw_outcomes.expanduser().resolve() if gt1_raw_outcomes else None
        )
        self.gt1_shelf_events = (
            gt1_shelf_events.expanduser().resolve() if gt1_shelf_events else None
        )
        self.timeline = timeline.expanduser().resolve() if timeline else None
        self.review_activity_truth = (
            review_activity_truth.expanduser().resolve()
            if review_activity_truth else None
        )
        for path in (
            self.cases, self.case_shelf, self.choice_truth,
            self.case_focals, self.focal_competitors,
        ):
            if not path.is_file():
                raise FileNotFoundError(path)
        for path in (
            self.rules_json, self.gt1_users, self.gt1_raw_outcomes,
            self.gt1_shelf_events, self.timeline, self.review_activity_truth,
        ):
            if path is not None and not path.is_file():
                raise FileNotFoundError(path)

        self.work_dir = self.output_dir / "_work"
        self.focal_metrics_path = self.output_dir / "quality_focal_metrics.parquet"
        self.focal_decisions_path = self.output_dir / "quality_focal_decisions.parquet"
        self.case_metrics_path = self.output_dir / "quality_case_metrics.parquet"
        self.case_decisions_path = self.output_dir / "quality_case_decisions.parquet"
        self.metrics_path = self.output_dir / "quality_metrics.parquet"
        self.decisions_path = self.output_dir / "quality_decisions.parquet"
        self.accepted_focals_path = self.output_dir / "accepted_focals.parquet"
        self.rejected_focals_path = self.output_dir / "rejected_focals.parquet"
        self.accepted_cases_path = self.output_dir / "accepted_cases.parquet"
        self.rejected_cases_path = self.output_dir / "rejected_cases.parquet"
        self.accepted_focal_competitors_path = (
            self.output_dir / "accepted_focal_competitors.parquet"
        )
        self.accepted_case_shelf_path = self.output_dir / "accepted_case_shelf.parquet"
        self.summary_path = self.output_dir / "quality_summary.json"

        self.con = duckdb.connect()
        self.con.execute("SET preserve_insertion_order=false")
        self.con.execute("SET memory_limit='200GB'")
        temp = self.output_dir / ".duckdb_tmp"
        temp.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"SET temp_directory={sql_literal(str(temp))}")
        self.con.execute("SET max_temp_directory_size='80GiB'")

    def close(self) -> None:
        self.con.close()

    def _copy_atomic(self, query: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        part = path.with_suffix(path.suffix + ".part")
        part.unlink(missing_ok=True)
        self.con.execute(
            f"COPY ({query}) TO {sql_literal(str(part))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        os.replace(part, path)

    def _rebuild_benchmark_tables(self) -> None:
        fd = sql_literal(str(self.focal_decisions_path))
        focals = sql_literal(str(self.case_focals))
        cases = sql_literal(str(self.cases))
        comps = sql_literal(str(self.focal_competitors))
        shelf = sql_literal(str(self.case_shelf))
        cm = sql_literal(str(self.case_metrics_path))
        cd = sql_literal(str(self.case_decisions_path))

        self._copy_atomic(f"""
            SELECT f.*,
                   d.focal_quality_pass,
                   d.focal_quality_status,
                   d.focal_quality_rejection_reasons
            FROM read_parquet({focals}) f
            JOIN read_parquet({fd}) d
              ON f.case_id = d.case_id AND f.focal_id = d.focal_id
            WHERE d.focal_quality_pass
            ORDER BY f.case_id, f.focal_id
        """, self.accepted_focals_path)
        self._copy_atomic(f"""
            SELECT f.*,
                   d.focal_quality_pass,
                   d.focal_quality_status,
                   d.focal_quality_rejection_reasons
            FROM read_parquet({focals}) f
            JOIN read_parquet({fd}) d
              ON f.case_id = d.case_id AND f.focal_id = d.focal_id
            WHERE NOT d.focal_quality_pass
            ORDER BY f.case_id, f.focal_id
        """, self.rejected_focals_path)
        self._copy_atomic(f"""
            SELECT c.*,
                   m.original_focal_count,
                   m.accepted_focal_count,
                   m.rejected_focal_count,
                   m.final_shelf_size,
                   d.case_quality_pass,
                   d.case_quality_status,
                   d.case_quality_rejection_reasons
            FROM read_parquet({cases}) c
            JOIN read_parquet({cm}) m ON c.case_id = m.case_id
            JOIN read_parquet({cd}) d ON c.case_id = d.case_id
            WHERE d.case_quality_pass
            ORDER BY c.case_id
        """, self.accepted_cases_path)
        self._copy_atomic(f"""
            SELECT c.*,
                   m.original_focal_count,
                   m.accepted_focal_count,
                   m.rejected_focal_count,
                   m.final_shelf_size,
                   d.case_quality_pass,
                   d.case_quality_status,
                   d.case_quality_rejection_reasons
            FROM read_parquet({cases}) c
            JOIN read_parquet({cm}) m ON c.case_id = m.case_id
            JOIN read_parquet({cd}) d ON c.case_id = d.case_id
            WHERE NOT d.case_quality_pass
            ORDER BY c.case_id
        """, self.rejected_cases_path)
        self._copy_atomic(f"""
            SELECT fc.*
            FROM read_parquet({comps}) fc
            JOIN read_parquet({fd}) d
              ON fc.case_id = d.case_id AND fc.focal_id = d.focal_id
            WHERE d.focal_quality_pass
              AND fc.competitor_selected
            ORDER BY fc.case_id, fc.focal_id, fc.competitor_selection_rank
        """, self.accepted_focal_competitors_path)
        self._copy_atomic(f"""
            WITH accepted AS (
                SELECT f.case_id, f.focal_id, f.focal_product_id
                FROM read_parquet({focals}) f
                JOIN read_parquet({fd}) d
                  ON f.case_id = d.case_id AND f.focal_id = d.focal_id
                WHERE d.focal_quality_pass
            ),
            products AS (
                SELECT case_id, focal_product_id AS product_id,
                       TRUE AS from_focal, FALSE AS from_competitor
                FROM accepted
                UNION ALL
                SELECT fc.case_id, fc.competitor_product_id,
                       FALSE AS from_focal, TRUE AS from_competitor
                FROM read_parquet({comps}) fc
                JOIN accepted a
                  ON fc.case_id = a.case_id AND fc.focal_id = a.focal_id
                WHERE fc.competitor_selected
            ),
            unioned AS (
                SELECT case_id, product_id,
                       bool_or(from_focal) AS is_focal,
                       bool_or(from_competitor) AS is_competitor
                FROM products
                GROUP BY case_id, product_id
            )
            SELECT
                u.case_id,
                s.source_partition,
                s.market_id,
                u.product_id,
                s.product_title,
                u.is_focal,
                u.is_competitor,
                s.first_review_date,
                s.last_review_date,
                s.metadata_snapshot_price
            FROM unioned u
            LEFT JOIN read_parquet({shelf}) s
              ON s.case_id = u.case_id AND s.product_id = u.product_id
            ORDER BY u.case_id, u.is_focal DESC, u.product_id
        """, self.accepted_case_shelf_path)

    def _write_case_metrics(self) -> None:
        fd = sql_literal(str(self.focal_decisions_path))
        focals = sql_literal(str(self.case_focals))
        cases = sql_literal(str(self.cases))
        shelf = sql_literal(str(self.accepted_case_shelf_path))
        self._copy_atomic(f"""
            WITH focal_counts AS (
                SELECT
                    f.case_id,
                    count(*)::BIGINT AS original_focal_count,
                    count(*) FILTER (d.focal_quality_pass)::BIGINT AS accepted_focal_count,
                    count(*) FILTER (NOT d.focal_quality_pass)::BIGINT AS rejected_focal_count
                FROM read_parquet({focals}) f
                JOIN read_parquet({fd}) d
                  ON f.case_id = d.case_id AND f.focal_id = d.focal_id
                GROUP BY f.case_id
            ),
            shelf_size AS (
                SELECT case_id, count(*)::BIGINT AS final_shelf_size
                FROM read_parquet({shelf})
                GROUP BY case_id
            )
            SELECT
                c.case_id,
                c.market_id,
                c.time_box_id,
                coalesce(fc.original_focal_count, 0)::BIGINT AS original_focal_count,
                coalesce(fc.accepted_focal_count, 0)::BIGINT AS accepted_focal_count,
                coalesce(fc.rejected_focal_count, 0)::BIGINT AS rejected_focal_count,
                coalesce(ss.final_shelf_size, 0)::BIGINT AS final_shelf_size
            FROM read_parquet({cases}) c
            LEFT JOIN focal_counts fc ON c.case_id = fc.case_id
            LEFT JOIN shelf_size ss ON c.case_id = ss.case_id
            ORDER BY c.case_id
        """, self.case_metrics_path)

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        rules = load_quality_rules(self.rules_json)
        write_focal_quality_metrics(
            self.con,
            self.cases,
            self.case_focals,
            self.case_shelf,
            self.focal_competitors,
            self.choice_truth,
            self.focal_metrics_path,
            self._copy_atomic,
            rules=rules,
            gt1_users=self.gt1_users,
            gt1_raw_outcomes=self.gt1_raw_outcomes,
            gt1_shelf_events=self.gt1_shelf_events,
            timeline=self.timeline,
            review_activity_truth=self.review_activity_truth,
        )
        write_focal_quality_decisions(
            self.con,
            self.focal_metrics_path,
            self.focal_decisions_path,
            self._copy_atomic,
            rules=rules,
        )
        # Rebuild shelf from accepted focals first so case metrics see final size.
        # Case metrics need decisions; write a temporary accepted shelf after
        # focal decisions, then case metrics/decisions, then full rebuild.
        self._copy_atomic(f"""
            WITH accepted AS (
                SELECT f.case_id, f.focal_id, f.focal_product_id
                FROM read_parquet({sql_literal(str(self.case_focals))}) f
                JOIN read_parquet({sql_literal(str(self.focal_decisions_path))}) d
                  ON f.case_id = d.case_id AND f.focal_id = d.focal_id
                WHERE d.focal_quality_pass
            ),
            products AS (
                SELECT case_id, focal_product_id AS product_id FROM accepted
                UNION
                SELECT fc.case_id, fc.competitor_product_id
                FROM read_parquet({sql_literal(str(self.focal_competitors))}) fc
                JOIN accepted a
                  ON fc.case_id = a.case_id AND fc.focal_id = a.focal_id
                WHERE fc.competitor_selected
            )
            SELECT case_id, product_id FROM products
        """, self.accepted_case_shelf_path)
        self._write_case_metrics()
        write_case_quality_decisions(
            self.con,
            self.case_metrics_path,
            self.case_decisions_path,
            self._copy_atomic,
        )
        self._rebuild_benchmark_tables()
        self._copy_atomic(
            f"SELECT * FROM read_parquet({sql_literal(str(self.case_metrics_path))})",
            self.metrics_path,
        )
        self._copy_atomic(
            f"SELECT * FROM read_parquet({sql_literal(str(self.case_decisions_path))})",
            self.decisions_path,
        )

        def _count(path: Path) -> int:
            return int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()[0])

        n_focal = _count(self.focal_decisions_path)
        n_focal_acc = int(self.con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE focal_quality_pass",
            [str(self.focal_decisions_path)],
        ).fetchone()[0])
        reason_counts = self.con.execute(f"""
            SELECT reason, count(*)::BIGINT AS n
            FROM (
                SELECT unnest(focal_quality_rejection_reasons) AS reason
                FROM read_parquet({sql_literal(str(self.focal_decisions_path))})
            )
            GROUP BY reason
            ORDER BY n DESC, reason
        """).fetchall()
        below6 = int(self.con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE selected_competitor_count < ?",
            [str(self.focal_metrics_path), int(rules["min_competitors"])],
        ).fetchone()[0])
        below_gt1 = int(self.con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE gt1_user_count < ?",
            [str(self.focal_metrics_path), int(rules["min_gt1_users"])],
        ).fetchone()[0])
        struct_fail = int(self.con.execute(f"""
            SELECT count(*) FROM read_parquet({sql_literal(str(self.focal_metrics_path))})
            WHERE NOT (
                coalesce(evaluation_window_complete, false)
                AND coalesce(focal_in_case_shelf, false)
                AND coalesce(competitor_relation_complete, false)
                AND coalesce(competitor_count_consistent, false)
                AND coalesce(competitor_time_eligibility_complete, false)
                AND coalesce(local_shelf_consistent, false)
                AND coalesce(gt1_unique_user_complete, false)
                AND coalesce(gt1_choice_membership_complete, false)
                AND coalesce(gt1_window_complete, false)
                AND coalesce(gt1_history_quality_complete, false)
                AND coalesce(gt1_reducer_unique, false)
                AND coalesce(gt1_traceable_to_window_event, false)
            )
        """).fetchone()[0])

        payload = {
            "status": "COMPLETE",
            "schema_version": "case_quality_v2_focal",
            "rules": rules,
            "candidate_case_count": _count(self.case_decisions_path),
            "accepted_case_count": _count(self.accepted_cases_path),
            "rejected_case_count": _count(self.rejected_cases_path),
            "candidate_focal_count": n_focal,
            "accepted_focal_count": n_focal_acc,
            "rejected_focal_count": n_focal - n_focal_acc,
            "accepted_case_shelf_rows": _count(self.accepted_case_shelf_path),
            "accepted_focal_competitor_rows": _count(self.accepted_focal_competitors_path),
            "focals_competitors_below_min": below6,
            "focals_gt1_users_below_min": below_gt1,
            "focals_structure_failed": struct_fail,
            "focal_rejection_reason_counts": {
                str(r): int(n) for r, n in reason_counts
            },
        }
        write_json(self.summary_path, payload)
        return payload
