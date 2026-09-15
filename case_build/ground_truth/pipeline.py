from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from case_build.config import MAX_DAYS_SINCE_LAST_EVENT, MIN_HISTORY_PRODUCTS
from utils import sql_literal, write_json
from .future_events import (
    filter_gt1_by_user_quality,
    write_case_future_market_events,
    write_gt1_shelf_events,
    write_review_activity_truth,
)
from .outcome import write_positive_user_outcomes
from .tables import write_choice_truth, write_market_truth, write_population_truth


class GroundTruthPipeline:
    """Electronics v1 production path is GT1 (no --case-users).

    Passing --case-users enables a legacy GT2 branch; it is not packaged in v1.
    """

    def __init__(
        self,
        cases: Path,
        case_focals: Path,
        case_users: Path | None,
        focal_competitors: Path,
        canonical_user_events: Path,
        user_history: Path,
        output_dir: Path,
        *,
        outcome_policy: str = "first_observed_event",
        rating_daily_summary: Path | None = None,
        min_history_products: int = MIN_HISTORY_PRODUCTS,
        max_days_since_last_event: int = MAX_DAYS_SINCE_LAST_EVENT,
    ) -> None:
        self.cases = cases.expanduser().resolve()
        self.case_focals = case_focals.expanduser().resolve()
        self.case_users = case_users.expanduser().resolve() if case_users else None
        self.focal_competitors = focal_competitors.expanduser().resolve()
        self.canonical_user_events = canonical_user_events.expanduser().resolve()
        self.user_history = user_history.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.outcome_policy = outcome_policy
        self.rating_daily_summary = (
            rating_daily_summary.expanduser().resolve()
            if rating_daily_summary else None
        )
        for path in (
            self.cases, self.case_focals, self.focal_competitors,
            self.canonical_user_events, self.user_history,
        ):
            if not path.is_file():
                raise FileNotFoundError(path)
        if self.case_users is not None and not self.case_users.is_file():
            raise FileNotFoundError(self.case_users)
        if self.rating_daily_summary is not None and not self.rating_daily_summary.is_file():
            raise FileNotFoundError(self.rating_daily_summary)
        self.min_history_products = min_history_products
        self.max_days_since_last_event = max_days_since_last_event

        self.work_dir = self.output_dir / "_work"
        self.gt1_events_path = self.work_dir / "gt1_shelf_events.parquet"
        self.gt1_raw_outcomes_path = self.work_dir / "gt1_raw_outcomes.parquet"
        self.gt1_users_path = self.output_dir / "gt1_users.parquet"
        self.future_events_path = self.work_dir / "future_market_events.parquet"
        self.positive_outcomes_path = self.work_dir / "positive_user_outcomes.parquet"
        self.choice_truth_path = self.output_dir / "choice_truth.parquet"
        self.population_truth_path = self.output_dir / "population_truth.parquet"
        self.market_truth_path = self.output_dir / "market_truth.parquet"
        self.review_activity_truth_path = self.output_dir / "review_activity_truth.parquet"
        self.summary_path = self.output_dir / "ground_truth_summary.json"

        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
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

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        write_gt1_shelf_events(
            self.con, self.cases, self.case_focals, self.focal_competitors,
            self.canonical_user_events, self.gt1_events_path, self._copy_atomic,
        )
        write_positive_user_outcomes(
            self.con, self.gt1_events_path, self.gt1_raw_outcomes_path,
            self._copy_atomic, outcome_policy=self.outcome_policy,
        )
        filter_gt1_by_user_quality(
            self.con, self.gt1_raw_outcomes_path, self.user_history,
            self.gt1_users_path, self._copy_atomic,
            min_history_products=self.min_history_products,
            max_days_since_last_event=self.max_days_since_last_event,
        )
        write_choice_truth(
            self.con, self.gt1_users_path, self.choice_truth_path, self._copy_atomic,
        )
        gt2_rows = 0
        if self.case_users is not None:
            write_case_future_market_events(
                self.con, self.cases, self.case_focals, self.case_users,
                self.focal_competitors, self.canonical_user_events,
                self.future_events_path, self._copy_atomic,
            )
            write_positive_user_outcomes(
                self.con, self.future_events_path, self.positive_outcomes_path,
                self._copy_atomic, outcome_policy=self.outcome_policy,
            )
            write_population_truth(
                self.con, self.case_users, self.case_focals,
                self.positive_outcomes_path,
                self.population_truth_path, self._copy_atomic,
            )
            write_market_truth(
                self.con, self.case_focals, self.focal_competitors,
                self.population_truth_path, self.market_truth_path, self._copy_atomic,
            )
            gt2_rows = int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.population_truth_path)]
            ).fetchone()[0])
        review_activity_status = "NOT_PROVIDED"
        if self.rating_daily_summary is not None:
            write_review_activity_truth(
                self.con, self.cases, self.case_focals, self.focal_competitors,
                self.rating_daily_summary, self.review_activity_truth_path,
                self._copy_atomic,
            )
            review_activity_status = "COMPUTED"

        payload = {
            "status": "COMPLETE",
            "schema_version": "ground_truth_v3",
            "outcome_policy": self.outcome_policy,
            "gt1_source": "local_shelf_window_events",
            "gt1_quality_filter": {
                "min_history_products": self.min_history_products,
                "max_days_since_last_event": self.max_days_since_last_event,
            },
            "raw_gt1_event_rows": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.gt1_events_path)]
            ).fetchone()[0]),
            "raw_gt1_user_rows": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.gt1_raw_outcomes_path)]
            ).fetchone()[0]),
            "gt1_rows": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.choice_truth_path)]
            ).fetchone()[0]),
            "gt2_rows": gt2_rows,
            "review_activity_truth_status": review_activity_status,
        }
        write_json(self.summary_path, payload)
        return payload
