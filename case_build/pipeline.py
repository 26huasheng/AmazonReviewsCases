from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any, Sequence

import duckdb

from .case_discovery import (
    attach_active_competitor_counts,
    write_active_product_count_events,
    write_case_candidates,
)
from .case_features import (
    attach_market_pre_t0_review_count,
    write_market_review_cumulative,
)
from .config import (
    DEFAULT_EVALUATION_DAYS,
    DEFAULT_RECENT_ACTIVITY_WINDOW_DAYS,
    MAX_COMPETITORS_PER_FOCAL,
)
from .market_timeline import write_market_product_map_and_timeline
from .product_timeline import (
    validate_product_time_summary,
    write_product_time_summary_from_daily,
)
from .shelf import (
    write_case_shelf,
    write_focal_competitor_candidates,
    write_focal_competitors,
    write_product_rating_cumulative,
)
from .time_windows import (
    TimeBox,
    attach_time_boxes,
    parse_time_boxes,
    time_boxes_identity,
)
from utils import read_json, sql_literal, write_json


class _DuckDBStage:
    con: duckdb.DuckDBPyConnection

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


def _configure_connection(con: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='200GB'")
    temp_dir = output_dir / ".duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory={sql_literal(str(temp_dir))}")
    con.execute("SET max_temp_directory_size='80GiB'")


class CaseDiscoveryPipeline(_DuckDBStage):
    """Final Market → 商品时间轴 → Case 候选。

    这一阶段不做用户筛选、不做 GT，也不使用未来表现阈值或固定 competitor 数阈值。
    """

    def __init__(
        self,
        final_market: Path,
        product_core: Path,
        storage_metadata: Path,
        output_dir: Path,
        *,
        product_time_summary: Path | None = None,
        rating_daily_summary: Path | None = None,
        time_boxes: Sequence[dict[str, Any]] | Sequence[TimeBox] | None = None,
        evaluation_days: int = DEFAULT_EVALUATION_DAYS,
    ) -> None:
        self.final_market = final_market.expanduser().resolve()
        self.product_core = product_core.expanduser().resolve()
        self.storage_metadata = storage_metadata.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.input_product_time = (
            product_time_summary.expanduser().resolve()
            if product_time_summary else None
        )
        self.rating_daily_summary = (
            rating_daily_summary.expanduser().resolve()
            if rating_daily_summary else None
        )
        self.time_boxes = parse_time_boxes(time_boxes)
        if evaluation_days <= 0:
            raise ValueError("evaluation_days must be positive")
        self.evaluation_days = evaluation_days

        for path in (self.final_market, self.product_core, self.storage_metadata):
            if not path.is_file():
                raise FileNotFoundError(path)
        if self.input_product_time is not None and not self.input_product_time.is_file():
            raise FileNotFoundError(self.input_product_time)
        if self.rating_daily_summary is not None and not self.rating_daily_summary.is_file():
            raise FileNotFoundError(self.rating_daily_summary)
        if self.input_product_time is None and self.rating_daily_summary is None:
            raise ValueError(
                "provide --product-time-summary or --rating-daily-summary"
            )

        self.work_dir = self.output_dir / "_work"
        self.product_time_path = (
            self.input_product_time
            if self.input_product_time is not None
            else self.work_dir / "product_time_summary.parquet"
        )
        self.market_product_map_path = self.output_dir / "market_product_map.parquet"
        self.market_timeline_path = self.output_dir / "market_product_timeline.parquet"
        self.interval_events_path = self.work_dir / "active_product_interval_events.parquet"
        self.active_count_path = self.work_dir / "active_product_count_cumulative.parquet"
        self.market_daily_path = self.work_dir / "market_daily_review_count.parquet"
        self.market_cumulative_path = self.work_dir / "market_review_cumulative.parquet"
        self.candidates_without_boxes_path = self.work_dir / "case_candidates_without_boxes.parquet"
        self.candidates_with_boxes_path = self.work_dir / "case_candidates_with_boxes.parquet"
        self.candidates_path = self.output_dir / "case_candidates.parquet"
        self.evaluable_path = self.output_dir / "case_candidates_evaluable.parquet"
        self.summary_path = self.output_dir / "case_discovery_summary.json"

        self.con = duckdb.connect()
        _configure_connection(self.con, self.output_dir)

    def _observation_end(self) -> date:
        metadata = read_json(self.storage_metadata)
        end = (metadata.get("rating_observation") or {}).get("end_date")
        if not end:
            raise ValueError(
                "storage_metadata.json missing rating_observation.end_date"
            )
        return date.fromisoformat(str(end))

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)

        if self.input_product_time is not None:
            validate_product_time_summary(self.con, self.input_product_time)
        else:
            assert self.rating_daily_summary is not None
            write_product_time_summary_from_daily(
                self.con,
                self.rating_daily_summary,
                self.product_time_path,
                self._copy_atomic,
            )

        counts = write_market_product_map_and_timeline(
            self.con,
            self.final_market,
            self.product_time_path,
            self.product_core,
            self.market_product_map_path,
            self.market_timeline_path,
            self._copy_atomic,
        )

        interval_rows = write_active_product_count_events(
            self.con,
            self.market_timeline_path,
            self.interval_events_path,
            self.active_count_path,
            self._copy_atomic,
        )
        attach_active_competitor_counts(
            self.con,
            self.market_timeline_path,
            self.active_count_path,
        )
        write_case_candidates(
            self.con,
            self._observation_end(),
            self.candidates_without_boxes_path,
            self._copy_atomic,
            evaluation_days=self.evaluation_days,
        )

        outside_time_box_count = attach_time_boxes(
            self.con,
            self.candidates_without_boxes_path,
            self.candidates_with_boxes_path,
            self.time_boxes,
            self._copy_atomic,
        )

        if self.rating_daily_summary is not None:
            write_market_review_cumulative(
                self.con,
                self.market_product_map_path,
                self.rating_daily_summary,
                self.market_daily_path,
                self.market_cumulative_path,
                self._copy_atomic,
            )
            attach_market_pre_t0_review_count(
                self.con,
                self.candidates_with_boxes_path,
                self.market_cumulative_path,
                self.candidates_path,
                self._copy_atomic,
            )
            market_pre_t0_status = "COMPUTED"
        else:
            self._copy_atomic(f"""
                SELECT *, NULL::BIGINT AS market_pre_t0_review_count
                FROM read_parquet({sql_literal(str(self.candidates_with_boxes_path))})
            """, self.candidates_path)
            market_pre_t0_status = "UNAVAILABLE_WITHOUT_RATING_DAILY"

        self._copy_atomic(f"""
            SELECT *
            FROM read_parquet({sql_literal(str(self.candidates_path))})
            WHERE valid_t0
              AND evaluation_window_complete
        """, self.evaluable_path)

        candidate_count = int(self.con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(self.candidates_path)]
        ).fetchone()[0])
        evaluable_count = int(self.con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(self.evaluable_path)]
        ).fetchone()[0])
        payload = {
            "status": "COMPLETE",
            "schema_version": "case_discovery_v1",
            "final_market": str(self.final_market),
            "product_time_summary": str(self.product_time_path),
            "product_time_source": (
                "provided" if self.input_product_time is not None
                else "built_from_rating_daily_summary"
            ),
            "market_product_count": counts["market_product_count"],
            "candidate_case_count": candidate_count,
            "evaluable_case_count": evaluable_count,
            "outside_time_box_count": outside_time_box_count,
            "active_interval_event_rows": interval_rows,
            "evaluation_days": self.evaluation_days,
            "entry_date_source": "first_rating_date",
            "market_pre_t0_review_count_status": market_pre_t0_status,
            "time_boxes": time_boxes_identity(self.time_boxes),
            "hard_quality_gates_applied": False,
            "evaluable_is_formal_case": False,
        }
        write_json(self.summary_path, payload)
        return payload


class CaseShelfBuilder(_DuckDBStage):
    """每个 focal 找 competitor（最多16），再 union 成 Case shelf。"""

    def __init__(
        self,
        cases_path: Path,
        case_focals: Path,
        market_timeline: Path,
        rating_daily_summary: Path,
        output_dir: Path,
        *,
        recent_window_days: int = DEFAULT_RECENT_ACTIVITY_WINDOW_DAYS,
        max_competitors_per_focal: int = MAX_COMPETITORS_PER_FOCAL,
    ) -> None:
        self.cases_path = cases_path.expanduser().resolve()
        self.case_focals = case_focals.expanduser().resolve()
        self.market_timeline = market_timeline.expanduser().resolve()
        self.rating_daily_summary = rating_daily_summary.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        if recent_window_days <= 0:
            raise ValueError("recent_window_days must be positive")
        if max_competitors_per_focal <= 0:
            raise ValueError("max_competitors_per_focal must be positive")
        self.recent_window_days = recent_window_days
        self.max_competitors_per_focal = max_competitors_per_focal

        for path in (
            self.cases_path,
            self.case_focals,
            self.market_timeline,
            self.rating_daily_summary,
        ):
            if not path.is_file():
                raise FileNotFoundError(path)

        self.work_dir = self.output_dir / "_work"
        self.cumulative_path = self.work_dir / "product_rating_cumulative.parquet"
        self.candidate_path = self.work_dir / "focal_competitor_candidates.parquet"
        self.focal_competitors_work_path = self.work_dir / "focal_competitors.parquet"
        self.focal_competitors_path = self.output_dir / "focal_competitors.parquet"
        self.shelf_path = self.output_dir / "case_shelf.parquet"
        self.summary_path = self.output_dir / "case_shelf_summary.json"

        self.con = duckdb.connect()
        _configure_connection(self.con, self.output_dir)

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)

        write_product_rating_cumulative(
            self.con,
            self.rating_daily_summary,
            self.cumulative_path,
            self._copy_atomic,
        )
        write_focal_competitor_candidates(
            self.con,
            self.case_focals,
            self.cases_path,
            self.market_timeline,
            self.candidate_path,
            self._copy_atomic,
        )
        write_focal_competitors(
            self.con,
            self.candidate_path,
            self.cumulative_path,
            self.focal_competitors_work_path,
            self._copy_atomic,
            recent_window_days=self.recent_window_days,
            max_competitors=self.max_competitors_per_focal,
        )
        self._copy_atomic(
            f"SELECT * FROM read_parquet({sql_literal(str(self.focal_competitors_work_path))})",
            self.focal_competitors_path,
        )
        write_case_shelf(
            self.con,
            self.cases_path,
            self.case_focals,
            self.focal_competitors_path,
            self.market_timeline,
            self.shelf_path,
            self._copy_atomic,
        )

        input_case_count = int(self.con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(self.cases_path)]
        ).fetchone()[0])
        case_count = int(self.con.execute(
            "SELECT count(DISTINCT case_id) FROM read_parquet(?)",
            [str(self.shelf_path)],
        ).fetchone()[0])
        if case_count != input_case_count:
            raise ValueError(
                f"shelf materialized {case_count} cases, expected {input_case_count}"
            )
        shelf_row_count = int(self.con.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(self.shelf_path)]
        ).fetchone()[0])
        max_shelf = int(self.con.execute("""
            SELECT coalesce(max(n), 0)
            FROM (
                SELECT case_id, count(*) AS n
                FROM read_parquet(?)
                GROUP BY case_id
            )
        """, [str(self.shelf_path)]).fetchone()[0])

        payload = {
            "status": "COMPLETE",
            "schema_version": "case_shelf_v2",
            "case_count": case_count,
            "shelf_row_count": shelf_row_count,
            "max_shelf_products_per_case": max_shelf,
            "recent_activity_window_days": self.recent_window_days,
            "max_competitors_per_focal": self.max_competitors_per_focal,
            "shelf_truncation_applied": False,
            "activity_threshold_applied": False,
            "price_at_t0_status": "NOT_YET_AVAILABLE",
            "focal_competitors": str(self.focal_competitors_path),
        }
        write_json(self.summary_path, payload)
        return payload
