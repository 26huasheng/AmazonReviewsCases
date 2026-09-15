from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from utils import sql_literal, write_json

from .audit import write_graph_market_overlap, write_market_graph_summary
from .components import write_full_graph_components
from .config import BehaviorGraphRules
from .cumulative import write_pair_cumulative, write_product_user_cumulative
from .full_graph import write_full_graph_edges
from .pair_events import write_pair_full_counts, write_pair_user_events
from .selection import write_case_focal_coreview_features, write_selected_case_shelf
from .user_product import write_product_user_totals, write_user_product_first


class _DuckDBStage:
    def _configure(self, output_dir: Path) -> None:
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        self.con.execute("SET preserve_insertion_order=false")
        temp = output_dir / ".duckdb_tmp"
        temp.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"SET temp_directory={sql_literal(str(temp))}")

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


class BehaviorGraphBuildPipeline(_DuckDBStage):
    """构造 Market 内共评累计索引，并保留完整时期 audit graph。"""

    def __init__(
        self,
        canonical_user_events: Path,
        market_products: Path,
        output_dir: Path,
        *,
        rules: BehaviorGraphRules | None = None,
    ) -> None:
        self.canonical_user_events = canonical_user_events.expanduser().resolve()
        self.market_products = market_products.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.rules = rules or BehaviorGraphRules()
        self.rules.validate()

        for path in (self.canonical_user_events, self.market_products):
            if not path.is_file():
                raise FileNotFoundError(path)

        self.work_dir = self.output_dir / "_work"
        self.user_product_first = self.work_dir / "user_product_first.parquet"
        self.product_user_totals = self.work_dir / "product_user_totals.parquet"
        self.pair_user_events = self.work_dir / "pair_user_events.parquet"
        self.pair_full_counts = self.work_dir / "pair_full_counts.parquet"

        self.product_user_cumulative = self.output_dir / "product_user_cumulative.parquet"
        self.pair_cumulative = self.output_dir / "pair_cumulative.parquet"
        self.full_graph_edges = self.output_dir / "full_graph_edges.parquet"
        self.full_graph_components = self.output_dir / "full_graph_components.parquet"
        self.graph_market_overlap = self.output_dir / "graph_market_overlap.parquet"
        self.market_graph_summary = self.output_dir / "market_graph_summary.parquet"
        self.summary_path = self.output_dir / "behavior_graph_summary.json"
        self._configure(self.output_dir)

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)

        write_user_product_first(
            self.con,
            self.canonical_user_events,
            self.market_products,
            self.user_product_first,
            self._copy_atomic,
        )
        write_product_user_totals(
            self.con,
            self.user_product_first,
            self.product_user_totals,
            self._copy_atomic,
        )
        write_product_user_cumulative(
            self.con,
            self.user_product_first,
            self.product_user_cumulative,
            self._copy_atomic,
        )
        write_pair_user_events(
            self.con,
            self.user_product_first,
            self.product_user_totals,
            self.pair_user_events,
            self._copy_atomic,
            min_endpoint_users=self.rules.min_endpoint_users,
        )
        write_pair_full_counts(
            self.con,
            self.pair_user_events,
            self.pair_full_counts,
            self._copy_atomic,
        )
        write_pair_cumulative(
            self.con,
            self.pair_user_events,
            self.pair_cumulative,
            self._copy_atomic,
        )

        # 完整时期图只保留作构建侧 audit，不参与历史 Case 选择。
        write_full_graph_edges(
            self.con,
            self.pair_full_counts,
            self.product_user_totals,
            self.full_graph_edges,
            self._copy_atomic,
            min_endpoint_users=self.rules.min_endpoint_users,
            min_shared_users=self.rules.min_shared_users,
        )
        write_full_graph_components(
            self.con,
            self.full_graph_edges,
            self.product_user_totals,
            self.full_graph_components,
            self._copy_atomic,
            min_endpoint_users=self.rules.min_endpoint_users,
        )
        write_graph_market_overlap(
            self.con,
            self.full_graph_components,
            self.market_products,
            self.graph_market_overlap,
            self._copy_atomic,
        )
        write_market_graph_summary(
            self.con,
            self.full_graph_components,
            self.market_products,
            self.market_graph_summary,
            self._copy_atomic,
        )

        payload = {
            "status": "COMPLETE",
            "schema_version": "behavior_graph_v2",
            "graph_rules": self.rules.as_dict(),
            "pair_scope": "same_final_market_and_same_leaf_category",
            "user_product_membership": "first_observed_interaction",
            "pair_event_date": "max(first_event_date_a, first_event_date_b)",
            "case_visibility_rule": "event_date < t0",
            "full_graph_role": "audit_only",
            "production_case_role": "focal_centered_competitor_selection_only",
            "pair_cumulative_path": str(self.pair_cumulative),
            "product_user_cumulative_path": str(self.product_user_cumulative),
        }
        write_json(self.summary_path, payload)
        return payload


class BehaviorGraphCasePipeline(_DuckDBStage):
    """LEGACY: pre-t0 co-review competitor truncation.

    Electronics v1 competitors come from case_build.shelf, not this pipeline.
    """

    def __init__(
        self,
        case_shelf: Path,
        market_products: Path,
        product_user_cumulative: Path,
        pair_cumulative: Path,
        output_dir: Path,
        *,
        rules: BehaviorGraphRules | None = None,
    ) -> None:
        self.case_shelf = case_shelf.expanduser().resolve()
        self.market_products = market_products.expanduser().resolve()
        self.product_user_cumulative = product_user_cumulative.expanduser().resolve()
        self.pair_cumulative = pair_cumulative.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.rules = rules or BehaviorGraphRules()
        self.rules.validate()

        for path in (
            self.case_shelf,
            self.market_products,
            self.product_user_cumulative,
            self.pair_cumulative,
        ):
            if not path.is_file():
                raise FileNotFoundError(path)

        self.work_dir = self.output_dir / "_work"
        self.focal_coreview_features = self.work_dir / "case_focal_coreview_features.parquet"
        self.case_shelf_selected = self.output_dir / "case_shelf_selected.parquet"
        self.summary_path = self.output_dir / "case_graph_summary.json"
        self._configure(self.output_dir)

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)

        write_case_focal_coreview_features(
            self.con,
            self.case_shelf,
            self.market_products,
            self.product_user_cumulative,
            self.pair_cumulative,
            self.focal_coreview_features,
            self._copy_atomic,
            min_endpoint_users=self.rules.min_endpoint_users,
            min_shared_users=self.rules.min_shared_users,
        )
        write_selected_case_shelf(
            self.con,
            self.case_shelf,
            self.focal_coreview_features,
            self.case_shelf_selected,
            self._copy_atomic,
            max_competitors=self.rules.max_competitors,
        )

        payload = {
            "status": "COMPLETE",
            "schema_version": "case_behavior_graph_v2",
            "graph_rules": self.rules.as_dict(),
            "future_data_used": False,
            "visibility_rule": "all co-review counts use event_date < case.t0",
            "selection_policy": {
                "trigger": f"competitor_pool_size > {self.rules.max_competitors}",
                "priority_1": "strong pre-t0 focal co-review relation",
                "priority_2": "shared_users_pre_t0 descending within strong relations",
                "fill": "pre_t0_recent_review_count then pre_t0_review_count",
            },
            "case_count": int(self.con.execute(
                "SELECT count(DISTINCT case_candidate_id) FROM read_parquet(?)",
                [str(self.case_shelf_selected)],
            ).fetchone()[0]),
            "selection_triggered_case_count": int(self.con.execute(
                "SELECT count(DISTINCT case_candidate_id) FROM read_parquet(?) "
                "WHERE selection_triggered",
                [str(self.case_shelf_selected)],
            ).fetchone()[0]),
            "max_selected_competitors": int(self.con.execute(
                "SELECT max(n) FROM ("
                "SELECT case_candidate_id, count(*) FILTER (WHERE role='competitor') AS n "
                "FROM read_parquet(?) GROUP BY case_candidate_id)",
                [str(self.case_shelf_selected)],
            ).fetchone()[0] or 0),
            "selected_shelf_path": str(self.case_shelf_selected),
        }
        write_json(self.summary_path, payload)
        return payload
