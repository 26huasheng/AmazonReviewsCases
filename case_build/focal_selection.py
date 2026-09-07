from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from .config import (
    FOCAL_SELECTION_SEED,
    MIN_VALID_BEHAVIOR_GROUP_SIZE,
    POPULATION_CUTOFF_POLICY,
)
from utils import sql_literal, write_json


class FocalSelectionPipeline:
    """Assemble formal Cases as Final Market × time_box with 1..N focals."""

    def __init__(
        self,
        evaluable_focals: Path,
        behavior_components: Path,
        output_dir: Path,
        *,
        selection_seed: str = FOCAL_SELECTION_SEED,
        min_valid_group_size: int = MIN_VALID_BEHAVIOR_GROUP_SIZE,
    ) -> None:
        self.evaluable_focals = evaluable_focals.expanduser().resolve()
        self.behavior_components = behavior_components.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.selection_seed = selection_seed
        if min_valid_group_size <= 0:
            raise ValueError("min_valid_group_size must be positive")
        self.min_valid_group_size = min_valid_group_size
        for path in (self.evaluable_focals, self.behavior_components):
            if not path.is_file():
                raise FileNotFoundError(path)
        self.cases_path = self.output_dir / "cases.parquet"
        self.case_focals_path = self.output_dir / "case_focals.parquet"
        self.summary_path = self.output_dir / "focal_selection_summary.json"
        self.con = duckdb.connect()
        self.con.execute("SET preserve_insertion_order=false")
        temp = self.output_dir / ".duckdb_tmp"
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

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        focals = sql_literal(str(self.evaluable_focals))
        graph = sql_literal(str(self.behavior_components))
        seed = sql_literal(self.selection_seed)
        min_size = int(self.min_valid_group_size)
        policy = sql_literal(POPULATION_CUTOFF_POLICY)

        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW evaluable_with_graph AS
            SELECT f.*,
                   g.graph_component_id,
                   g.component_size,
                   coalesce(g.valid_behavior_group, false) AS valid_behavior_group
            FROM read_parquet({focals}) f
            LEFT JOIN read_parquet({graph}) g
              ON f.source_partition=g.source_partition
             AND f.market_id=g.market_id
             AND f.focal_product_id=g.product_id
            WHERE f.time_box_id IS NOT NULL
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW market_valid_groups AS
            SELECT source_partition, market_id,
                   count(DISTINCT graph_component_id)::BIGINT AS n_valid_behavior_groups
            FROM read_parquet({graph})
            WHERE valid_behavior_group
              AND graph_component_id IS NOT NULL
              AND component_size >= {min_size}
            GROUP BY source_partition, market_id
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW slot_mode AS
            SELECT DISTINCT e.source_partition,
                   e.market_id,
                   e.market_label,
                   e.time_box_id,
                   e.time_box_start_date,
                   e.time_box_end_date,
                   coalesce(v.n_valid_behavior_groups, 0)::BIGINT AS n_valid_behavior_groups,
                   (coalesce(v.n_valid_behavior_groups, 0) >= 2) AS graph_used
            FROM evaluable_with_graph e
            LEFT JOIN market_valid_groups v
              ON e.source_partition=v.source_partition
             AND e.market_id=v.market_id
        """)
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW ranked_candidates AS
            SELECT e.*,
                   s.n_valid_behavior_groups,
                   s.graph_used,
                   row_number() OVER (
                       PARTITION BY e.source_partition, e.market_id, e.time_box_id,
                                    CASE
                                        WHEN s.graph_used AND e.valid_behavior_group
                                            THEN e.graph_component_id
                                        ELSE NULL
                                    END
                       ORDER BY sha256(CAST(to_json(list_value(
                           {seed},
                           e.source_partition,
                           e.market_id,
                           e.time_box_id,
                           CASE WHEN s.graph_used THEN e.graph_component_id END,
                           e.focal_product_id
                       )) AS VARCHAR)), e.focal_product_id
                   ) AS selection_rank
            FROM evaluable_with_graph e
            JOIN slot_mode s
              ON e.source_partition=s.source_partition
             AND e.market_id=s.market_id
             AND e.time_box_id=s.time_box_id
        """)
        self.con.execute("""
            CREATE OR REPLACE TEMP VIEW selected_focals AS
            SELECT *
            FROM ranked_candidates
            WHERE (NOT graph_used AND selection_rank=1)
               OR (graph_used AND valid_behavior_group AND selection_rank=1)
        """)

        self._copy_atomic(f"""
            WITH aggregated AS (
                SELECT source_partition,
                       market_id,
                       any_value(market_label) AS market_label,
                       time_box_id,
                       any_value(time_box_start_date) AS time_box_start,
                       any_value(time_box_end_date) AS time_box_end,
                       count(*)::BIGINT AS n_focals,
                       any_value(graph_used) AS graph_used,
                       any_value(n_valid_behavior_groups) AS n_valid_behavior_groups,
                       min(t0) AS population_cutoff
                FROM selected_focals
                GROUP BY source_partition, market_id, time_box_id
            )
            SELECT 'case_' || substr(
                       sha256(CAST(to_json(list_value(
                           source_partition, market_id, time_box_id
                       )) AS VARCHAR)),
                       1, 20
                   ) AS case_id,
                   source_partition,
                   market_id,
                   market_label,
                   time_box_id,
                   time_box_start,
                   time_box_end,
                   n_focals,
                   graph_used,
                   n_valid_behavior_groups,
                   population_cutoff,
                   {policy}::VARCHAR AS population_cutoff_policy
            FROM aggregated
            WHERE n_focals > 0
            ORDER BY source_partition, market_id, time_box_id
        """, self.cases_path)

        cases = sql_literal(str(self.cases_path))
        self._copy_atomic(f"""
            SELECT c.case_id,
                   'focal_' || substr(
                       sha256(CAST(to_json(list_value(
                           c.case_id, f.focal_product_id, CAST(f.t0 AS VARCHAR)
                       )) AS VARCHAR)),
                       1, 20
                   ) AS focal_id,
                   coalesce(f.focal_candidate_id, f.case_candidate_id) AS focal_candidate_id,
                   f.focal_product_id,
                   f.focal_product_title,
                   f.t0,
                   f.evaluation_start,
                   f.evaluation_end_exclusive,
                   f.evaluation_days,
                   f.valid_t0,
                   f.evaluation_window_complete,
                   f.post90_rating_count,
                   f.market_pre_t0_review_count,
                   f.active_competitor_count_at_t0,
                   f.graph_component_id,
                   f.component_size,
                   CASE
                       WHEN c.graph_used THEN 'behavior_group_random'
                       ELSE 'single_or_no_valid_group_random'
                   END AS focal_selection_reason
            FROM selected_focals f
            JOIN read_parquet({cases}) c
              ON f.source_partition=c.source_partition
             AND f.market_id=c.market_id
             AND f.time_box_id=c.time_box_id
            ORDER BY c.case_id, f.t0, f.focal_product_id
        """, self.case_focals_path)

        payload = {
            "status": "COMPLETE",
            "schema_version": "focal_selection_v1",
            "selection_seed": self.selection_seed,
            "min_valid_behavior_group_size": self.min_valid_group_size,
            "population_cutoff_policy": POPULATION_CUTOFF_POLICY,
            "case_count": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.cases_path)]
            ).fetchone()[0]),
            "focal_count": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.case_focals_path)]
            ).fetchone()[0]),
        }
        write_json(self.summary_path, payload)
        return payload
