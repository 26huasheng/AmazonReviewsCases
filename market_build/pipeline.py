from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from market_discovery.cross_path_merge import MIN_FINAL_MARKET_PRODUCT_COUNT
from utils import sql_literal, write_json
from .history import write_user_history_indexes
from .population import write_market_population
from .products import write_market_products
from .user_events import (
    write_canonical_user_events,
    write_user_event_store,
    write_user_event_store_manifest,
)
from .behavior_graph.components import write_behavior_components
from .behavior_graph.full_graph import (
    write_product_graph_degrees,
    write_strong_copreview_edges,
)


class MarketBuildPipeline:
    """把 Final Market 变成可被多个 Case 复用的 Market-level 资产。"""

    def __init__(
        self,
        final_market: Path,
        product_core: Path,
        user_events: Path,
        user_summary: Path,
        output_dir: Path,
        *,
        product_time_summary: Path | None = None,
        fallback_source_partition: str | None = None,
        population_source: str = "category",
        population_size: int | None = None,
        population_seed: str = "market_population_v1",
        user_bucket_count: int = 256,
        min_product_count: int = MIN_FINAL_MARKET_PRODUCT_COUNT,
    ) -> None:
        self.final_market = final_market.expanduser().resolve()
        self.product_core = product_core.expanduser().resolve()
        self.user_events = user_events.expanduser().resolve()
        self.user_summary = user_summary.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        self.product_time_summary = (
            product_time_summary.expanduser().resolve()
            if product_time_summary else None
        )
        self.fallback_source_partition = fallback_source_partition
        self.population_source = population_source
        self.population_size = population_size
        self.population_seed = population_seed
        self.user_bucket_count = user_bucket_count
        if min_product_count < 0:
            raise ValueError("min_product_count must be >= 0")
        self.min_product_count = min_product_count
        for path in (
            self.final_market, self.product_core, self.user_events, self.user_summary
        ):
            if not path.exists():
                raise FileNotFoundError(path)
        if self.product_time_summary and not self.product_time_summary.is_file():
            raise FileNotFoundError(self.product_time_summary)

        self.work_dir = self.output_dir / "_work"
        self.canonical_events_path = self.output_dir / "canonical_user_events.parquet"
        self.user_event_store_dir = self.output_dir / "user_event_store"
        self.user_event_store_manifest = self.output_dir / "user_event_store_manifest.json"
        self.market_products_path = self.output_dir / "market_products.parquet"
        self.market_population_path = self.output_dir / "market_population.parquet"
        self.user_history_path = self.output_dir / "user_history_cumulative.parquet"
        self.user_category_history_path = self.output_dir / "user_category_history_cumulative.parquet"
        self.user_market_history_path = self.output_dir / "user_market_history_cumulative.parquet"
        self.graph_dir = self.output_dir / "behavior_graph"
        self.graph_edges_path = self.graph_dir / "full_graph_edges.parquet"
        self.graph_degrees_path = self.work_dir / "product_graph_degrees.parquet"
        self.behavior_components_path = (
            self.output_dir / "market_behavior_components.parquet"
        )
        self.behavior_component_summary_path = (
            self.output_dir / "market_behavior_component_summary.parquet"
        )
        self.full_graph_components_path = self.graph_dir / "full_graph_components.parquet"
        self.summary_path = self.output_dir / "market_build_summary.json"

        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        self.con.execute("SET preserve_insertion_order=false")
        self.con.execute("SET memory_limit='100GB'")
        temp = self.output_dir / ".duckdb_tmp"
        temp.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"SET temp_directory={sql_literal(str(temp))}")
        self.con.execute("SET max_temp_directory_size='400GiB'")

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

        source_meta = write_canonical_user_events(
            self.con,
            self.user_events,
            self.canonical_events_path,
            self._copy_atomic,
            fallback_source_partition=self.fallback_source_partition,
        )
        write_user_event_store(
            self.con,
            self.canonical_events_path,
            self.user_event_store_dir,
            bucket_count=self.user_bucket_count,
        )
        write_user_event_store_manifest(
            self.user_event_store_manifest,
            source=self.user_events,
            bucket_count=self.user_bucket_count,
            source_metadata=source_meta,
        )
        write_market_products(
            self.con,
            self.final_market,
            self.product_core,
            self.market_products_path,
            self._copy_atomic,
            product_time_summary=self.product_time_summary,
            min_product_count=self.min_product_count,
        )
        market_count = int(self.con.execute(
            "SELECT count(DISTINCT market_id) FROM read_parquet(?)",
            [str(self.market_products_path)],
        ).fetchone()[0])
        if market_count == 0:
            raise ValueError(
                f"no markets with product_count >= {self.min_product_count} "
                f"in {self.final_market}"
            )
        write_market_population(
            self.con,
            self.market_products_path,
            self.user_summary,
            self.market_population_path,
            self._copy_atomic,
            population_source=self.population_source,
            population_size=self.population_size,
            seed=self.population_seed,
        )
        write_user_history_indexes(
            self.con,
            self.canonical_events_path,
            self.market_products_path,
            self.user_history_path,
            self.user_category_history_path,
            self.user_market_history_path,
            self._copy_atomic,
        )
        self.graph_dir.mkdir(parents=True, exist_ok=True)
        write_product_graph_degrees(
            self.con,
            self.market_products_path,
            self.canonical_events_path,
            self.graph_degrees_path,
            self._copy_atomic,
        )
        write_strong_copreview_edges(
            self.con,
            self.market_products_path,
            self.canonical_events_path,
            self.graph_edges_path,
            self._copy_atomic,
        )
        graph_stats = write_behavior_components(
            self.con,
            self.graph_degrees_path,
            self.graph_edges_path,
            self.behavior_components_path,
            self.behavior_component_summary_path,
            self._copy_atomic,
        )
        self._copy_atomic(
            f"SELECT * FROM read_parquet({sql_literal(str(self.behavior_components_path))})",
            self.full_graph_components_path,
        )

        payload = {
            "status": "COMPLETE",
            "schema_version": "market_build_v1",
            "population_source": self.population_source,
            "population_size_cap": self.population_size,
            "population_seed": self.population_seed,
            "min_product_count": self.min_product_count,
            "market_count": market_count,
            "market_product_rows": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)",
                [str(self.market_products_path)],
            ).fetchone()[0]),
            "market_population_rows": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)",
                [str(self.market_population_path)],
            ).fetchone()[0]),
            "canonical_user_event_rows": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)",
                [str(self.canonical_events_path)],
            ).fetchone()[0]),
            "behavior_graph": graph_stats,
            "future_conditioned_population_selection": False,
        }
        write_json(self.summary_path, payload)
        return payload
