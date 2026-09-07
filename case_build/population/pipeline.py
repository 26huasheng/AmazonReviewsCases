from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from case_build.config import (
    BACKGROUND_CANDIDATE_POOL,
    MAX_DAYS_SINCE_LAST_EVENT,
    MIN_HISTORY_PRODUCTS,
    TARGET_BACKGROUND_USERS_PER_CASE,
)
from utils import sql_literal, write_json
from .cohorts import (
    write_case_background_users,
    write_case_market_users,
    write_case_population_union,
)


class CasePopulationPipeline:
    """Split Case users into a complete Market cohort and a sampled background cohort."""

    def __init__(
        self,
        cases: Path,
        user_history: Path,
        user_category_history: Path,
        user_market_history: Path,
        user_summary: Path,
        output_dir: Path,
        *,
        min_history_products: int = MIN_HISTORY_PRODUCTS,
        max_days_since_last_event: int = MAX_DAYS_SINCE_LAST_EVENT,
        target_background_users: int = TARGET_BACKGROUND_USERS_PER_CASE,
        background_candidate_pool: int = BACKGROUND_CANDIDATE_POOL,
        sampling_seed: str = "case_population_v1",
        market_population: Path | None = None,
    ) -> None:
        self.cases = cases.expanduser().resolve()
        self.user_history = user_history.expanduser().resolve()
        self.user_category_history = user_category_history.expanduser().resolve()
        self.user_market_history = user_market_history.expanduser().resolve()
        self.user_summary = user_summary.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        for path in (
            self.cases, self.user_history, self.user_category_history,
            self.user_market_history, self.user_summary,
        ):
            if not path.is_file():
                raise FileNotFoundError(path)
        self.min_history_products = min_history_products
        self.max_days_since_last_event = max_days_since_last_event
        self.target_background_users = target_background_users
        self.background_candidate_pool = background_candidate_pool
        self.sampling_seed = sampling_seed

        self.market_users_path = self.output_dir / "case_market_users.parquet"
        self.background_users_path = self.output_dir / "case_background_users.parquet"
        self.population_users_path = self.output_dir / "case_population_users.parquet"
        self.users_path = self.output_dir / "case_users.parquet"
        self.summary_path = self.output_dir / "case_population_summary.json"
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

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        work = self.output_dir / "_work"
        write_case_market_users(
            self.con,
            self.cases,
            self.user_history,
            self.user_market_history,
            self.market_users_path,
            self._copy_atomic,
            work_dir=work / "market_users",
            min_history_products=self.min_history_products,
            max_days_since_last_event=self.max_days_since_last_event,
        )
        write_case_background_users(
            self.con,
            self.cases,
            self.user_summary,
            self.user_history,
            self.user_category_history,
            self.user_market_history,
            self.market_users_path,
            self.background_users_path,
            self._copy_atomic,
            work_dir=work / "background_users",
            min_history_products=self.min_history_products,
            max_days_since_last_event=self.max_days_since_last_event,
            candidate_pool=self.background_candidate_pool,
            target_users_per_case=self.target_background_users,
            seed=self.sampling_seed,
        )
        write_case_population_union(
            self.con,
            self.market_users_path,
            self.background_users_path,
            self.population_users_path,
            self._copy_atomic,
        )
        self._copy_atomic(
            f"SELECT * FROM read_parquet({sql_literal(str(self.population_users_path))})",
            self.users_path,
        )
        payload = {
            "status": "COMPLETE",
            "schema_version": "case_population_v2",
            "case_count": int(self.con.execute(
                "SELECT count(DISTINCT case_id) FROM read_parquet(?)",
                [str(self.cases)],
            ).fetchone()[0]),
            "market_user_rows": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.market_users_path)]
            ).fetchone()[0]),
            "background_user_rows": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.background_users_path)]
            ).fetchone()[0]),
            "union_user_rows": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.users_path)]
            ).fetchone()[0]),
            "eligibility_policy": {
                "min_history_products": self.min_history_products,
                "max_days_since_last_event": self.max_days_since_last_event,
            },
            "background_candidate_pool": self.background_candidate_pool,
            "target_background_users_per_case": self.target_background_users,
            "sampling_seed": self.sampling_seed,
            "market_cohort_capped": False,
            "future_data_used_for_selection": False,
        }
        write_json(self.summary_path, payload)
        return payload
