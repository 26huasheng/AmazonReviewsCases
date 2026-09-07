from __future__ import annotations

import argparse
import json
from pathlib import Path

from case_build.config import (
    BACKGROUND_CANDIDATE_POOL,
    MAX_DAYS_SINCE_LAST_EVENT,
    MIN_HISTORY_PRODUCTS,
    TARGET_BACKGROUND_USERS_PER_CASE,
)
from .pipeline import CasePopulationPipeline


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Build Market cohort (complete) and background cohort (sampled)"
    )
    p.add_argument("--cases", type=Path, required=True)
    p.add_argument("--user-history", type=Path, required=True)
    p.add_argument("--user-category-history", type=Path, required=True)
    p.add_argument("--user-market-history", type=Path, required=True)
    p.add_argument("--user-summary", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--min-history-products", type=int, default=MIN_HISTORY_PRODUCTS)
    p.add_argument("--max-days-since-last-event", type=int, default=MAX_DAYS_SINCE_LAST_EVENT)
    p.add_argument(
        "--target-background-users",
        type=int,
        default=TARGET_BACKGROUND_USERS_PER_CASE,
    )
    p.add_argument(
        "--background-candidate-pool",
        type=int,
        default=BACKGROUND_CANDIDATE_POOL,
    )
    p.add_argument("--sampling-seed", default="case_population_v1")
    return p


def main() -> None:
    a = build_parser().parse_args()
    worker = CasePopulationPipeline(
        a.cases,
        a.user_history,
        a.user_category_history,
        a.user_market_history,
        a.user_summary,
        a.output_dir,
        min_history_products=a.min_history_products,
        max_days_since_last_event=a.max_days_since_last_event,
        target_background_users=a.target_background_users,
        background_candidate_pool=a.background_candidate_pool,
        sampling_seed=a.sampling_seed,
    )
    try:
        result = worker.run()
    finally:
        worker.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
