from __future__ import annotations

import argparse
import json
from pathlib import Path

from .pipeline import BenchmarkExportPipeline


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Materialize final SEMS benchmark")
    for name in (
        "final-market", "market-products", "market-population",
        "canonical-user-events", "accepted-cases", "case-shelf", "case-users",
        "choice-truth", "population-truth", "market-truth", "split-assignments",
        "output-dir",
    ):
        p.add_argument(f"--{name}", type=Path, required=True)
    return p


def main() -> None:
    a = build_parser().parse_args()
    worker = BenchmarkExportPipeline(
        a.final_market, a.market_products, a.market_population,
        a.canonical_user_events, a.accepted_cases, a.case_shelf, a.case_users,
        a.choice_truth, a.population_truth, a.market_truth,
        a.split_assignments, a.output_dir,
    )
    try:
        result = worker.run()
    finally:
        worker.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
