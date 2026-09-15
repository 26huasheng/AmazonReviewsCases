from __future__ import annotations

import argparse
import json
from pathlib import Path

from .pipeline import BenchmarkSplitPipeline


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Split accepted cases into benchmark partitions")
    p.add_argument("--accepted-cases", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument(
        "--strategy",
        choices=["market_holdout", "temporal_within_market", "hybrid"],
        default="hybrid",
    )
    p.add_argument("--seed", default="benchmark_split_v1")
    p.add_argument("--learning-fraction", type=float, default=0.7)
    p.add_argument("--validation-fraction", type=float, default=0.1)
    p.add_argument("--unseen-market-fraction", type=float, default=0.2)
    return p


def main() -> None:
    a = build_parser().parse_args()
    worker = BenchmarkSplitPipeline(
        a.accepted_cases,
        a.output_dir,
        strategy=a.strategy,
        seed=a.seed,
        learning_fraction=a.learning_fraction,
        validation_fraction=a.validation_fraction,
        unseen_market_fraction=a.unseen_market_fraction,
    )
    try:
        result = worker.run()
    finally:
        worker.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
