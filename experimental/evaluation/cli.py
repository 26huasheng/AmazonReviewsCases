from __future__ import annotations

import argparse
import json
from pathlib import Path

from .pipeline import EvaluationPipeline


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Evaluate SEMS user choice and market ranking")
    p.add_argument("--population-truth", type=Path, required=True)
    p.add_argument("--choice-truth", type=Path, required=True)
    p.add_argument("--market-truth", type=Path, required=True)
    p.add_argument("--individual-predictions", type=Path, required=True)
    p.add_argument("--market-predictions", type=Path)
    p.add_argument("--split-assignments", type=Path)
    p.add_argument("--output-dir", type=Path, required=True)
    return p


def main() -> None:
    a = build_parser().parse_args()
    worker = EvaluationPipeline(
        a.population_truth, a.choice_truth, a.market_truth,
        a.individual_predictions, a.output_dir,
        market_predictions=a.market_predictions,
        split_assignments=a.split_assignments,
    )
    try:
        result = worker.run()
    finally:
        worker.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
