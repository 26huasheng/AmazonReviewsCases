from __future__ import annotations

import argparse
import json
from pathlib import Path

from .pipeline import CaseQualityPipeline


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Focal-level Quality gate, then rebuild accepted Case content"
    )
    p.add_argument("--cases", type=Path, required=True)
    p.add_argument("--case-shelf", type=Path, required=True)
    p.add_argument("--choice-truth", type=Path, required=True)
    p.add_argument("--case-focals", type=Path, required=True)
    p.add_argument("--focal-competitors", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--rules-json", type=Path)
    p.add_argument("--gt1-users", type=Path)
    p.add_argument("--gt1-raw-outcomes", type=Path)
    p.add_argument("--gt1-shelf-events", type=Path)
    p.add_argument("--timeline", type=Path)
    p.add_argument("--review-activity-truth", type=Path)
    # Unused by the frozen focal gate; kept so old wrappers still parse.
    p.add_argument("--case-users", type=Path)
    p.add_argument("--population-truth", type=Path)
    p.add_argument("--market-truth", type=Path)
    p.add_argument("--external-signals", type=Path)
    return p


def main() -> None:
    a = build_parser().parse_args()
    worker = CaseQualityPipeline(
        a.cases,
        a.case_shelf,
        a.choice_truth,
        a.output_dir,
        case_focals=a.case_focals,
        focal_competitors=a.focal_competitors,
        rules_json=a.rules_json,
        gt1_users=a.gt1_users,
        gt1_raw_outcomes=a.gt1_raw_outcomes,
        gt1_shelf_events=a.gt1_shelf_events,
        timeline=a.timeline,
        review_activity_truth=a.review_activity_truth,
        case_users=a.case_users,
        population_truth=a.population_truth,
        market_truth=a.market_truth,
        external_signals=a.external_signals,
    )
    try:
        result = worker.run()
    finally:
        worker.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
