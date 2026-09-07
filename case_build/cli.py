#!/usr/bin/env python3
"""Case Build CLI.

Examples:

python -m case_build.cli discover ...
python -m case_build.cli select ...
python -m case_build.cli shelf ...
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .focal_selection import FocalSelectionPipeline
from .pipeline import CaseDiscoveryPipeline, CaseShelfBuilder


def _load_time_boxes(path: Path | None) -> list[dict[str, Any]] | None:
    if path is None:
        return None
    value = json.loads(path.expanduser().read_text(encoding="utf-8"))
    if isinstance(value, dict):
        value = value.get("time_boxes")
    if not isinstance(value, list):
        raise ValueError("time-boxes JSON must be a list or {'time_boxes': [...]} object")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build product-side Market -> Case artifacts."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser(
        "discover",
        help="Build market product timeline and candidate new-product entry cases.",
    )
    discover.add_argument("--final-market", required=True, type=Path)
    discover.add_argument("--product-core", required=True, type=Path)
    discover.add_argument("--storage-metadata", required=True, type=Path)
    discover.add_argument("--output-dir", required=True, type=Path)
    discover.add_argument("--product-time-summary", type=Path)
    discover.add_argument(
        "--rating-daily-summary",
        type=Path,
        help=(
            "Required only when --product-time-summary is omitted; used to build "
            "the product time summary."
        ),
    )
    discover.add_argument("--time-boxes-json", type=Path)
    discover.add_argument("--evaluation-days", type=int, default=90)

    select = sub.add_parser(
        "select",
        help="Assemble Market × time_box Cases and choose focals.",
    )
    select.add_argument("--evaluable-focals", required=True, type=Path)
    select.add_argument("--behavior-components", required=True, type=Path)
    select.add_argument("--output-dir", required=True, type=Path)
    select.add_argument("--selection-seed", default=None)

    shelf = sub.add_parser(
        "shelf",
        help="Materialize per-focal competitors and the union Case shelf.",
    )
    shelf.add_argument("--cases", required=True, type=Path)
    shelf.add_argument("--case-focals", required=True, type=Path)
    shelf.add_argument("--market-timeline", required=True, type=Path)
    shelf.add_argument("--rating-daily-summary", required=True, type=Path)
    shelf.add_argument("--output-dir", required=True, type=Path)
    shelf.add_argument("--recent-window-days", type=int, default=120)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "discover":
        if args.product_time_summary is None and args.rating_daily_summary is None:
            raise SystemExit(
                "discover requires --product-time-summary or --rating-daily-summary"
            )
        pipeline = CaseDiscoveryPipeline(
            args.final_market,
            args.product_core,
            args.storage_metadata,
            args.output_dir,
            product_time_summary=args.product_time_summary,
            rating_daily_summary=args.rating_daily_summary,
            time_boxes=_load_time_boxes(args.time_boxes_json),
            evaluation_days=args.evaluation_days,
        )
    elif args.command == "select":
        kwargs = {}
        if args.selection_seed:
            kwargs["selection_seed"] = args.selection_seed
        pipeline = FocalSelectionPipeline(
            args.evaluable_focals,
            args.behavior_components,
            args.output_dir,
            **kwargs,
        )
    else:
        pipeline = CaseShelfBuilder(
            args.cases,
            args.case_focals,
            args.market_timeline,
            args.rating_daily_summary,
            args.output_dir,
            recent_window_days=args.recent_window_days,
        )
    try:
        summary = pipeline.run()
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
