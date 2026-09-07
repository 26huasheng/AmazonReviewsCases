#!/usr/bin/env python3
"""Rebuild final_market from an existing first_market without any LLM calls."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .cross_path_merge import (
    MIN_FINAL_MARKET_PRODUCT_COUNT,
    merge_exact_normalized_markets,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Merge cross-path markets only when market labels are equal after "
            "safe formatting normalization. No LLM calls are made."
        )
    )
    parser.add_argument(
        "--discovery-dir",
        required=True,
        type=Path,
        help="Directory containing first_market.csv",
    )
    parser.add_argument(
        "--min-market-products",
        type=int,
        default=MIN_FINAL_MARKET_PRODUCT_COUNT,
        help=(
            "Drop merged markets with fewer products than this from final_market "
            f"(default: {MIN_FINAL_MARKET_PRODUCT_COUNT})"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.min_market_products < 0:
        raise SystemExit("--min-market-products must be >= 0")
    summary = merge_exact_normalized_markets(
        args.discovery_dir,
        min_product_count=args.min_market_products,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
