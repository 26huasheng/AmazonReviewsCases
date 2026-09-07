#!/usr/bin/env python3
"""Market Discovery CLI.

Run from repository root as ``python -m market_discovery.cli``.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from paths import default_output_root
from utils import configure_logging
from .cross_path_merge import MIN_FINAL_MARKET_PRODUCT_COUNT
from .discovery_pipeline import MarketDiscoveryPipeline
from .market_llm import FixtureMarketLLMClient, MarketLLMClient

LOGGER = logging.getLogger("market_discovery.cli")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Discover path-local product markets and deterministically merge "
            "same-name cross-path markets after formatting normalization."
        )
    )
    parser.add_argument("--product-core", required=True, type=Path)
    parser.add_argument("--product-core-cleaning", type=Path)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=default_output_root() / "market_discovery",
    )
    parser.add_argument("--discovery-version", default="market_v1")
    parser.add_argument("--source-partition")
    parser.add_argument("--max-paths", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--model")
    parser.add_argument("--base-url")
    parser.add_argument("--provider", default="openai_compatible")
    parser.add_argument("--llm-fixture", type=Path)
    parser.add_argument(
        "--llm-workers",
        type=int,
        default=3,
        help="Concurrent LLM calls for path-local round-1 discovery (default: 3)",
    )
    parser.add_argument("--verbose", action="store_true")
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
    configure_logging(args.verbose)
    if args.max_paths is not None and args.max_paths <= 0:
        raise SystemExit("--max-paths must be positive")
    if args.llm_workers is not None and args.llm_workers <= 0:
        raise SystemExit("--llm-workers must be positive")
    if args.min_market_products < 0:
        raise SystemExit("--min-market-products must be >= 0")

    pipeline = MarketDiscoveryPipeline(
        args.product_core,
        args.output_root,
        args.discovery_version,
        args.source_partition,
        args.product_core_cleaning,
        min_final_market_product_count=args.min_market_products,
    )
    try:
        summary = pipeline.prepare_local_evidence()
        LOGGER.info(
            "Local evidence ready: paths=%s titles=%s output=%s",
            summary["discovery_path_count"],
            summary["round1_sample_title_count"],
            pipeline.output_dir,
        )
        if args.dry_run:
            LOGGER.info("Dry-run complete; no API calls were made")
            return

        client = (
            FixtureMarketLLMClient(args.llm_fixture)
            if args.provider == "fixture" and args.llm_fixture
            else MarketLLMClient(
                model=args.model,
                base_url=args.base_url,
                provider=args.provider,
            )
        )
        workers = 1 if isinstance(client, FixtureMarketLLMClient) else args.llm_workers
        result = pipeline.run_discovery(
            client,
            args.max_paths,
            args.resume,
            llm_workers=workers,
        )
        merge = result["exact_name_merge"]
        LOGGER.info(
            "Discovery complete: processed_paths=%s local_markets=%s final_markets=%s merged_groups=%s",
            result["processed_paths"],
            merge["local_market_count"],
            merge["final_market_count"],
            merge["merged_group_count"],
        )
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
