#!/usr/bin/env python3
"""Download one Amazon Reviews 2023 category and build tables used before Market Discovery.

Run from repository root as ``python -m data_prep.cli``.
The only required interaction is the category name.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from paths import default_data_root, default_output_root
from utils import configure_logging, json_safe

from .pipeline import prepare_category_inputs
from .sources import validate_category_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download full Amazon Reviews 2023 sources for one category, "
            "build pre-discovery tables, and scan every category user into "
            "the population pool. Stops before Market Discovery."
        )
    )
    parser.add_argument("--category", help="Amazon category, for example Electronics")
    parser.add_argument("--data-root", type=Path, default=default_data_root())
    parser.add_argument(
        "--output-root",
        type=Path,
        default=default_output_root() / "data_prep",
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Rebuild parquet tables even if a complete cache already exists",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def choose_category(configured: str | None) -> str:
    if configured:
        try:
            return validate_category_name(configured)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
    raw = input("Amazon category (e.g. Electronics): ").strip()
    try:
        return validate_category_name(raw)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    category = choose_category(args.category)
    cache = args.output_root.expanduser().resolve() / category
    result = prepare_category_inputs(
        category,
        args.data_root,
        cache,
        force_rebuild=args.force_rebuild,
    )
    print("\n=== CATEGORY SOURCE DATA ===")
    print(f"\nCategory: {category}")
    labels = {"rating": "rating-only", "metadata": "metadata", "reviews": "full reviews"}
    for kind in ("rating", "metadata", "reviews"):
        item = result["downloads"][kind]
        print(f"\n{labels[kind]}:")
        print(f"  status: {item['status']}")
        print(f"  path: {item['path']}")
    print("\n=== PRE-DISCOVERY TABLES ===")
    print(f"cache: {cache}")
    print(f"rebuilt: {result['rebuilt']}")
    for name, path in result["outputs"].items():
        print(f"  {name}: {path}")
    print("\n=== CATEGORY USER POOL ===")
    print(f"  status: {result['population']['status']}")
    print("  policy: all users with at least one rating in this category; no sampling")
    if "user_count" in result["population"]:
        print(f"  users: {result['population']['user_count']}")
        print(f"  events: {result['population']['event_count']}")
    print("\nStopped before Market Discovery.")
    print(json.dumps(json_safe({
        "category": category,
        "rebuilt": result["rebuilt"],
        "population": result["population"],
        "outputs": result["outputs"],
    }), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
