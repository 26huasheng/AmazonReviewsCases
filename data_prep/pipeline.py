from __future__ import annotations

from pathlib import Path
from typing import Any

from population_scan.scan import PopulationScanner
from utils import read_json

from .build_inputs import build_market_discovery_inputs
from .download import prepare_category_sources
from .sources import category_source_paths, validate_category_name


REQUIRED_CACHE_FILES = (
    "product_core.parquet",
    "product_core_cleaning.json",
    "rating_daily_summary.parquet",
    "product_time_summary.parquet",
    "storage_metadata.json",
)


def cache_outputs_ready(cache_dir: Path) -> bool:
    if not all((cache_dir / name).is_file() for name in REQUIRED_CACHE_FILES):
        return False
    event_store = cache_dir / "rating_event_store"
    if not event_store.is_dir():
        return False
    return any(event_store.glob("source_partition=*/bucket=*/events.parquet"))


def population_scan_ready(population_dir: Path) -> bool:
    users = population_dir / "users.parquet"
    summary_path = population_dir / "summary.json"
    if not users.is_file() or not summary_path.is_file():
        return False
    if not (population_dir / "review_events.parquet").is_file():
        return False
    try:
        summary = read_json(summary_path)
    except (OSError, ValueError):
        return False
    return (
        summary.get("min_history_unit") == "n_reviews"
        and summary.get("source_metadata", {}).get("source_kind") == "jsonl_reviews"
        and int(summary.get("min_history") or 0) == 2
    )


def scan_category_user_pool(
    category: str,
    reviews: Path,
    population_dir: Path,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Scan full-review JSONL. Empty text still counts; users with one review are dropped."""
    if population_scan_ready(population_dir) and not force:
        return {
            "status": "existing / verified",
            "users": population_dir / "users.parquet",
            "review_events": population_dir / "review_events.parquet",
            "summary": population_dir / "summary.json",
        }
    scanner = PopulationScanner(
        reviews,
        population_dir,
        source_partition=category,
        min_history=2,
    )
    try:
        summary = scanner.run()
    finally:
        scanner.close()
    return {
        "status": "scanned",
        "users": scanner.users_path,
        "review_events": scanner.events_path,
        "summary": scanner.summary_path,
        "user_count": summary["user_count"],
        "event_count": summary["event_count"],
    }


def prepare_category_inputs(
    category: str,
    data_root: Path,
    output_cache: Path,
    *,
    force_rebuild: bool = False,
    event_store_bucket_count: int | None = None,
) -> dict[str, Any]:
    category = validate_category_name(category)
    downloads = prepare_category_sources(data_root, category)
    cache = output_cache.expanduser().resolve()
    rebuilt = False
    if cache_outputs_ready(cache) and not force_rebuild:
        outputs = {
            "product_core": cache / "product_core.parquet",
            "product_core_cleaning": cache / "product_core_cleaning.json",
            "rating_daily_summary": cache / "rating_daily_summary.parquet",
            "product_time_summary": cache / "product_time_summary.parquet",
            "rating_event_store": cache / "rating_event_store",
            "storage_metadata": cache / "storage_metadata.json",
        }
    else:
        outputs = build_market_discovery_inputs(
            category, data_root, cache, event_store_bucket_count=event_store_bucket_count
        )
        rebuilt = True
    population_dir = cache / "population_scan"
    population = scan_category_user_pool(
        category,
        category_source_paths(data_root, category)["reviews"],
        population_dir,
        force=force_rebuild or rebuilt,
    )
    outputs = dict(outputs)
    outputs["users"] = population["users"]
    outputs["review_events"] = population.get(
        "review_events", population_dir / "review_events.parquet"
    )
    outputs["population_summary"] = population["summary"]
    return {
        "category": category,
        "downloads": downloads,
        "rebuilt": rebuilt,
        "population": population,
        "outputs": outputs,
        "sources": {kind: str(path) for kind, path in category_source_paths(data_root, category).items()},
    }
