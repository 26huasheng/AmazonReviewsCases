from __future__ import annotations

import csv
import json
from pathlib import Path


HF_REPO = "McAuley-Lab/Amazon-Reviews-2023"
HF_ENDPOINT = "https://huggingface.co"


def category_source_paths(data_root: Path, category: str) -> dict[str, Path]:
    root = data_root.expanduser().resolve()
    return {
        "rating": root / "benchmark/0core/rating_only" / f"{category}.csv",
        "metadata": root / "raw/meta_categories" / f"meta_{category}.jsonl",
        "reviews": root / "raw/review_categories" / f"{category}.jsonl",
    }


def category_source_urls(category: str) -> dict[str, str]:
    root = f"{HF_ENDPOINT}/datasets/{HF_REPO}/resolve/main"
    return {
        "rating": f"{root}/benchmark/0core/rating_only/{category}.csv",
        "metadata": f"{root}/raw/meta_categories/meta_{category}.jsonl",
        "reviews": f"{root}/raw/review_categories/{category}.jsonl",
    }


def validate_category_name(category: str) -> str:
    text = category.strip()
    if not text or any(value in text for value in ("/", "\\", "..")):
        raise ValueError(f"invalid Amazon category: {category!r}")
    return text


def validate_category_source(kind: str, path: Path) -> tuple[bool, str | None]:
    if not path.is_file() or path.stat().st_size == 0:
        return False, "missing_or_empty"
    try:
        if kind == "rating":
            with path.open(newline="", encoding="utf-8") as handle:
                columns = set(next(csv.reader(handle)))
            missing = {"user_id", "parent_asin", "rating", "timestamp"} - columns
        elif kind == "metadata":
            with path.open(encoding="utf-8") as handle:
                first = next(line for line in handle if line.strip())
            columns = set(json.loads(first))
            missing = {"parent_asin", "title", "categories"} - columns
        elif kind == "reviews":
            with path.open(encoding="utf-8") as handle:
                first = next(line for line in handle if line.strip())
            columns = set(json.loads(first))
            missing = {"rating", "timestamp"} - columns
            if not ({"parent_asin", "asin"} & columns):
                missing.add("parent_asin_or_asin")
        else:
            return False, f"unknown_source_kind:{kind}"
        return (False, "missing_columns:" + ",".join(sorted(missing))) if missing else (True, None)
    except (OSError, UnicodeError, csv.Error, json.JSONDecodeError, StopIteration) as exc:
        return False, f"{type(exc).__name__}:{exc}"
