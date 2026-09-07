from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from data_prep.download import prepare_category_sources
from data_prep.pipeline import prepare_category_inputs
from data_prep.sources import validate_category_name, validate_category_source


def _ms(day: datetime) -> int:
    return int(day.timestamp() * 1000)


def _write_sources(root: Path, category: str, products: list[dict], ratings: list[dict],
                   reviews: list[dict] | None = None) -> None:
    rating_path = root / f"benchmark/0core/rating_only/{category}.csv"
    rating_path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["user_id,parent_asin,rating,timestamp"]
    for row in ratings:
        lines.append(f"{row['user_id']},{row['parent_asin']},{row['rating']},{row['timestamp']}")
    rating_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    meta_path = root / f"raw/meta_categories/meta_{category}.jsonl"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in products),
        encoding="utf-8",
    )
    review_path = root / f"raw/review_categories/{category}.jsonl"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    payload = reviews if reviews is not None else [
        {
            "rating": row["rating"],
            "timestamp": int(row["timestamp"]),
            "parent_asin": row["parent_asin"],
            "user_id": row["user_id"],
            "text": "ok",
        }
        for row in ratings
    ]
    review_path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in payload),
        encoding="utf-8",
    )


def test_validate_category_name_rejects_paths() -> None:
    with pytest.raises(ValueError):
        validate_category_name("../Electronics")


def test_existing_full_sources_are_not_redownloaded(tmp_path: Path) -> None:
    _write_sources(tmp_path, "Electronics", [
        {"parent_asin": "A", "title": "Focal", "categories": ["Root", "Leaf"]},
    ], [{"user_id": "u1", "parent_asin": "A", "rating": "5", "timestamp": "1"}])
    with patch("data_prep.download.download_file", side_effect=AssertionError("must not download")):
        result = prepare_category_sources(tmp_path, "Electronics")
    assert result["reviews"]["status"] == "existing / verified"
    assert result["rating"]["status"] == "existing / verified"
    assert result["metadata"]["status"] == "existing / verified"


def test_missing_reviews_are_downloaded(tmp_path: Path) -> None:
    _write_sources(tmp_path, "Electronics", [
        {"parent_asin": "A", "title": "Focal", "categories": ["Root", "Leaf"]},
    ], [{"user_id": "u1", "parent_asin": "A", "rating": "5", "timestamp": "1"}])
    (tmp_path / "raw/review_categories/Electronics.jsonl").unlink()

    def download(_url: str, path: Path) -> dict:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"rating": 5, "timestamp": 1, "parent_asin": "A"}) + "\n",
            encoding="utf-8",
        )
        return {}

    with patch("data_prep.download.probe_url", return_value={"size_bytes": 10}), \
            patch("data_prep.download.download_file", side_effect=download) as downloader:
        result = prepare_category_sources(tmp_path, "Electronics")
    assert downloader.call_count == 1
    assert downloader.call_args.args[1].name == "Electronics.jsonl"
    assert result["reviews"]["status"] == "downloaded"


def test_product_time_summary_post90_is_left_closed_right_open(tmp_path: Path) -> None:
    t0 = datetime(2020, 1, 1, 12, 0, tzinfo=timezone.utc)
    offsets_and_counts = ((0, 2), (30, 3), (89, 4), (90, 5), (120, 6))
    ratings = []
    user = 0
    for offset, count in offsets_and_counts:
        stamp = _ms(t0 + timedelta(days=offset))
        for _ in range(count):
            user += 1
            ratings.append({
                "user_id": f"u{user}",
                "parent_asin": "A",
                "rating": "5",
                "timestamp": str(stamp),
            })
    data = tmp_path / "data"
    _write_sources(data, "Electronics", [
        {"parent_asin": "A", "title": "Focal", "categories": ["Root", "Leaf"],
         "details": {"Date First Available": ""}},
        {"parent_asin": "UNRATED", "title": "No ratings", "categories": ["Root", "Leaf"]},
        {"parent_asin": "NOTITLE", "title": "   ", "categories": ["Root", "Leaf"]},
    ], ratings + [
        {"user_id": "keep2", "parent_asin": "A", "rating": "4",
         "timestamp": str(_ms(t0))},
        {"user_id": "keep2", "parent_asin": "A", "rating": "5",
         "timestamp": str(_ms(t0 + timedelta(days=1)))},
    ], reviews=[
        {
            "user_id": "keep2", "parent_asin": "A", "rating": 4,
            "timestamp": _ms(t0), "text": "",
        },
        {
            "user_id": "keep2", "parent_asin": "A", "rating": 5,
            "timestamp": _ms(t0 + timedelta(days=1)), "text": "ok",
        },
        {
            "user_id": "only_empty", "parent_asin": "A", "rating": 5,
            "timestamp": _ms(t0), "text": "",
        },
        {
            "user_id": "only_text", "parent_asin": "A", "rating": 5,
            "timestamp": _ms(t0), "text": "once",
        },
    ])
    cache = tmp_path / "cache"
    with patch("data_prep.download.download_file", side_effect=AssertionError("must not download")):
        result = prepare_category_inputs("Electronics", data, cache, event_store_bucket_count=7)
    paths = result["outputs"]
    con = duckdb.connect()
    row = con.execute("""
        SELECT source_partition, product_id, entry_date, first_rating_date,
               last_rating_date, total_rating_count, post90_rating_count
        FROM read_parquet(?)
    """, [str(paths["product_time_summary"])]).fetchone()
    assert row[0] == "Electronics"
    assert row[1] == "A"
    assert str(row[2]) == "2020-01-01"
    assert str(row[3]) == "2020-01-01"
    assert str(row[4]) == "2020-04-30"
    assert row[5] == 22
    assert row[6] == 11
    core_ids = {item[0] for item in con.execute(
        "SELECT product_id FROM read_parquet(?)", [str(paths["product_core"])]
    ).fetchall()}
    assert core_ids == {"A"}
    users = con.execute(
        "SELECT user_id, n_events, n_products FROM read_parquet(?) ORDER BY user_id",
        [str(paths["users"])],
    ).fetchall()
    assert users == [("keep2", 2, 1)]
    event_cols = [row[0] for row in con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [str(result["outputs"]["review_events"])]
    ).fetchall()]
    assert "review_text" not in event_cols
    event_rows = con.execute(
        "SELECT user_id, has_review_text FROM read_parquet(?) ORDER BY user_id, has_review_text",
        [str(result["outputs"]["review_events"])],
    ).fetchall()
    assert ("keep2", False) in event_rows
    assert ("keep2", True) in event_rows
    assert ("only_empty", False) in event_rows
    summary = json.loads(Path(result["outputs"]["population_summary"]).read_text(encoding="utf-8"))
    assert summary["empty_review_text_count"] >= 1
    assert summary["min_history"] == 2
    assert summary["source_metadata"]["empty_review_text_kept"] is True
    assert summary["source_metadata"]["review_text_persisted"] is False
    with patch("data_prep.build_inputs.build_market_discovery_inputs",
               side_effect=AssertionError("must reuse cache")):
        again = prepare_category_inputs("Electronics", data, cache)
    assert again["rebuilt"] is False
    assert again["population"]["status"] == "existing / verified"


def test_reviews_require_parent_asin_or_asin(tmp_path: Path) -> None:
    path = tmp_path / "reviews.jsonl"
    path.write_text(json.dumps({"rating": 5, "timestamp": 1}) + "\n", encoding="utf-8")
    valid, reason = validate_category_source("reviews", path)
    assert valid is False
    assert reason and "parent_asin_or_asin" in reason
