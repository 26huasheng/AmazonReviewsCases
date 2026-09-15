from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "attach_history_review_text.py"


def _load_attach():
    spec = importlib.util.spec_from_file_location("attach_history_review_text", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _ts(year, month, day, hour=12):
    return datetime(year, month, day, hour, 0, 0, tzinfo=timezone.utc)


def _write_events(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _event(**kwargs) -> dict:
    base = {
        "case_id": "C1",
        "focal_id": "F1",
        "user_id": "U1",
        "event_date": "2022-01-01",
        "event_timestamp": "2022-01-01T12:00:00",
        "product_id": "P1",
        "rating": 5.0,
        "verified_purchase": True,
        "source_partition": "Electronics",
    }
    base.update(kwargs)
    return base


def test_enrichment_preserves_row_count_and_identity(tmp_path: Path) -> None:
    attach = _load_attach()
    events_path = tmp_path / "mkt" / "users" / "histories" / "events.jsonl"
    identity = [
        _event(user_id="U1", product_id="P1", event_timestamp="2022-01-01T12:00:00", event_date="2022-01-01"),
        _event(user_id="U2", product_id="P2", event_timestamp="2022-02-02T08:00:00", event_date="2022-02-02"),
        _event(user_id="U3", product_id="P3", event_timestamp="2022-03-03T09:00:00", event_date="2022-03-03"),
    ]
    _write_events(events_path, identity)
    before = _read_jsonl(events_path)

    reviews = tmp_path / "reviews.jsonl"
    ts1 = int(_ts(2022, 1, 1, 12).timestamp() * 1000)
    ts2 = int(_ts(2022, 2, 2, 8).timestamp() * 1000)
    reviews.write_text(
        json.dumps({
            "user_id": "U1", "asin": "P1", "parent_asin": "P1",
            "timestamp": ts1, "title": "t1", "text": "body1", "rating": 5,
        }) + "\n"
        + json.dumps({
            "user_id": "U2", "asin": "P2", "parent_asin": "P2",
            "timestamp": ts2, "title": "t2", "text": "body2", "rating": 4,
        }) + "\n",
        encoding="utf-8",
    )

    payload = attach.attach_review_text(tmp_path, reviews)
    after = _read_jsonl(events_path)

    assert payload["event_rows"] == 3
    assert payload["source_events"] == 3
    assert payload["rewritten_event_rows"] == 3
    assert len(after) == len(before)
    keys = ("case_id", "focal_id", "user_id", "event_date", "event_timestamp", "product_id")
    assert [{k: row[k] for k in keys} for row in after] == [{k: row[k] for k in keys} for row in before]
    by_user = {row["user_id"]: row for row in after}
    assert by_user["U1"]["review_title"] == "t1"
    assert by_user["U1"]["review_text"] == "body1"
    assert by_user["U2"]["review_text"] == "body2"
    assert by_user["U3"]["review_text"] is None


def test_date_fallback_does_not_duplicate_rows(tmp_path: Path) -> None:
    attach = _load_attach()
    events_path = tmp_path / "mkt" / "users" / "histories" / "events.jsonl"
    _write_events(events_path, [
        _event(user_id="U1", product_id="P1", event_timestamp="2022-01-01T12:00:00", event_date="2022-01-01"),
        _event(user_id="U1", product_id="P1", event_timestamp="2022-01-01T18:00:00", event_date="2022-01-01"),
    ])
    reviews = tmp_path / "reviews.jsonl"
    # Two source reviews same user/product/day, different times; neither matches 12:00 exactly.
    reviews.write_text(
        json.dumps({
            "user_id": "U1", "asin": "P1", "parent_asin": "P1",
            "timestamp": int(_ts(2022, 1, 1, 10).timestamp() * 1000),
            "title": "short", "text": "x", "rating": 5,
        }) + "\n"
        + json.dumps({
            "user_id": "U1", "asin": "P1", "parent_asin": "P1",
            "timestamp": int(_ts(2022, 1, 1, 11).timestamp() * 1000),
            "title": "long-title", "text": "much longer body", "rating": 4,
        }) + "\n",
        encoding="utf-8",
    )
    payload = attach.attach_review_text(tmp_path, reviews)
    after = _read_jsonl(events_path)
    assert payload["rewritten_event_rows"] == 2
    assert len(after) == 2
    keys = ("user_id", "product_id", "event_timestamp")
    assert [tuple(row[k] for k in keys) for row in after] == [
        ("U1", "P1", "2022-01-01T12:00:00"),
        ("U1", "P1", "2022-01-01T18:00:00"),
    ]
    # Date fallback is 1:1 per event; both events share the same day key so both
    # may receive the single chosen date-level review, but must not explode rows.
    texts = {row.get("review_text") for row in after}
    assert None not in texts
    assert len(texts) == 1
