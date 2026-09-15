from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb


def _hash01(value: str, seed: str) -> float:
    digest = hashlib.sha256(f"{seed}\x1f{value}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / float(2**64)


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def read_cases(con: duckdb.DuckDBPyConnection, accepted_cases: Path) -> list[dict[str, Any]]:
    rows = con.execute("""
        SELECT case_candidate_id, market_id, t0
        FROM read_parquet(?)
        ORDER BY market_id, t0, case_candidate_id
    """, [str(accepted_cases)]).fetchall()
    return [
        {"case_candidate_id": str(case_id), "market_id": str(market_id), "t0": _as_date(t0)}
        for case_id, market_id, t0 in rows
    ]


def market_holdout_split(
    cases: list[dict[str, Any]],
    *,
    learning_fraction: float,
    validation_fraction: float,
    seed: str,
) -> list[dict[str, Any]]:
    if learning_fraction < 0 or validation_fraction < 0 or learning_fraction + validation_fraction > 1:
        raise ValueError("invalid split fractions")
    market_split: dict[str, str] = {}
    for market_id in sorted({row["market_id"] for row in cases}):
        value = _hash01(market_id, seed)
        if value < learning_fraction:
            split = "learning"
        elif value < learning_fraction + validation_fraction:
            split = "validation"
        else:
            split = "evaluation"
        market_split[market_id] = split
    return [
        {
            **row,
            "split_name": market_split[row["market_id"]],
            "evaluation_regime": (
                "unseen_market" if market_split[row["market_id"]] == "evaluation" else None
            ),
        }
        for row in cases
    ]


def temporal_within_market_split(
    cases: list[dict[str, Any]],
    *,
    learning_fraction: float,
    validation_fraction: float,
) -> list[dict[str, Any]]:
    if learning_fraction < 0 or validation_fraction < 0 or learning_fraction + validation_fraction > 1:
        raise ValueError("invalid split fractions")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cases:
        grouped[row["market_id"]].append(row)
    output: list[dict[str, Any]] = []
    for market_id, rows in grouped.items():
        rows = sorted(rows, key=lambda r: (r["t0"], r["case_candidate_id"]))
        n = len(rows)
        for index, row in enumerate(rows):
            position = index / n if n else 0.0
            if n >= 2 and index == n - 1:
                split = "evaluation"
            elif position < learning_fraction:
                split = "learning"
            elif position < learning_fraction + validation_fraction:
                split = "validation"
            else:
                split = "evaluation"
            output.append({
                **row,
                "split_name": split,
                "evaluation_regime": (
                    "seen_market_temporal" if split == "evaluation" else None
                ),
            })
    return output


def hybrid_split(
    cases: list[dict[str, Any]],
    *,
    unseen_market_fraction: float,
    seen_learning_fraction: float,
    seen_validation_fraction: float,
    seed: str,
) -> list[dict[str, Any]]:
    if not 0 <= unseen_market_fraction <= 1:
        raise ValueError("unseen_market_fraction must be in [0,1]")
    unseen = {
        market_id
        for market_id in {row["market_id"] for row in cases}
        if _hash01(market_id, seed) < unseen_market_fraction
    }
    seen_cases = [row for row in cases if row["market_id"] not in unseen]
    seen_split = temporal_within_market_split(
        seen_cases,
        learning_fraction=seen_learning_fraction,
        validation_fraction=seen_validation_fraction,
    )
    output = list(seen_split)
    for row in cases:
        if row["market_id"] in unseen:
            output.append({
                **row,
                "split_name": "evaluation",
                "evaluation_regime": "unseen_market",
            })
    return sorted(output, key=lambda r: (r["market_id"], r["t0"], r["case_candidate_id"]))
