from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb

from .discovery_rules import normalize_market_label
from .market_io import csv_to_parquet, read_market_csv, write_market_csv


MERGE_POLICY = "exact_normalized_market_label_v1"
# Keep markets with strictly more than this many products in final_market.
MIN_FINAL_MARKET_PRODUCT_COUNT = 9


def _dedupe_paths(values: list[list[str]]) -> list[list[str]]:
    seen: set[tuple[str, ...]] = set()
    output: list[list[str]] = []
    for value in values:
        key = tuple(str(item) for item in value)
        if key in seen:
            continue
        seen.add(key)
        output.append(list(key))
    return output


def merge_exact_normalized_rows(
    first_markets: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Merge local markets only when their names are equal after safe formatting normalization.

    The merge key is ``(source_partition, normalize_market_label(market_label))``.
    This intentionally handles formatting-only variants such as ``Phone_Case``,
    ``phone-case``, ``phone case`` and ``PhoneCase``. It does not perform synonym,
    plural, stemming, embedding, or LLM-based semantic merging.
    """
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in first_markets:
        normalized = normalize_market_label(str(row["market_label"]))
        if not normalized:
            raise ValueError(f"empty normalized market label for {row['market_id']}")
        grouped[(str(row["source_partition"]), normalized)].append(row)

    final_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    for (source_partition, normalized_label), members in sorted(grouped.items()):
        versions = {str(row["discovery_version"]) for row in members}
        if len(versions) != 1:
            raise ValueError(
                f"multiple discovery versions in merge group {source_partition}/{normalized_label}: "
                f"{sorted(versions)}"
            )

        source_market_ids = sorted({
            str(source_id)
            for row in members
            for source_id in row["source_market_ids"]
        })
        source_path_ids = sorted({
            str(path_id)
            for row in members
            for path_id in row["source_path_ids"]
        })
        source_category_paths = _dedupe_paths([
            list(path)
            for row in members
            for path in row["source_category_paths"]
        ])
        product_ids = sorted({
            str(product_id)
            for row in members
            for product_id in row["product_ids"]
        })
        keep_id = min(str(row["market_id"]) for row in members)
        if keep_id not in source_market_ids:
            source_market_ids = sorted({*source_market_ids, keep_id})

        final_rows.append({
            "discovery_version": next(iter(versions)),
            "source_partition": source_partition,
            "market_id": keep_id,
            "market_label": normalized_label,
            "source_market_ids": source_market_ids,
            "source_path_ids": source_path_ids,
            "source_category_paths": source_category_paths,
            "product_ids": product_ids,
            "product_count": len(product_ids),
        })

        if len(members) > 1:
            original_labels = sorted({str(row["market_label"]) for row in members})
            audit_rows.append({
                "source_partition": source_partition,
                "normalized_market_label": normalized_label,
                "member_market_ids": sorted(str(row["market_id"]) for row in members),
                "original_market_labels": original_labels,
                "source_path_ids": source_path_ids,
                "n_local_markets": len(members),
                "n_paths": len(source_path_ids),
                "merged_product_count": len(product_ids),
                "merge_reason": "exact_or_format_normalized_name",
            })

    return final_rows, audit_rows


def _apply_min_product_count(
    rows: list[dict[str, Any]],
    min_product_count: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if min_product_count < 0:
        raise ValueError("min_product_count must be >= 0")
    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for row in rows:
        if int(row["product_count"]) >= min_product_count:
            kept.append(row)
        else:
            dropped.append({
                "market_id": row["market_id"],
                "market_label": row["market_label"],
                "source_partition": row["source_partition"],
                "product_count": int(row["product_count"]),
                "drop_reason": f"product_count<{min_product_count}",
            })
    return kept, dropped


def merge_exact_normalized_markets(
    discovery_dir: Path,
    con: duckdb.DuckDBPyConnection | None = None,
    *,
    min_product_count: int = MIN_FINAL_MARKET_PRODUCT_COUNT,
) -> dict[str, Any]:
    """Materialize ``final_market`` from ``first_market`` with no merge-time LLM calls."""
    discovery = discovery_dir.expanduser().resolve()
    first_csv = discovery / "first_market.csv"
    if not first_csv.is_file():
        raise FileNotFoundError(first_csv)

    first_markets = read_market_csv(first_csv)
    merged_rows, audit_rows = merge_exact_normalized_rows(first_markets)
    final_rows, dropped_rows = _apply_min_product_count(merged_rows, min_product_count)

    final_csv = discovery / "final_market.csv"
    final_parquet = discovery / "final_market.parquet"
    write_market_csv(final_csv, final_rows)

    owns_connection = con is None
    connection = con or duckdb.connect()
    try:
        csv_to_parquet(connection, final_csv, final_parquet, rows=final_rows)
    finally:
        if owns_connection:
            connection.close()

    audit_path = discovery / "cross_path_exact_merge_audit.json"
    audit_path.write_text(
        json.dumps({
            "merge_policy": MERGE_POLICY,
            "merge_groups": audit_rows,
        }, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    dropped_path = discovery / "dropped_small_markets.json"
    dropped_path.write_text(
        json.dumps({
            "min_product_count": min_product_count,
            "dropped_market_count": len(dropped_rows),
            "markets": dropped_rows,
        }, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    summary = {
        "status": "COMPLETE",
        "merge_policy": MERGE_POLICY,
        "llm_merge_used": False,
        "min_product_count": min_product_count,
        "local_market_count": len(first_markets),
        "merged_market_count": len(merged_rows),
        "final_market_count": len(final_rows),
        "dropped_small_market_count": len(dropped_rows),
        "merged_group_count": len(audit_rows),
        "removed_by_merge_count": len(first_markets) - len(merged_rows),
        "final_market_csv": str(final_csv),
        "final_market_parquet": str(final_parquet),
        "audit_file": str(audit_path),
        "dropped_small_markets_file": str(dropped_path),
    }
    (discovery / "cross_path_exact_merge_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary
