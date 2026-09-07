from __future__ import annotations

from pathlib import Path

from market_discovery.cross_path_merge import (
    MERGE_POLICY,
    merge_exact_normalized_markets,
    merge_exact_normalized_rows,
)
from market_discovery.discovery_rules import normalize_market_label
from market_discovery.market_io import read_market_csv, write_market_csv


def _market(
    market_id: str,
    label: str,
    path_id: str,
    products: list[str],
    partition: str = "Electronics",
) -> dict:
    return {
        "discovery_version": "market_v1",
        "source_partition": partition,
        "market_id": market_id,
        "market_label": label,
        "source_market_ids": [market_id],
        "source_path_ids": [path_id],
        "source_category_paths": [["Root", path_id]],
        "product_ids": products,
        "product_count": len(products),
    }


def test_format_variants_normalize_to_same_label() -> None:
    assert normalize_market_label("Phone_Case") == "phone_case"
    assert normalize_market_label("phone-case") == "phone_case"
    assert normalize_market_label("phone case") == "phone_case"
    assert normalize_market_label("  PHONE---CASE  ") == "phone_case"


def test_exact_normalized_merge_unions_products_and_paths() -> None:
    rows = [
        _market("local_03", "Phone_Case", "path_c", ["P3", "P4"]),
        _market("local_01", "phone-case", "path_a", ["P1", "P2"]),
        _market("local_02", "phone case", "path_b", ["P2", "P3"]),
    ]
    final_rows, audit = merge_exact_normalized_rows(rows)

    assert len(final_rows) == 1
    merged = final_rows[0]
    assert merged["market_id"] == "local_01"
    assert merged["market_label"] == "phone_case"
    assert merged["source_market_ids"] == ["local_01", "local_02", "local_03"]
    assert merged["source_path_ids"] == ["path_a", "path_b", "path_c"]
    assert merged["product_ids"] == ["P1", "P2", "P3", "P4"]
    assert merged["product_count"] == 4
    assert len(audit) == 1
    assert audit[0]["merge_reason"] == "exact_or_format_normalized_name"


def test_merge_is_partition_scoped_and_not_semantic() -> None:
    rows = [
        _market("a", "smart_watch", "p1", ["A"]),
        _market("b", "smartwatch", "p2", ["B"]),
        _market("c", "smart_watch", "p3", ["C"], partition="Sports"),
    ]
    final_rows, audit = merge_exact_normalized_rows(rows)

    assert len(final_rows) == 3
    assert audit == []


def test_materialization_writes_final_market_and_summary(tmp_path: Path) -> None:
    write_market_csv(tmp_path / "first_market.csv", [
        _market("local_b", "phone case", "path_b", ["P2"]),
        _market("local_a", "phone_case", "path_a", ["P1"]),
    ])

    summary = merge_exact_normalized_markets(tmp_path, min_product_count=1)
    final_rows = read_market_csv(tmp_path / "final_market.csv")

    assert summary["merge_policy"] == MERGE_POLICY
    assert summary["llm_merge_used"] is False
    assert summary["local_market_count"] == 2
    assert summary["final_market_count"] == 1
    assert summary["merged_group_count"] == 1
    assert (tmp_path / "final_market.parquet").is_file()
    assert (tmp_path / "cross_path_exact_merge_audit.json").is_file()
    assert final_rows[0]["market_label"] == "phone_case"
    assert final_rows[0]["product_ids"] == ["P1", "P2"]


def test_small_merged_markets_are_dropped_from_final_market(tmp_path: Path) -> None:
    write_market_csv(tmp_path / "first_market.csv", [
        _market("local_small", "tiny_case", "path_a", [f"P{i}" for i in range(8)]),
        _market("local_keep", "big_case", "path_b", [f"Q{i}" for i in range(9)]),
    ])
    summary = merge_exact_normalized_markets(tmp_path)
    final_rows = read_market_csv(tmp_path / "final_market.csv")
    assert summary["merged_market_count"] == 2
    assert summary["final_market_count"] == 1
    assert summary["dropped_small_market_count"] == 1
    assert summary["min_product_count"] == 9
    assert [row["market_label"] for row in final_rows] == ["big_case"]
    assert final_rows[0]["product_count"] == 9
