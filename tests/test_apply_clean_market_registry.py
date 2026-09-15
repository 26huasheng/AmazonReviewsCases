from __future__ import annotations

import csv
import hashlib
import importlib.util
from pathlib import Path

import duckdb

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "apply_clean_market_registry.py"


def _load():
    spec = importlib.util.spec_from_file_location("apply_clean_market_registry", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _copy(path: Path, ddl: str, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TABLE value {ddl}")
        if rows:
            placeholders = ",".join("?" for _ in rows[0])
            con.executemany(f"INSERT INTO value VALUES ({placeholders})", rows)
        con.execute("COPY value TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        con.close()


def _write_quality(root: Path) -> None:
    _copy(root / "accepted_cases.parquet", """(
        case_id VARCHAR, source_partition VARCHAR, market_id VARCHAR, market_label VARCHAR
    )""", [
        ("C_clean", "Electronics", "M1", "laptop"),
        ("C_dirty", "Electronics", "M2", "electronics"),
        ("C_ghost", "Electronics", "M3", "not_in_quality_wait"),
    ])
    _copy(root / "accepted_focals.parquet", """(
        case_id VARCHAR, focal_id VARCHAR, focal_product_id VARCHAR
    )""", [
        ("C_clean", "F1", "P1"),
        ("C_dirty", "F2", "P2"),
    ])
    _copy(root / "accepted_focal_competitors.parquet", """(
        case_id VARCHAR, focal_id VARCHAR, competitor_product_id VARCHAR
    )""", [
        ("C_clean", "F1", "C1"),
        ("C_dirty", "F2", "C2"),
    ])
    _copy(root / "accepted_case_shelf.parquet", """(
        case_id VARCHAR, product_id VARCHAR
    )""", [
        ("C_clean", "P1"),
        ("C_dirty", "P2"),
    ])
    _copy(root / "quality_focal_metrics.parquet", """(
        case_id VARCHAR, focal_id VARCHAR, gt1_user_count BIGINT
    )""", [
        ("C_clean", "F1", 30),
        ("C_dirty", "F2", 40),
    ])
    _copy(root / "quality_case_decisions.parquet", """(case_id VARCHAR)""", [("C_clean",), ("C_dirty",)])
    _copy(root / "quality_case_metrics.parquet", """(case_id VARCHAR)""", [("C_clean",), ("C_dirty",)])
    _copy(root / "quality_focal_decisions.parquet", """(
        case_id VARCHAR, focal_id VARCHAR
    )""", [("C_clean", "F1"), ("C_dirty", "F2")])


def _write_registry(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "source_partition", "market_id", "market_name", "audit_status", "provenance",
        "product_count", "n_paths", "n_source_markets",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_apply_drops_non_clean_and_keeps_clean(tmp_path: Path) -> None:
    quality = tmp_path / "quality"
    _write_quality(quality)
    registry = tmp_path / "registry.csv"
    _write_registry(registry, [
        {
            "source_partition": "Electronics",
            "market_id": "M1",
            "market_name": "laptop",
            "audit_status": "CLEAN",
            "provenance": "manual_audit",
            "product_count": 10,
            "n_paths": 1,
            "n_source_markets": 1,
        },
        {
            "source_partition": "Electronics",
            "market_id": "M9",
            "market_name": "orphan_clean",
            "audit_status": "CLEAN",
            "provenance": "manual_audit",
            "product_count": 3,
            "n_paths": 1,
            "n_source_markets": 1,
        },
    ])
    out = tmp_path / "out"
    summary = _load().apply_registry(quality, registry, out)
    con = duckdb.connect()
    try:
        cases = con.execute(
            "SELECT case_id FROM read_parquet(?) ORDER BY 1",
            [str(out / "accepted_cases.parquet")],
        ).fetchall()
        assert cases == [("C_clean",)]
        focals = con.execute(
            "SELECT case_id, focal_id FROM read_parquet(?)",
            [str(out / "accepted_focals.parquet")],
        ).fetchall()
        assert focals == [("C_clean", "F1")]
        comps = con.execute(
            "SELECT competitor_product_id FROM read_parquet(?)",
            [str(out / "accepted_focal_competitors.parquet")],
        ).fetchall()
        assert comps == [("C1",)]
        shelf = con.execute(
            "SELECT product_id FROM read_parquet(?)",
            [str(out / "accepted_case_shelf.parquet")],
        ).fetchall()
        assert shelf == [("P1",)]
        leftover = con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE case_id='C_dirty'",
            [str(out / "accepted_focals.parquet")],
        ).fetchone()[0]
        assert leftover == 0
        markets = con.execute(
            "SELECT market_id, market_label FROM read_parquet(?) ORDER BY market_id",
            [str(out / "clean_markets.parquet")],
        ).fetchall()
        assert markets == [("M1", "laptop"), ("M9", "orphan_clean")]
        payload = con.execute(
            "SELECT gt1_user_count FROM read_parquet(?)",
            [str(out / "quality_focal_metrics.parquet")],
        ).fetchone()[0]
        assert payload == 30
    finally:
        con.close()
    assert summary["registry_row_count"] == 2
    assert summary["accepted_case_count"] == 1
    assert summary["accepted_focal_count"] == 1
    assert summary["match_key"] == "market_name"


def test_registry_does_not_create_markets_or_mutate_rows(tmp_path: Path) -> None:
    quality = tmp_path / "quality"
    _write_quality(quality)
    registry = tmp_path / "registry.csv"
    _write_registry(registry, [{
        "source_partition": "Electronics",
        "market_id": "M1",
        "market_name": "laptop",
        "audit_status": "CLEAN",
        "provenance": "manual_audit",
        "product_count": 10,
        "n_paths": 1,
        "n_source_markets": 1,
    }])
    out = tmp_path / "out"
    _load().apply_registry(quality, registry, out)
    con = duckdb.connect()
    try:
        before = con.execute(
            "SELECT * FROM read_parquet(?) WHERE case_id='C_clean'",
            [str(quality / "accepted_cases.parquet")],
        ).fetchall()
        after = con.execute(
            "SELECT * FROM read_parquet(?)",
            [str(out / "accepted_cases.parquet")],
        ).fetchall()
        assert after == before
        assert con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE case_id='C_ghost'",
            [str(out / "accepted_cases.parquet")],
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_name_based_match_and_deterministic_replay(tmp_path: Path) -> None:
    quality = tmp_path / "quality"
    _write_quality(quality)
    registry = tmp_path / "registry.csv"
    _write_registry(registry, [{
        "source_partition": "Electronics",
        "market_id": "DIFFERENT_ID",
        "market_name": "laptop",
        "audit_status": "CLEAN",
        "provenance": "manual_audit",
        "product_count": 1,
        "n_paths": 1,
        "n_source_markets": 1,
    }])
    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    apply = _load().apply_registry
    apply(quality, registry, out1)
    apply(quality, registry, out2)
    con = duckdb.connect()
    try:
        cases = con.execute(
            "SELECT case_id, market_id FROM read_parquet(?)",
            [str(out1 / "accepted_cases.parquet")],
        ).fetchall()
        # Name matches even if registry market_id differs.
        assert cases == [("C_clean", "M1")]
        h1 = hashlib.sha256((out1 / "accepted_cases.parquet").read_bytes()).hexdigest()
        h2 = hashlib.sha256((out2 / "accepted_cases.parquet").read_bytes()).hexdigest()
        assert h1 == h2
    finally:
        con.close()
