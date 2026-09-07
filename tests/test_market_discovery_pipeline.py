from __future__ import annotations

import json
from pathlib import Path

import duckdb

from market_discovery.discovery_pipeline import MarketDiscoveryPipeline
from market_discovery.discovery_rules import stable_path_id
from market_discovery.market_io import read_market_csv
from market_discovery.market_llm import FixtureMarketLLMClient


def _write_product_core(path: Path) -> None:
    con = duckdb.connect()
    try:
        con.execute("""
            CREATE TABLE product_core(
                source_partition VARCHAR,
                product_id VARCHAR,
                product_title VARCHAR,
                category_path VARCHAR[]
            )
        """)
        con.executemany(
            "INSERT INTO product_core VALUES (?, ?, ?, ?)",
            [
                ("Electronics", "P1", "Clear phone case for iPhone", ["Cases", "A"]),
                ("Electronics", "P2", "Leather phone case", ["Cases", "A"]),
                ("Electronics", "P3", "Protective phone case", ["Cases", "B"]),
                ("Electronics", "P4", "Slim phone case", ["Cases", "B"]),
            ],
        )
        con.execute("COPY product_core TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        con.close()


def test_fixture_discovery_materializes_final_market(tmp_path: Path) -> None:
    product_core = tmp_path / "product_core.parquet"
    _write_product_core(product_core)

    path_a = stable_path_id("Electronics", ["Cases", "A"])
    path_b = stable_path_id("Electronics", ["Cases", "B"])
    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps({
        "responses": {
            f"{path_a}:round1": {
                "decision": "KEEP",
                "markets": [{
                    "market_label": "Phone_Case",
                    "center_term": "phone case",
                    "equivalent_terms": [],
                    "support_terms": [],
                    "confidence": 0.95,
                }],
            },
            f"{path_b}:round1": {
                "decision": "KEEP",
                "markets": [{
                    "market_label": "phone-case",
                    "center_term": "phone case",
                    "equivalent_terms": [],
                    "support_terms": [],
                    "confidence": 0.95,
                }],
            },
        },
    }), encoding="utf-8")

    pipeline = MarketDiscoveryPipeline(
        product_core=product_core,
        output_root=tmp_path / "out",
        discovery_version="test_v1",
        source_partition="Electronics",
        min_final_market_product_count=1,
    )
    try:
        summary = pipeline.prepare_local_evidence()
        assert summary["discovery_path_count"] == 2
        result = pipeline.run_discovery(
            FixtureMarketLLMClient(fixture),
            max_paths=None,
            resume=False,
            llm_workers=1,
        )
        discovery_dir = pipeline.output_dir
    finally:
        pipeline.close()

    first = read_market_csv(discovery_dir / "first_market.csv")
    final = read_market_csv(discovery_dir / "final_market.csv")

    assert len(first) == 2
    assert len(final) == 1
    assert final[0]["market_label"] == "phone_case"
    assert final[0]["product_ids"] == ["P1", "P2", "P3", "P4"]
    assert result["exact_name_merge"]["llm_merge_used"] is False
    assert result["exact_name_merge"]["merged_group_count"] == 1
