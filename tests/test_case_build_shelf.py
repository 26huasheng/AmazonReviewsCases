from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from case_build.pipeline import CaseShelfBuilder


PARTITION = "Electronics"
VERSION = "market_v1"
MARKET = "M1"


def _copy_rows(path: Path, ddl: str, rows: list[tuple]) -> None:
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


def _write_cases(path: Path) -> None:
    _copy_rows(path, """(
        case_id VARCHAR,
        source_partition VARCHAR,
        market_id VARCHAR,
        market_label VARCHAR,
        time_box_id VARCHAR,
        n_focals BIGINT
    )""", [
        ("C1", PARTITION, MARKET, "dog_collar", "2022-H1", 1),
        ("C2", PARTITION, MARKET, "dog_collar", "2022-H2", 1),
    ])


def _write_focals(path: Path) -> None:
    _copy_rows(path, """(
        case_id VARCHAR,
        focal_id VARCHAR,
        focal_product_id VARCHAR,
        t0 DATE
    )""", [
        ("C1", "F1", "X", date(2022, 1, 1)),
        ("C2", "F2", "Y", date(2022, 7, 1)),
    ])


def _write_timeline(path: Path) -> None:
    _copy_rows(path, """(
        source_partition VARCHAR,
        discovery_version VARCHAR,
        market_id VARCHAR,
        market_label VARCHAR,
        product_id VARCHAR,
        product_title VARCHAR,
        category_path VARCHAR[],
        first_available_date VARCHAR,
        metadata_snapshot_price DOUBLE,
        entry_date DATE,
        first_rating_date DATE,
        last_rating_date DATE,
        total_rating_count BIGINT,
        post90_rating_count BIGINT,
        entry_date_source VARCHAR
    )""", [
        (PARTITION, VERSION, MARKET, "dog_collar", "X", "X", ["Pet"], None, 30.0,
         date(2022, 1, 1), date(2022, 1, 1), date(2023, 1, 1), 10, 5, "first_rating_date"),
        (PARTITION, VERSION, MARKET, "dog_collar", "Y", "Y", ["Pet"], None, 40.0,
         date(2022, 7, 1), date(2022, 7, 1), date(2023, 1, 1), 10, 5, "first_rating_date"),
        (PARTITION, VERSION, MARKET, "dog_collar", "A", "A", ["Pet"], None, 10.0,
         date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 100, 20, "first_rating_date"),
        (PARTITION, VERSION, MARKET, "dog_collar", "SAME", "Same day", ["Pet"], None, 11.0,
         date(2022, 1, 1), date(2022, 1, 1), date(2023, 1, 1), 20, 10, "first_rating_date"),
        (PARTITION, VERSION, MARKET, "dog_collar", "EDGE", "Edge", ["Pet"], None, 12.0,
         date(2020, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 20, 10, "first_rating_date"),
        (PARTITION, VERSION, MARKET, "dog_collar", "GONE", "Gone", ["Pet"], None, 13.0,
         date(2020, 1, 1), date(2020, 1, 1), date(2021, 12, 31), 20, 10, "first_rating_date"),
    ])


def test_shelf_time_boundaries_and_pre_t0_features(tmp_path: Path) -> None:
    cases = tmp_path / "cases.parquet"
    focals = tmp_path / "case_focals.parquet"
    timeline = tmp_path / "timeline.parquet"
    daily = tmp_path / "daily.parquet"
    _write_cases(cases)
    _write_focals(focals)
    _write_timeline(timeline)

    t0 = date(2022, 1, 1)
    window_start = t0 - timedelta(days=120)
    _copy_rows(daily, """(
        source_partition VARCHAR,
        product_id VARCHAR,
        event_date DATE,
        rating_count BIGINT,
        rating_sum DOUBLE
    )""", [
        # A: window 起点当天计入，t0 当天不计入。
        (PARTITION, "A", window_start, 5, 20.0),
        (PARTITION, "A", t0 - timedelta(days=1), 5, 25.0),
        (PARTITION, "A", t0, 100, 500.0),
        (PARTITION, "A", date(2022, 6, 30), 2, 10.0),
        (PARTITION, "X", t0, 1, 5.0),
        (PARTITION, "X", date(2022, 2, 1), 2, 8.0),
        (PARTITION, "Y", date(2022, 7, 1), 1, 5.0),
        (PARTITION, "SAME", date(2022, 1, 1), 1, 4.0),
        (PARTITION, "SAME", date(2022, 6, 1), 1, 4.0),
        (PARTITION, "EDGE", date(2021, 12, 1), 1, 3.0),
        (PARTITION, "GONE", date(2021, 12, 1), 1, 3.0),
    ])

    out = tmp_path / "out"
    builder = CaseShelfBuilder(cases, focals, timeline, daily, out)
    try:
        summary = builder.run()
    finally:
        builder.close()

    assert summary["shelf_truncation_applied"] is False
    assert summary["activity_threshold_applied"] is False

    con = duckdb.connect()
    try:
        c1 = con.execute("""
            SELECT product_id, is_focal, is_competitor, metadata_snapshot_price
            FROM read_parquet(?)
            WHERE case_id='C1'
            ORDER BY product_id
        """, [str(out / "case_shelf.parquet")]).fetchall()
        feats = con.execute("""
            SELECT competitor_product_id,
                   pre_t0_review_count,
                   pre_t0_recent_review_count,
                   pre_t0_rating_mean,
                   competitor_selected
            FROM read_parquet(?)
            WHERE case_id='C1'
            ORDER BY competitor_product_id
        """, [str(out / "focal_competitors.parquet")]).fetchall()
        c2_ids = {
            row[0]
            for row in con.execute("""
                SELECT product_id
                FROM read_parquet(?)
                WHERE case_id='C2'
            """, [str(out / "case_shelf.parquet")]).fetchall()
        }
    finally:
        con.close()

    by_id = {row[0]: row[1:] for row in c1}
    feat = {row[0]: row[1:] for row in feats}
    # SAME 与 focal X 同日首评，所以 C1 不算既有 competitor。
    assert "SAME" not in by_id
    # EDGE 最后评论日正好等于 t0，仍算 t0 当日活跃。
    assert "EDGE" in by_id
    assert "GONE" not in by_id
    assert by_id["X"][0] is True
    assert by_id["X"][1] is False
    # A 的 t0 前窗口只有两笔各 5 条；t0 当天 100 条不应进入历史。
    assert feat["A"][0] == 10
    assert feat["A"][1] == 10
    assert feat["A"][2] == 4.5
    assert by_id["A"][2] == 10.0
    # X 在较晚的 C2 中自然成为 competitor；SAME 也已经早于 C2。
    assert {"X", "SAME", "A", "Y"}.issubset(c2_ids)


def test_shelf_caps_competitors_at_16(tmp_path: Path) -> None:
    t0 = date(2022, 1, 1)
    cases = tmp_path / "cases.parquet"
    focals = tmp_path / "case_focals.parquet"
    timeline = tmp_path / "timeline.parquet"
    daily = tmp_path / "daily.parquet"
    _copy_rows(cases, """(
        case_id VARCHAR,
        source_partition VARCHAR,
        market_id VARCHAR,
        market_label VARCHAR,
        time_box_id VARCHAR,
        n_focals BIGINT
    )""", [("C1", PARTITION, MARKET, "large_market", "2022-H1", 1)])
    _copy_rows(focals, """(
        case_id VARCHAR,
        focal_id VARCHAR,
        focal_product_id VARCHAR,
        t0 DATE
    )""", [("C1", "F1", "F", t0)])

    competitors = [f"P{i:03d}" for i in range(151)]
    timeline_rows = [
        (PARTITION, VERSION, MARKET, "large_market", "F", "F", ["Root"], None, None,
         t0, t0, date(2023, 1, 1), 1, 1, "first_rating_date")
    ] + [
        (PARTITION, VERSION, MARKET, "large_market", pid, pid, ["Root"], None, None,
         date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 1, 1,
         "first_rating_date")
        for pid in competitors
    ]
    _copy_rows(timeline, """(
        source_partition VARCHAR,
        discovery_version VARCHAR,
        market_id VARCHAR,
        market_label VARCHAR,
        product_id VARCHAR,
        product_title VARCHAR,
        category_path VARCHAR[],
        first_available_date VARCHAR,
        metadata_snapshot_price DOUBLE,
        entry_date DATE,
        first_rating_date DATE,
        last_rating_date DATE,
        total_rating_count BIGINT,
        post90_rating_count BIGINT,
        entry_date_source VARCHAR
    )""", timeline_rows)
    _copy_rows(daily, """(
        source_partition VARCHAR,
        product_id VARCHAR,
        event_date DATE,
        rating_count BIGINT,
        rating_sum DOUBLE
    )""", [
        (PARTITION, pid, date(2021, 12, 1), 1, 5.0)
        for pid in competitors
    ] + [(PARTITION, "F", t0, 1, 5.0)])

    out = tmp_path / "out"
    builder = CaseShelfBuilder(cases, focals, timeline, daily, out)
    try:
        summary = builder.run()
    finally:
        builder.close()

    assert summary["max_shelf_products_per_case"] == 17
    con = duckdb.connect()
    try:
        selected = con.execute("""
            SELECT count(*) FILTER (competitor_selected)
            FROM read_parquet(?)
        """, [str(out / "focal_competitors.parquet")]).fetchone()[0]
    finally:
        con.close()
    assert selected == 16
