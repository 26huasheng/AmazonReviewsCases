#!/usr/bin/env python3
"""Attach Amazon review title/text onto packaged history events.

Review body is bound to users/histories/events.jsonl only.
Does not write text into users.jsonl or products.jsonl.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb


def sql_lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def copy_jsonl(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_suffix(path.suffix + ".part")
    part.unlink(missing_ok=True)
    con.execute(f"COPY ({query}) TO {sql_lit(str(part))} (FORMAT JSON)")
    os.replace(part, path)
    n = 0
    with path.open("rb") as handle:
        for _ in handle:
            n += 1
    return n


def attach_review_text(package_dir: Path, reviews_jsonl: Path) -> dict:
    package_dir = package_dir.expanduser().resolve()
    reviews_jsonl = reviews_jsonl.expanduser().resolve()
    if not package_dir.is_dir():
        raise FileNotFoundError(package_dir)
    if not reviews_jsonl.is_file():
        raise FileNotFoundError(reviews_jsonl)

    glob = str(package_dir / "*" / "users" / "histories" / "events.jsonl")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='200GB'")
    tmp = package_dir / ".duckdb_tmp"
    tmp.mkdir(exist_ok=True)
    con.execute(f"SET temp_directory={sql_lit(str(tmp))}")
    con.execute("SET max_temp_directory_size='80GiB'")

    con.execute(f"""
        CREATE OR REPLACE TABLE hist_events AS
        SELECT
            filename AS source_file,
            regexp_extract(filename, '/([^/]+)/users/histories/events\\.jsonl$', 1)
                AS market_folder_name,
            case_id,
            focal_id,
            user_id,
            CAST(event_date AS DATE) AS event_date,
            try_cast(event_timestamp AS TIMESTAMP) AS event_timestamp,
            product_id,
            rating,
            verified_purchase,
            source_partition
        FROM read_json(
            {sql_lit(glob)},
            format := 'newline_delimited',
            union_by_name := true,
            filename := true
        )
    """)
    n_events = int(con.execute("SELECT count(*) FROM hist_events").fetchone()[0])
    n_users = int(con.execute("SELECT count(DISTINCT user_id) FROM hist_events").fetchone()[0])

    con.execute("""
        CREATE OR REPLACE TABLE needed_users AS
        SELECT DISTINCT user_id FROM hist_events
    """)

    reviews_sql = sql_lit(str(reviews_jsonl))
    con.execute(f"""
        CREATE OR REPLACE TABLE review_raw AS
        SELECT
            r.user_id,
            r.asin,
            r.parent_asin,
            r.timestamp,
            CAST(r.timestamp / 1000 AS BIGINT) AS timestamp_sec,
            NULLIF(trim(CAST(r.title AS VARCHAR)), '') AS review_title,
            NULLIF(trim(CAST(r.text AS VARCHAR)), '') AS review_text,
            r.rating AS review_rating
        FROM read_json(
            {reviews_sql},
            format := 'newline_delimited',
            ignore_errors := true,
            columns := {{
                user_id: 'VARCHAR',
                asin: 'VARCHAR',
                parent_asin: 'VARCHAR',
                timestamp: 'BIGINT',
                title: 'VARCHAR',
                text: 'VARCHAR',
                rating: 'DOUBLE'
            }}
        ) r
        JOIN needed_users u ON r.user_id = u.user_id
    """)

    con.execute("""
        CREATE OR REPLACE TABLE review_by_parent AS
        SELECT *
        FROM (
            SELECT
                user_id,
                parent_asin AS product_id,
                timestamp_sec,
                review_title,
                review_text,
                review_rating,
                row_number() OVER (
                    PARTITION BY user_id, parent_asin, timestamp_sec
                    ORDER BY (review_text IS NOT NULL) DESC,
                             length(coalesce(review_text, '')) DESC
                ) AS rn
            FROM review_raw
            WHERE parent_asin IS NOT NULL AND trim(parent_asin) <> ''
        )
        WHERE rn = 1
    """)
    con.execute("""
        CREATE OR REPLACE TABLE review_by_asin AS
        SELECT *
        FROM (
            SELECT
                user_id,
                asin AS product_id,
                timestamp_sec,
                review_title,
                review_text,
                review_rating,
                row_number() OVER (
                    PARTITION BY user_id, asin, timestamp_sec
                    ORDER BY (review_text IS NOT NULL) DESC,
                             length(coalesce(review_text, '')) DESC
                ) AS rn
            FROM review_raw
            WHERE asin IS NOT NULL AND trim(asin) <> ''
        )
        WHERE rn = 1
    """)

    con.execute("""
        CREATE OR REPLACE TABLE review_by_date AS
        SELECT *
        FROM (
            SELECT
                user_id,
                product_id,
                CAST(to_timestamp(timestamp_sec) AS DATE) AS review_date,
                timestamp_sec,
                review_title,
                review_text,
                review_rating,
                row_number() OVER (
                    PARTITION BY user_id, product_id, CAST(to_timestamp(timestamp_sec) AS DATE)
                    ORDER BY (review_text IS NOT NULL) DESC,
                             length(coalesce(review_text, '')) DESC
                ) AS rn
            FROM (
                SELECT user_id, product_id, timestamp_sec, review_title, review_text, review_rating
                FROM review_by_parent
                UNION ALL
                SELECT user_id, product_id, timestamp_sec, review_title, review_text, review_rating
                FROM review_by_asin
            )
        )
        WHERE rn = 1
    """)
    con.execute("""
        CREATE OR REPLACE TABLE hist_with_text AS
        SELECT
            e.market_folder_name,
            e.case_id,
            e.focal_id,
            e.user_id,
            e.event_date,
            e.event_timestamp,
            e.product_id,
            e.rating,
            e.verified_purchase,
            e.source_partition,
            coalesce(p.review_title, a.review_title, d.review_title) AS review_title,
            coalesce(p.review_text, a.review_text, d.review_text) AS review_text,
            (coalesce(p.review_text, a.review_text, d.review_text) IS NOT NULL)
                AS has_review_text
        FROM hist_events e
        LEFT JOIN review_by_parent p
          ON e.user_id = p.user_id
         AND e.product_id = p.product_id
         AND date_diff('second', TIMESTAMP '1970-01-01', e.event_timestamp) = p.timestamp_sec
        LEFT JOIN review_by_asin a
          ON e.user_id = a.user_id
         AND e.product_id = a.product_id
         AND date_diff('second', TIMESTAMP '1970-01-01', e.event_timestamp) = a.timestamp_sec
        LEFT JOIN review_by_date d
          ON e.user_id = d.user_id
         AND e.product_id = d.product_id
         AND e.event_date = d.review_date
    """)

    stats = con.execute("""
        SELECT
            count(*)::BIGINT AS event_rows,
            count(*) FILTER (has_review_text)::BIGINT AS with_text,
            count(*) FILTER (review_title IS NOT NULL)::BIGINT AS with_title,
            count(*) FILTER (NOT has_review_text)::BIGINT AS missing_text
        FROM hist_with_text
    """).fetchone()
    payload = {
        "event_rows": int(stats[0]),
        "with_review_text": int(stats[1]),
        "with_review_title": int(stats[2]),
        "missing_review_text": int(stats[3]),
        "distinct_history_users": n_users,
        "source_events": n_events,
        "reviews_jsonl": str(reviews_jsonl),
    }

    folders = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT market_folder_name FROM hist_with_text ORDER BY 1"
        ).fetchall()
    ]
    rewritten = 0
    for folder in folders:
        path = package_dir / folder / "users" / "histories" / "events.jsonl"
        rewritten += copy_jsonl(
            con,
            f"""
            SELECT
                case_id,
                focal_id,
                user_id,
                strftime(event_date, '%Y-%m-%d') AS event_date,
                strftime(event_timestamp, '%Y-%m-%dT%H:%M:%S') AS event_timestamp,
                product_id,
                rating,
                verified_purchase,
                source_partition,
                review_title,
                review_text,
                has_review_text
            FROM hist_with_text
            WHERE market_folder_name = {sql_lit(folder)}
            """,
            path,
        )
    payload["rewritten_event_rows"] = rewritten
    payload["markets_rewritten"] = len(folders)

    summary_path = package_dir / "package_summary.json"
    if summary_path.is_file():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["review_text"] = payload
        summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    else:
        summary_path.write_text(json.dumps({"review_text": payload}, indent=2) + "\n", encoding="utf-8")

    con.close()
    return payload


def main() -> None:
    p = argparse.ArgumentParser(description="Bind review text onto history events.jsonl")
    p.add_argument("--package-dir", type=Path, required=True)
    p.add_argument("--reviews-jsonl", type=Path, required=True)
    args = p.parse_args()
    result = attach_review_text(args.package_dir, args.reviews_jsonl)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
