from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

# Electronics first_market rows can have product_id arrays larger than the
# default 128KiB csv field limit.
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

import duckdb

from utils import sql_literal


MARKET_TABLE_FIELDS = (
    "discovery_version",
    "source_partition",
    "market_id",
    "market_label",
    "source_market_ids",
    "source_path_ids",
    "source_category_paths",
    "product_ids",
    "product_count",
)

LIST_FIELDS = (
    "source_market_ids",
    "source_path_ids",
    "source_category_paths",
    "product_ids",
)


def encode_list_field(value: Any) -> str:
    if value is None:
        return "[]"
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            json.loads(text)
            return text
        return json.dumps([item for item in text.split("|") if item], ensure_ascii=False)
    items = list(value)
    normalized = []
    for item in items:
        if isinstance(item, (list, tuple)):
            normalized.append(list(item))
        else:
            normalized.append(item)
    return json.dumps(normalized, ensure_ascii=False)


def decode_list_field(value: Any) -> list[Any]:
    if value is None or (isinstance(value, float) and value != value):
        return []
    if isinstance(value, list):
        return value
    text = str(value).strip()
    if not text:
        return []
    parsed = json.loads(text)
    if not isinstance(parsed, list):
        raise ValueError(f"list field must be a JSON array, got {text!r}")
    return parsed


def validate_market_row(row: dict[str, Any], *, source_partition: str | None = None) -> dict[str, Any]:
    missing = [name for name in MARKET_TABLE_FIELDS if name not in row]
    if missing:
        raise ValueError(f"market table missing columns: {missing}")
    market_id = str(row["market_id"] or "").strip()
    if not market_id:
        raise ValueError("market_id must be non-empty")
    partition = str(row["source_partition"] or "").strip()
    if source_partition is not None and partition != source_partition:
        raise ValueError(f"source_partition={partition!r}; expected {source_partition!r}")
    source_market_ids = [str(item) for item in decode_list_field(row["source_market_ids"])]
    source_path_ids = [str(item) for item in decode_list_field(row["source_path_ids"])]
    source_category_paths = decode_list_field(row["source_category_paths"])
    product_ids = [str(item) for item in decode_list_field(row["product_ids"])]
    if market_id not in source_market_ids:
        raise ValueError(
            f"market {market_id}: market_id must be one of source_market_ids={source_market_ids}"
        )
    count = int(row["product_count"] or 0)
    unique_products = list(dict.fromkeys(product_ids))
    if count != len(unique_products):
        raise ValueError(
            f"market {market_id}: product_count={count} != unique product_ids={len(unique_products)}"
        )
    return {
        "discovery_version": str(row["discovery_version"]),
        "source_partition": partition,
        "market_id": market_id,
        "market_label": str(row["market_label"] or ""),
        "source_market_ids": source_market_ids,
        "source_path_ids": source_path_ids,
        "source_category_paths": source_category_paths,
        "product_ids": unique_products,
        "product_count": len(unique_products),
    }


def read_market_csv(path: Path, *, source_partition: str | None = None) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or list(reader.fieldnames) != list(MARKET_TABLE_FIELDS):
            raise ValueError(
                f"{path} columns {list(reader.fieldnames or [])} != {list(MARKET_TABLE_FIELDS)}"
            )
        rows = [validate_market_row(row, source_partition=source_partition) for row in reader]
    ids = [row["market_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate market_id in market table")
    return rows


def write_market_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(MARKET_TABLE_FIELDS))
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "discovery_version": row["discovery_version"],
                "source_partition": row["source_partition"],
                "market_id": row["market_id"],
                "market_label": row["market_label"],
                "source_market_ids": encode_list_field(row["source_market_ids"]),
                "source_path_ids": encode_list_field(row["source_path_ids"]),
                "source_category_paths": encode_list_field(row["source_category_paths"]),
                "product_ids": encode_list_field(row["product_ids"]),
                "product_count": int(row["product_count"]),
            })


def write_market_parquet_from_relation(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(path.name + ".part")
    part.unlink(missing_ok=True)
    con.execute(
        f"COPY ({query}) TO {sql_literal(str(part))} (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    part.replace(path)


def parquet_to_csv(con: duckdb.DuckDBPyConnection, parquet_path: Path, csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    part = csv_path.with_name(csv_path.name + ".part")
    part.unlink(missing_ok=True)
    con.execute(f"""
        COPY (
            SELECT discovery_version, source_partition, market_id, market_label,
                   to_json(source_market_ids) AS source_market_ids,
                   to_json(source_path_ids) AS source_path_ids,
                   to_json(source_category_paths) AS source_category_paths,
                   to_json(product_ids) AS product_ids,
                   product_count
            FROM read_parquet({sql_literal(str(parquet_path))})
        ) TO {sql_literal(str(part))} (FORMAT CSV, HEADER true)
    """)
    part.replace(csv_path)


def csv_to_parquet(con: duckdb.DuckDBPyConnection, csv_path: Path, parquet_path: Path,
                   rows: Iterable[dict[str, Any]] | None = None) -> None:
    payload = list(rows) if rows is not None else read_market_csv(csv_path)
    con.execute("DROP TABLE IF EXISTS market_table_load")
    con.execute("""
        CREATE TEMP TABLE market_table_load (
            discovery_version VARCHAR,
            source_partition VARCHAR,
            market_id VARCHAR,
            market_label VARCHAR,
            source_market_ids VARCHAR[],
            source_path_ids VARCHAR[],
            source_category_paths VARCHAR[][],
            product_ids VARCHAR[],
            product_count BIGINT
        )
    """)
    con.executemany(
        "INSERT INTO market_table_load VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                row["discovery_version"], row["source_partition"], row["market_id"],
                row["market_label"], row["source_market_ids"], row["source_path_ids"],
                row["source_category_paths"], row["product_ids"], row["product_count"],
            )
            for row in payload
        ],
    )
    write_market_parquet_from_relation(con, "SELECT * FROM market_table_load", parquet_path)
    con.execute("DROP TABLE IF EXISTS market_table_load")
