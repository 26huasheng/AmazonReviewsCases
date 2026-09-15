from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from utils import sql_literal, write_json


def _columns(con: duckdb.DuckDBPyConnection, path: Path) -> set[str]:
    return {
        str(row[0])
        for row in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
    }


class ExternalSignalsPipeline:
    """把 provider-agnostic 历史价格 / BSR 表连接到 Case shelf。

    具体 Keepa API 获取留在 provider adapter；本层只定义稳定的 benchmark 接口。
    """

    def __init__(
        self,
        cases: Path,
        case_shelf: Path,
        external_history: Path,
        output_dir: Path,
    ) -> None:
        self.cases = cases.expanduser().resolve()
        self.case_shelf = case_shelf.expanduser().resolve()
        self.external_history = external_history.expanduser().resolve()
        self.output_dir = output_dir.expanduser().resolve()
        for path in (self.cases, self.case_shelf, self.external_history):
            if not path.is_file():
                raise FileNotFoundError(path)
        self.product_signals_path = self.output_dir / "case_product_external_signals.parquet"
        self.case_signals_path = self.output_dir / "case_external_signals.parquet"
        self.enriched_shelf_path = self.output_dir / "case_shelf_with_external.parquet"
        self.summary_path = self.output_dir / "external_signals_summary.json"
        self.con = duckdb.connect()
        self.con.execute("SET TimeZone='UTC'")
        self.con.execute("SET preserve_insertion_order=false")
        temp = self.output_dir / ".duckdb_tmp"
        temp.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"SET temp_directory={sql_literal(str(temp))}")

    def close(self) -> None:
        self.con.close()

    def _copy(self, query: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        part = path.with_suffix(path.suffix + ".part")
        part.unlink(missing_ok=True)
        self.con.execute(
            f"COPY ({query}) TO {sql_literal(str(part))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        os.replace(part, path)

    def _canonical_history_view(self) -> None:
        cols = _columns(self.con, self.external_history)
        required = {"source_partition", "product_id"}
        missing = required - cols
        if missing:
            raise ValueError(f"external history missing columns: {sorted(missing)}")
        if "event_timestamp" in cols:
            ts = "try_cast(event_timestamp AS TIMESTAMP)"
        elif "timestamp" in cols:
            ts = "try_cast(timestamp AS TIMESTAMP)"
        elif "event_date" in cols:
            ts = "try_cast(event_date AS TIMESTAMP)"
        elif "date" in cols:
            ts = "try_cast(date AS TIMESTAMP)"
        else:
            raise ValueError("external history requires event_timestamp/timestamp/event_date/date")
        price = "try_cast(price AS DOUBLE)" if "price" in cols else "NULL::DOUBLE"
        if "bsr" in cols:
            bsr = "try_cast(bsr AS DOUBLE)"
        elif "sales_rank" in cols:
            bsr = "try_cast(sales_rank AS DOUBLE)"
        else:
            bsr = "NULL::DOUBLE"
        src = sql_literal(str(self.external_history))
        self.con.execute(f"""
            CREATE OR REPLACE TEMP VIEW canonical_external_history AS
            SELECT CAST(source_partition AS VARCHAR) AS source_partition,
                   CAST(product_id AS VARCHAR) AS product_id,
                   {ts} AS event_timestamp,
                   CAST({ts} AS DATE) AS event_date,
                   {price} AS price,
                   {bsr} AS bsr
            FROM read_parquet({src})
            WHERE product_id IS NOT NULL AND {ts} IS NOT NULL
        """)

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._canonical_history_view()
        c = sql_literal(str(self.cases))
        s = sql_literal(str(self.case_shelf))

        self._copy(f"""
            WITH base AS (
                SELECT c.case_candidate_id,
                       c.source_partition,
                       c.evaluation_start,
                       c.evaluation_end_exclusive,
                       c.t0,
                       s.product_id,
                       s.role
                FROM read_parquet({c}) c
                JOIN read_parquet({s}) s USING(case_candidate_id)
            ), t0_snapshot AS (
                SELECT b.*,
                       h.event_timestamp AS external_snapshot_timestamp,
                       h.price AS price_at_t0,
                       h.bsr AS bsr_at_t0
                FROM base b
                ASOF LEFT JOIN canonical_external_history h
                  ON b.source_partition=h.source_partition
                 AND b.product_id=h.product_id
                 AND h.event_timestamp <= CAST(b.t0 AS TIMESTAMP)
            ), future AS (
                SELECT b.case_candidate_id,
                       b.product_id,
                       count(h.bsr)::BIGINT AS future_bsr_observation_count,
                       median(h.bsr)::DOUBLE AS future_bsr_median,
                       min(h.bsr)::DOUBLE AS future_bsr_best,
                       avg(h.price)::DOUBLE AS future_price_mean
                FROM base b
                LEFT JOIN canonical_external_history h
                  ON b.source_partition=h.source_partition
                 AND b.product_id=h.product_id
                 AND h.event_timestamp >= CAST(b.evaluation_start AS TIMESTAMP)
                 AND h.event_timestamp < CAST(b.evaluation_end_exclusive AS TIMESTAMP)
                GROUP BY b.case_candidate_id, b.product_id
            ), joined AS (
                SELECT t.*, f.future_bsr_observation_count,
                       f.future_bsr_median, f.future_bsr_best, f.future_price_mean
                FROM t0_snapshot t
                LEFT JOIN future f USING(case_candidate_id, product_id)
            )
            SELECT *,
                   CASE WHEN future_bsr_median IS NULL THEN NULL
                        ELSE row_number() OVER (
                            PARTITION BY case_candidate_id
                            ORDER BY future_bsr_median ASC NULLS LAST, product_id
                        ) END AS external_future_bsr_rank
            FROM joined
            ORDER BY case_candidate_id, role DESC, product_id
        """, self.product_signals_path)

        self._copy(f"""
            SELECT case_candidate_id,
                   count(*)::BIGINT AS external_shelf_product_count,
                   count(price_at_t0)::BIGINT AS price_at_t0_covered_products,
                   count(bsr_at_t0)::BIGINT AS bsr_at_t0_covered_products,
                   count(future_bsr_median)::BIGINT AS future_bsr_covered_products,
                   avg(CASE WHEN price_at_t0 IS NOT NULL THEN 1.0 ELSE 0.0 END)::DOUBLE
                       AS price_at_t0_coverage,
                   avg(CASE WHEN bsr_at_t0 IS NOT NULL THEN 1.0 ELSE 0.0 END)::DOUBLE
                       AS bsr_at_t0_coverage,
                   avg(CASE WHEN future_bsr_median IS NOT NULL THEN 1.0 ELSE 0.0 END)::DOUBLE
                       AS future_bsr_coverage,
                   max(price_at_t0) FILTER (role='focal') AS focal_price_at_t0,
                   max(bsr_at_t0) FILTER (role='focal') AS focal_bsr_at_t0,
                   max(future_bsr_median) FILTER (role='focal') AS focal_future_bsr_median,
                   max(external_future_bsr_rank) FILTER (role='focal')::BIGINT
                       AS focal_external_future_bsr_rank
            FROM read_parquet({sql_literal(str(self.product_signals_path))})
            GROUP BY case_candidate_id
            ORDER BY case_candidate_id
        """, self.case_signals_path)

        self._copy(f"""
            SELECT s.* EXCLUDE(price_at_t0, price_source),
                   coalesce(e.price_at_t0, s.price_at_t0) AS price_at_t0,
                   CASE WHEN e.price_at_t0 IS NOT NULL THEN 'external_history'
                        ELSE s.price_source END AS price_source,
                   e.bsr_at_t0,
                   e.external_snapshot_timestamp
            FROM read_parquet({s}) s
            LEFT JOIN read_parquet({sql_literal(str(self.product_signals_path))}) e
              USING(case_candidate_id, product_id)
            ORDER BY s.case_candidate_id,
                     CASE WHEN s.role='focal' THEN 0 ELSE 1 END,
                     s.product_id
        """, self.enriched_shelf_path)

        payload = {
            "status": "COMPLETE",
            "schema_version": "external_signals_v1",
            "external_history": str(self.external_history),
            "case_count": int(self.con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(self.case_signals_path)]
            ).fetchone()[0]),
            "provider_specific_acquisition_included": False,
            "quality_interface": "case_external_signals.parquet",
            "shelf_price_interface": "case_shelf_with_external.parquet",
        }
        write_json(self.summary_path, payload)
        return payload
