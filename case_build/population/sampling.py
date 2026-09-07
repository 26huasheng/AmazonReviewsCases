from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


def write_case_user_sample(
    con: duckdb.DuckDBPyConnection,
    eligibility: Path,
    destination: Path,
    copy_atomic,
    *,
    target_users_per_case: int | None,
    seed: str = "case_population_v1",
) -> None:
    """从 pre-t0 合格用户中做确定性抽样。

    当前实现是稳定哈希均匀抽样；relation_stratum 保留在审计表里，未来如果冻结
    分层比例，只替换抽样策略，不需要重算用户历史特征。
    """
    if target_users_per_case is not None and target_users_per_case <= 0:
        raise ValueError("target_users_per_case must be positive or None")
    src = sql_literal(str(eligibility))
    seed_sql = sql_literal(seed)
    where = (
        f"WHERE sample_rank <= {int(target_users_per_case)}"
        if target_users_per_case is not None else ""
    )
    copy_atomic(f"""
        WITH eligible AS (
            SELECT * FROM read_parquet({src}) WHERE eligible_pre_cutoff
        ), ranked AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY case_id
                       ORDER BY sha256(CAST(to_json(list_value(
                           {seed_sql}, case_id, user_id
                       )) AS VARCHAR)), user_id
                   ) AS sample_rank
            FROM eligible
        )
        SELECT case_id,
               market_id,
               source_partition,
               population_cutoff,
               user_id,
               relation_stratum,
               sample_rank::BIGINT
        FROM ranked
        {where}
        ORDER BY case_id, sample_rank
    """, destination)
