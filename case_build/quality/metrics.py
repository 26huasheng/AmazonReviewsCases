from __future__ import annotations

from pathlib import Path

import duckdb

from utils import sql_literal


def write_focal_quality_metrics(
    con: duckdb.DuckDBPyConnection,
    cases: Path,
    case_focals: Path,
    case_shelf: Path,
    focal_competitors: Path,
    choice_truth: Path,
    destination: Path,
    copy_atomic,
    *,
    rules: dict,
    gt1_users: Path | None = None,
    gt1_raw_outcomes: Path | None = None,
    gt1_shelf_events: Path | None = None,
    timeline: Path | None = None,
    review_activity_truth: Path | None = None,
) -> None:
    c = sql_literal(str(cases))
    f = sql_literal(str(case_focals))
    s = sql_literal(str(case_shelf))
    fc = sql_literal(str(focal_competitors))
    g1 = sql_literal(str(choice_truth))
    min_hist = int(rules["min_history_product_count"])
    max_recency = int(rules["max_days_since_last_event"])

    if timeline is not None:
        tl = sql_literal(str(timeline))
        time_join = f"""
            LEFT JOIN (
                SELECT market_id, product_id,
                       first_rating_date AS tl_first_rating_date,
                       last_rating_date AS tl_last_rating_date
                FROM read_parquet({tl})
            ) tl
              ON tl.market_id = focals.market_id
             AND tl.product_id = sel.competitor_product_id
        """
        time_ok = """
            sel.first_rating_date < focals.t0
            AND sel.last_rating_date >= focals.t0
            AND (
                tl.tl_first_rating_date IS NULL
                OR (
                    tl.tl_first_rating_date < focals.t0
                    AND tl.tl_last_rating_date >= focals.t0
                )
            )
        """
    else:
        time_join = ""
        time_ok = """
            sel.first_rating_date < focals.t0
            AND sel.last_rating_date >= focals.t0
        """

    if gt1_shelf_events is not None:
        se = sql_literal(str(gt1_shelf_events))
        shelf_events_cte = f"""
            shelf_first AS (
                SELECT case_id, focal_id, user_id,
                       min(event_timestamp) AS first_event_timestamp,
                       count(*)::BIGINT AS raw_event_count
                FROM read_parquet({se})
                GROUP BY case_id, focal_id, user_id
            ),
        """
        shelf_events_join = """
            LEFT JOIN shelf_first sf
              ON ct.case_id = sf.case_id
             AND ct.focal_id = sf.focal_id
             AND ct.user_id = sf.user_id
        """
        trace_fail = "sf.user_id IS NULL OR sf.first_event_timestamp IS DISTINCT FROM ct.event_timestamp"
        reducer_fail = "coalesce(sf.raw_event_count, 0) < 1"
    else:
        shelf_events_cte = ""
        shelf_events_join = ""
        trace_fail = "FALSE"
        reducer_fail = "FALSE"

    if gt1_raw_outcomes is not None:
        raw = sql_literal(str(gt1_raw_outcomes))
        raw_cte = f"""
            raw_users AS (
                SELECT case_id, focal_id, user_id, count(*)::BIGINT AS raw_rows
                FROM read_parquet({raw})
                GROUP BY case_id, focal_id, user_id
            ),
        """
        raw_join = """
            LEFT JOIN raw_users ru
              ON ct.case_id = ru.case_id
             AND ct.focal_id = ru.focal_id
             AND ct.user_id = ru.user_id
        """
        raw_dup_fail = "coalesce(ru.raw_rows, 0) <> 1"
    else:
        raw_cte = ""
        raw_join = ""
        raw_dup_fail = "FALSE"

    if gt1_users is not None:
        gu = sql_literal(str(gt1_users))
        users_cte = f"""
            gt1_user_rows AS (
                SELECT case_id, focal_id,
                       count(*)::BIGINT AS gt1_users_table_rows,
                       count(DISTINCT user_id)::BIGINT AS gt1_users_table_distinct
                FROM read_parquet({gu})
                GROUP BY case_id, focal_id
            ),
        """
        users_join = """
            LEFT JOIN gt1_user_rows gur
              ON gur.case_id = f.case_id AND gur.focal_id = f.focal_id
        """
        users_select = """
            coalesce(gur.gt1_users_table_distinct, 0)::BIGINT AS gt1_users_table_distinct,
        """
        users_complete = """
            coalesce(gt1.gt1_user_count, 0) = coalesce(gur.gt1_users_table_distinct, 0)
        """
    else:
        users_cte = ""
        users_join = ""
        users_select = "NULL::BIGINT AS gt1_users_table_distinct,"
        users_complete = "TRUE"

    review_join = ""
    review_select = (
        "NULL::BIGINT AS review_activity_count, NULL::BIGINT AS review_activity_rank"
    )
    if review_activity_truth is not None:
        rt = sql_literal(str(review_activity_truth))
        review_join = f"""
            LEFT JOIN (
                SELECT case_id, focal_id,
                       review_activity_count,
                       review_activity_rank
                FROM read_parquet({rt})
                WHERE is_focal
            ) ra ON f.case_id = ra.case_id AND f.focal_id = ra.focal_id
        """
        review_select = "ra.review_activity_count, ra.review_activity_rank"

    copy_atomic(f"""
        WITH focals AS (
            SELECT
                f.case_id,
                f.focal_id,
                f.focal_product_id,
                f.t0,
                f.evaluation_start,
                f.evaluation_end_exclusive,
                f.valid_t0,
                f.evaluation_window_complete,
                f.post90_rating_count,
                f.market_pre_t0_review_count,
                c.market_id,
                c.source_partition
            FROM read_parquet({f}) f
            JOIN read_parquet({c}) c ON f.case_id = c.case_id
        ),
        sel AS (
            SELECT *
            FROM read_parquet({fc})
            WHERE competitor_selected
        ),
        local_items AS (
            SELECT case_id, focal_id, focal_product_id AS product_id, TRUE AS is_focal_item
            FROM focals
            UNION ALL
            SELECT case_id, focal_id, competitor_product_id AS product_id, FALSE AS is_focal_item
            FROM sel
        ),
        comp_struct AS (
            SELECT
                focals.case_id,
                focals.focal_id,
                count(sel.competitor_product_id)::BIGINT AS selected_competitor_count,
                count(*) FILTER (
                    sel.competitor_product_id = focals.focal_product_id
                )::BIGINT AS competitor_self_reference_count,
                (
                    count(sel.competitor_product_id)
                    - count(DISTINCT sel.competitor_product_id)
                )::BIGINT AS duplicate_competitor_count,
                count(*) FILTER (NOT ({time_ok}))::BIGINT
                    AS competitor_time_eligibility_fail_count,
                count(*) FILTER (shelf.product_id IS NULL)::BIGINT
                    AS competitor_missing_from_shelf_count
            FROM focals
            LEFT JOIN sel
              ON sel.case_id = focals.case_id
             AND sel.focal_id = focals.focal_id
            {time_join}
            LEFT JOIN read_parquet({s}) shelf
              ON shelf.case_id = focals.case_id
             AND shelf.product_id = sel.competitor_product_id
            GROUP BY focals.case_id, focals.focal_id
        ),
        local_struct AS (
            SELECT
                li.case_id,
                li.focal_id,
                count(*)::BIGINT AS local_shelf_size,
                count(*) FILTER (shelf.product_id IS NOT NULL)::BIGINT
                    AS local_shelf_on_case_shelf,
                max(CASE WHEN li.is_focal_item AND shelf.product_id IS NOT NULL
                         THEN 1 ELSE 0 END)::BOOLEAN AS focal_in_case_shelf
            FROM local_items li
            LEFT JOIN read_parquet({s}) shelf
              ON shelf.case_id = li.case_id
             AND shelf.product_id = li.product_id
            GROUP BY li.case_id, li.focal_id
        ),
        {shelf_events_cte}
        {raw_cte}
        {users_cte}
        gt1_rows AS (
            SELECT
                ct.case_id,
                ct.focal_id,
                ct.user_id,
                ct.product_id,
                ct.event_timestamp,
                ct.history_product_count,
                ct.days_since_last_event,
                focals.t0,
                focals.evaluation_end_exclusive,
                focals.focal_product_id,
                (local_hit.product_id IS NOT NULL) AS choice_on_local_shelf,
                ({trace_fail}) AS trace_fail,
                ({reducer_fail} OR {raw_dup_fail}) AS reducer_fail
            FROM read_parquet({g1}) ct
            JOIN focals
              ON focals.case_id = ct.case_id AND focals.focal_id = ct.focal_id
            LEFT JOIN local_items local_hit
              ON local_hit.case_id = ct.case_id
             AND local_hit.focal_id = ct.focal_id
             AND local_hit.product_id = ct.product_id
            {shelf_events_join}
            {raw_join}
        ),
        gt1 AS (
            SELECT
                case_id,
                focal_id,
                count(*)::BIGINT AS gt1_row_count,
                count(DISTINCT user_id)::BIGINT AS gt1_user_count,
                count(*) FILTER (product_id IS NULL)::BIGINT AS gt1_null_choice_count,
                count(*) FILTER (NOT choice_on_local_shelf)::BIGINT
                    AS gt1_choice_outside_local_shelf_count,
                count(*) FILTER (
                    event_timestamp IS NULL
                    OR CAST(event_timestamp AS DATE) < t0
                    OR CAST(event_timestamp AS DATE) >= evaluation_end_exclusive
                )::BIGINT AS gt1_choice_outside_window_count,
                count(*) FILTER (
                    history_product_count IS NULL
                    OR history_product_count < {min_hist}
                    OR days_since_last_event IS NULL
                    OR days_since_last_event > {max_recency}
                )::BIGINT AS gt1_history_quality_fail_count,
                count(*) FILTER (trace_fail)::BIGINT AS gt1_untraceable_count,
                count(*) FILTER (reducer_fail)::BIGINT AS gt1_reducer_fail_count,
                (count(*) - count(DISTINCT user_id))::BIGINT AS gt1_duplicate_user_count,
                count(*) FILTER (product_id = focal_product_id)::BIGINT AS focal_choice_count
            FROM gt1_rows
            GROUP BY case_id, focal_id
        ),
        choice_by_product AS (
            SELECT case_id, focal_id, product_id, count(*)::BIGINT AS choice_count
            FROM read_parquet({g1})
            GROUP BY case_id, focal_id, product_id
        ),
        local_choice AS (
            SELECT
                li.case_id,
                li.focal_id,
                li.product_id,
                li.is_focal_item,
                coalesce(cb.choice_count, 0)::BIGINT AS choice_count
            FROM local_items li
            LEFT JOIN choice_by_product cb
              ON cb.case_id = li.case_id
             AND cb.focal_id = li.focal_id
             AND cb.product_id = li.product_id
        ),
        choice_stats AS (
            SELECT
                case_id,
                focal_id,
                sum(choice_count)::BIGINT AS local_choice_total,
                sum(choice_count) FILTER (NOT is_focal_item)::BIGINT
                    AS competitor_choice_count,
                sum(choice_count) FILTER (is_focal_item)::BIGINT AS focal_choice_count_local
            FROM local_choice
            GROUP BY case_id, focal_id
        ),
        focal_rank AS (
            SELECT
                lc.case_id,
                lc.focal_id,
                1 + count(*) FILTER (
                    o.choice_count > lc.choice_count
                    OR (
                        o.choice_count = lc.choice_count
                        AND o.product_id < lc.product_id
                    )
                )::BIGINT AS focal_choice_rank
            FROM local_choice lc
            JOIN local_choice o
              ON o.case_id = lc.case_id AND o.focal_id = lc.focal_id
            WHERE lc.is_focal_item
            GROUP BY lc.case_id, lc.focal_id
        )
        SELECT
            f.case_id,
            f.focal_id,
            f.focal_product_id,
            f.t0,
            f.evaluation_end_exclusive,
            coalesce(cs.selected_competitor_count, 0)::BIGINT AS selected_competitor_count,
            coalesce(gt1.gt1_user_count, 0)::BIGINT AS gt1_user_count,
            coalesce(f.evaluation_window_complete, false) AS evaluation_window_complete,
            coalesce(ls.focal_in_case_shelf, false) AS focal_in_case_shelf,
            (
                coalesce(cs.competitor_self_reference_count, 0) = 0
                AND coalesce(cs.duplicate_competitor_count, 0) = 0
                AND coalesce(cs.competitor_missing_from_shelf_count, 0) = 0
            ) AS competitor_relation_complete,
            TRUE AS competitor_count_consistent,
            coalesce(cs.competitor_time_eligibility_fail_count, 0) = 0
                AS competitor_time_eligibility_complete,
            (
                coalesce(ls.local_shelf_size, 0)
                    = 1 + coalesce(cs.selected_competitor_count, 0)
                AND coalesce(ls.local_shelf_on_case_shelf, 0)
                    = coalesce(ls.local_shelf_size, 0)
            ) AS local_shelf_consistent,
            (
                coalesce(gt1.gt1_row_count, 0) = coalesce(gt1.gt1_user_count, 0)
                AND coalesce(gt1.gt1_duplicate_user_count, 0) = 0
                AND {users_complete}
            ) AS gt1_unique_user_complete,
            (
                coalesce(gt1.gt1_null_choice_count, 0) = 0
                AND coalesce(gt1.gt1_choice_outside_local_shelf_count, 0) = 0
            ) AS gt1_choice_membership_complete,
            coalesce(gt1.gt1_choice_outside_window_count, 0) = 0 AS gt1_window_complete,
            coalesce(gt1.gt1_history_quality_fail_count, 0) = 0
                AS gt1_history_quality_complete,
            coalesce(gt1.gt1_reducer_fail_count, 0) = 0 AS gt1_reducer_unique,
            coalesce(gt1.gt1_untraceable_count, 0) = 0 AS gt1_traceable_to_window_event,
            coalesce(cs.competitor_self_reference_count, 0)::BIGINT
                AS competitor_self_reference_count,
            coalesce(cs.duplicate_competitor_count, 0)::BIGINT AS duplicate_competitor_count,
            coalesce(cs.competitor_missing_from_shelf_count, 0)::BIGINT
                AS competitor_missing_from_shelf_count,
            coalesce(ls.local_shelf_size, 0)::BIGINT AS local_shelf_size,
            coalesce(ch.focal_choice_count_local, gt1.focal_choice_count, 0)::BIGINT
                AS focal_choice_count,
            coalesce(ch.competitor_choice_count, 0)::BIGINT AS competitor_choice_count,
            CASE WHEN coalesce(ch.local_choice_total, 0) > 0
                 THEN coalesce(ch.focal_choice_count_local, 0)::DOUBLE
                      / ch.local_choice_total
                 ELSE NULL END AS focal_choice_share,
            fr.focal_choice_rank,
            f.post90_rating_count,
            f.market_pre_t0_review_count,
            {users_select}
            {review_select}
        FROM focals f
        LEFT JOIN comp_struct cs ON f.case_id = cs.case_id AND f.focal_id = cs.focal_id
        LEFT JOIN local_struct ls ON f.case_id = ls.case_id AND f.focal_id = ls.focal_id
        LEFT JOIN gt1 ON f.case_id = gt1.case_id AND f.focal_id = gt1.focal_id
        LEFT JOIN choice_stats ch ON f.case_id = ch.case_id AND f.focal_id = ch.focal_id
        LEFT JOIN focal_rank fr ON f.case_id = fr.case_id AND f.focal_id = fr.focal_id
        {users_join}
        {review_join}
        ORDER BY f.case_id, f.focal_id
    """, destination)
