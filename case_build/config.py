from __future__ import annotations

DEFAULT_EVALUATION_DAYS = 90
POST90_DAYS = 90
DEFAULT_RECENT_ACTIVITY_WINDOW_DAYS = 120
ENTRY_DATE_SOURCE = "first_rating_date"
MAX_COMPETITORS_PER_FOCAL = 16
FOCAL_SELECTION_SEED = "focal_selection_v1"
POPULATION_CUTOFF_POLICY = "earliest_focal_t0"
MIN_VALID_BEHAVIOR_GROUP_SIZE = 6
MIN_HISTORY_PRODUCTS = 3
MAX_DAYS_SINCE_LAST_EVENT = 365
MIN_CATEGORY_PRODUCTS = None
MIN_MARKET_PRODUCTS = None
TARGET_USERS_PER_CASE = 1000
TARGET_BACKGROUND_USERS_PER_CASE = 1000
BACKGROUND_CANDIDATE_POOL = 20000

# 与 v5 已确认的时间划分保持一致：2021 年以前按两年段，2021-2023 按半年。
# 所有区间都按 [start_date, end_date) 解释，因此 2023-H2 的结束点使用 2024-01-01。
DEFAULT_TIME_BOXES: list[dict[str, str]] = [
    {"time_box_id": "1996", "start_date": "1996-01-01", "end_date": "1997-01-01"},
    {"time_box_id": "1997-1998", "start_date": "1997-01-01", "end_date": "1999-01-01"},
    {"time_box_id": "1999-2000", "start_date": "1999-01-01", "end_date": "2001-01-01"},
    {"time_box_id": "2001-2002", "start_date": "2001-01-01", "end_date": "2003-01-01"},
    {"time_box_id": "2003-2004", "start_date": "2003-01-01", "end_date": "2005-01-01"},
    {"time_box_id": "2005-2006", "start_date": "2005-01-01", "end_date": "2007-01-01"},
    {"time_box_id": "2007-2008", "start_date": "2007-01-01", "end_date": "2009-01-01"},
    {"time_box_id": "2009-2010", "start_date": "2009-01-01", "end_date": "2011-01-01"},
    {"time_box_id": "2011-2012", "start_date": "2011-01-01", "end_date": "2013-01-01"},
    {"time_box_id": "2013-2014", "start_date": "2013-01-01", "end_date": "2015-01-01"},
    {"time_box_id": "2015-2016", "start_date": "2015-01-01", "end_date": "2017-01-01"},
    {"time_box_id": "2017-2018", "start_date": "2017-01-01", "end_date": "2019-01-01"},
    {"time_box_id": "2019-2020", "start_date": "2019-01-01", "end_date": "2021-01-01"},
    {"time_box_id": "2021-H1", "start_date": "2021-01-01", "end_date": "2021-07-01"},
    {"time_box_id": "2021-H2", "start_date": "2021-07-01", "end_date": "2022-01-01"},
    {"time_box_id": "2022-H1", "start_date": "2022-01-01", "end_date": "2022-07-01"},
    {"time_box_id": "2022-H2", "start_date": "2022-07-01", "end_date": "2023-01-01"},
    {"time_box_id": "2023-H1", "start_date": "2023-01-01", "end_date": "2023-07-01"},
    {"time_box_id": "2023-H2", "start_date": "2023-07-01", "end_date": "2024-01-01"},
]
