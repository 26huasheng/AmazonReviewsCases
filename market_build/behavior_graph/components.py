from __future__ import annotations

import hashlib
from collections import defaultdict
from pathlib import Path

import duckdb

from utils import sql_literal
from .full_graph import MIN_NODE_USERS

MIN_VALID_BEHAVIOR_GROUP_SIZE = 6


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}
        self.size: dict[str, int] = {}

    def add(self, value: str) -> None:
        if value not in self.parent:
            self.parent[value] = value
            self.size[value] = 1

    def find(self, value: str) -> str:
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, left: str, right: str) -> None:
        self.add(left)
        self.add(right)
        a = self.find(left)
        b = self.find(right)
        if a == b:
            return
        if self.size[a] < self.size[b]:
            a, b = b, a
        self.parent[b] = a
        self.size[a] += self.size[b]


def _stable_component_id(source_partition: str, leaf_category: str, members: list[str]) -> str:
    payload = source_partition + "\x1f" + leaf_category + "\x1f" + "\x1f".join(sorted(members))
    return "graph_" + hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def write_full_graph_components(
    con: duckdb.DuckDBPyConnection,
    full_graph_edges: Path,
    product_user_totals: Path,
    destination: Path,
    copy_atomic,
    *,
    min_endpoint_users: int,
) -> None:
    """Legacy audit components. Not used by MarketBuildPipeline / Electronics v1.

    Production focal-diversity components are write_behavior_components.
    size >= 2 的连通分量写 `graph_status='component'`；完整时期用户数已经达到
    端点资格、但没有任何 strong edge 的商品写 `graph_status='isolated'`。
    """
    if min_endpoint_users <= 0:
        raise ValueError("min_endpoint_users must be positive")

    edges = sql_literal(str(full_graph_edges))
    totals = sql_literal(str(product_user_totals))

    eligible_rows = con.execute(f"""
        SELECT source_partition, leaf_category, product_id
        FROM read_parquet({totals})
        WHERE n_users_full >= {int(min_endpoint_users)}
          AND leaf_category IS NOT NULL
          AND trim(leaf_category) <> ''
        ORDER BY source_partition, leaf_category, product_id
    """).fetchall()

    by_leaf: dict[tuple[str, str], _UnionFind] = defaultdict(_UnionFind)
    eligible_by_leaf: dict[tuple[str, str], list[str]] = defaultdict(list)
    for source_partition, leaf_category, product_id in eligible_rows:
        key = (str(source_partition), str(leaf_category))
        product = str(product_id)
        eligible_by_leaf[key].append(product)
        by_leaf[key].add(product)

    edge_rows = con.execute(f"""
        SELECT source_partition, leaf_category, product_a, product_b
        FROM read_parquet({edges})
        ORDER BY source_partition, leaf_category, product_a, product_b
    """).fetchall()
    for source_partition, leaf_category, product_a, product_b in edge_rows:
        key = (str(source_partition), str(leaf_category))
        by_leaf[key].union(str(product_a), str(product_b))

    output_rows: list[tuple[str, str, str, str | None, int, str]] = []
    for (source_partition, leaf_category), products in sorted(eligible_by_leaf.items()):
        uf = by_leaf[(source_partition, leaf_category)]
        groups: dict[str, list[str]] = defaultdict(list)
        for product in products:
            groups[uf.find(product)].append(product)

        for members in groups.values():
            members = sorted(members)
            size = len(members)
            if size >= 2:
                component_id = _stable_component_id(source_partition, leaf_category, members)
                status = "component"
            else:
                component_id = None
                status = "isolated"
            for product in members:
                output_rows.append((
                    source_partition,
                    leaf_category,
                    product,
                    component_id,
                    size,
                    status,
                ))

    con.execute("DROP TABLE IF EXISTS behavior_graph_components_tmp")
    con.execute("""
        CREATE TEMP TABLE behavior_graph_components_tmp (
            source_partition VARCHAR,
            leaf_category VARCHAR,
            product_id VARCHAR,
            graph_component_id VARCHAR,
            component_size BIGINT,
            graph_status VARCHAR
        )
    """)
    if output_rows:
        con.executemany(
            "INSERT INTO behavior_graph_components_tmp VALUES (?, ?, ?, ?, ?, ?)",
            output_rows,
        )
    copy_atomic("""
        SELECT *
        FROM behavior_graph_components_tmp
        ORDER BY source_partition, leaf_category,
                 coalesce(graph_component_id, ''), product_id
    """, destination)


class _ProductionBehaviorUnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, item: str) -> str:
        self.parent.setdefault(item, item)
        root = item
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[item] != root:
            nxt = self.parent[item]
            self.parent[item] = root
            item = nxt
        return root

    def union(self, left: str, right: str) -> None:
        a = self.find(left)
        b = self.find(right)
        if a != b:
            self.parent[b] = a


def _production_component_id(
    source_partition: str,
    market_id: str,
    leaf_category: str,
    members: list[str],
) -> str:
    payload = "|".join(
        [source_partition, market_id, leaf_category, *sorted(members)]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"comp_{digest}"


def write_behavior_components(
    con: duckdb.DuckDBPyConnection,
    product_degrees: Path,
    strong_edges: Path,
    components_path: Path,
    summary_path: Path,
    copy_atomic,
    *,
    min_node_users: int = MIN_NODE_USERS,
    min_valid_group_size: int = MIN_VALID_BEHAVIOR_GROUP_SIZE,
) -> dict[str, int]:
    """Assign Market-scoped connected components. Graph is not a Market definition."""
    products = con.execute("""
        SELECT source_partition, market_id, market_label, product_id,
               leaf_category, n_users
        FROM read_parquet(?)
        ORDER BY source_partition, market_id, product_id
    """, [str(product_degrees)]).fetchall()
    edges = con.execute("""
        SELECT source_partition, market_id, leaf_category, product_id_a, product_id_b
        FROM read_parquet(?)
    """, [str(strong_edges)]).fetchall()

    forests: dict[tuple[str, str, str], _ProductionBehaviorUnionFind] = defaultdict(
        _ProductionBehaviorUnionFind
    )
    for source_partition, market_id, leaf, a, b in edges:
        forests[(str(source_partition), str(market_id), str(leaf))].union(str(a), str(b))

    members: dict[tuple[str, str, str, str], list[str]] = defaultdict(list)
    for (source_partition, market_id, leaf), uf in forests.items():
        nodes = set(uf.parent)
        for node in nodes:
            root = uf.find(node)
            members[(source_partition, market_id, leaf, root)].append(node)

    component_meta: dict[tuple[str, str, str, str], tuple[str, int]] = {}
    for key, group in members.items():
        source_partition, market_id, leaf, root = key
        cid = _production_component_id(source_partition, market_id, leaf, group)
        component_meta[key] = (cid, len(group))
        for product_id in group:
            component_meta[(source_partition, market_id, leaf, product_id)] = (
                cid, len(group)
            )

    rows: list[tuple] = []
    for source_partition, market_id, market_label, product_id, leaf, n_users in products:
        eligible = (
            int(n_users) >= min_node_users
            and leaf is not None
            and str(leaf).strip() != ""
        )
        if eligible:
            uf = forests.get((str(source_partition), str(market_id), str(leaf)))
            if uf is not None and str(product_id) in uf.parent:
                root = uf.find(str(product_id))
                cid, size = component_meta[(str(source_partition), str(market_id), str(leaf), root)]
                status = "clustered"
                component_id = cid
                component_size = size
            else:
                status = "isolated"
                component_id = None
                component_size = 1
        else:
            status = "ineligible"
            component_id = None
            component_size = 1
        rows.append((
            str(source_partition),
            str(market_id),
            str(market_label),
            None if leaf is None else str(leaf),
            str(product_id),
            component_id,
            int(component_size),
            status,
            bool(component_size >= min_valid_group_size and status == "clustered"),
            int(n_users),
        ))

    con.execute("DROP TABLE IF EXISTS market_behavior_component_rows")
    con.execute("""
        CREATE TEMP TABLE market_behavior_component_rows(
            source_partition VARCHAR,
            market_id VARCHAR,
            market_label VARCHAR,
            leaf_category VARCHAR,
            product_id VARCHAR,
            graph_component_id VARCHAR,
            component_size BIGINT,
            graph_status VARCHAR,
            valid_behavior_group BOOLEAN,
            n_users BIGINT
        )
    """)
    if rows:
        con.executemany(
            "INSERT INTO market_behavior_component_rows VALUES (?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
    copy_atomic("""
        SELECT source_partition, market_id, market_label, leaf_category, product_id,
               graph_component_id, component_size, graph_status, valid_behavior_group
        FROM market_behavior_component_rows
        ORDER BY source_partition, market_id, product_id
    """, components_path)
    copy_atomic("""
        SELECT source_partition,
               market_id,
               graph_component_id,
               any_value(leaf_category) AS leaf_category,
               any_value(component_size) AS component_size,
               count(*)::BIGINT AS n_products,
               any_value(valid_behavior_group) AS valid_behavior_group
        FROM market_behavior_component_rows
        WHERE graph_component_id IS NOT NULL
        GROUP BY source_partition, market_id, graph_component_id
        ORDER BY source_partition, market_id, component_size DESC, graph_component_id
    """, summary_path)
    n_products = len(rows)
    n_clustered = sum(1 for row in rows if row[7] == "clustered")
    n_valid = sum(1 for row in rows if row[8])
    n_components = int(con.execute("""
        SELECT count(DISTINCT graph_component_id)
        FROM market_behavior_component_rows
        WHERE graph_component_id IS NOT NULL
    """).fetchone()[0])
    return {
        "product_rows": n_products,
        "clustered_product_rows": n_clustered,
        "valid_behavior_group_product_rows": n_valid,
        "component_count": n_components,
    }
