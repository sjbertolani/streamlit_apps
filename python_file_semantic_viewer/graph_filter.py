from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from semantic_parser import RelationshipInfo, SemanticGraph


@dataclass
class FilterSpec:
    concept: str
    column: str
    value: str


@dataclass
class ReachQuery:
    sql: str
    cols: List[str]


def _sql_literal(value: str) -> str:
    if value is None:
        return "NULL"
    v = value.strip()
    if v == "":
        return "NULL"
    if _is_number(v):
        return v
    escaped = v.replace("'", "''")
    return f"'{escaped}'"


def _is_number(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


def _join_condition(
    alias_rel: str,
    alias_reach: str,
    join_map: List[Tuple[str, str]],
    reach_cols: List[str],
) -> Optional[str]:
    filtered = [(rel_col, id_col) for rel_col, id_col in join_map if id_col in reach_cols]
    if not filtered:
        return None

    grouped: Dict[str, List[str]] = {}
    for rel_col, id_col in filtered:
        grouped.setdefault(id_col, []).append(rel_col)

    clauses: List[str] = []
    for id_col, rel_cols in grouped.items():
        if len(rel_cols) == 1:
            clauses.append(f'{alias_rel}."{rel_cols[0]}" = {alias_reach}."{id_col}"')
        else:
            ors = " OR ".join(
                f'{alias_rel}."{col}" = {alias_reach}."{id_col}"' for col in rel_cols
            )
            clauses.append(f"({ors})")
    return " AND ".join(clauses)


def _build_reach_sql(
    rel: RelationshipInfo,
    source: str,
    reach: ReachQuery,
) -> Optional[ReachQuery]:
    if source == rel.source:
        src_join = rel.source_join
        dst_join = rel.target_join
    elif source == rel.target:
        src_join = rel.target_join
        dst_join = rel.source_join
    else:
        return None

    if not rel.rel_table:
        return None
    if not dst_join or not src_join:
        return None

    join_cond = _join_condition("r", "s", src_join, reach.cols)
    if not join_cond:
        return None

    select_cols: List[str] = []
    seen: set = set()
    for rel_col, id_col in dst_join:
        if id_col in seen:
            continue
        select_cols.append(f'r."{rel_col}" AS "{id_col}"')
        seen.add(id_col)

    if not select_cols:
        return None

    sql = (
        f"SELECT DISTINCT {', '.join(select_cols)} "
        f"FROM {rel.rel_table} r "
        f"JOIN ({reach.sql}) s ON {join_cond}"
    )
    return ReachQuery(sql=sql, cols=list(seen))


def compute_reach(
    graph: SemanticGraph,
    filter_spec: FilterSpec,
) -> Dict[str, ReachQuery]:
    """
    Pure BFS over relationship graph — no Snowflake queries.

    Returns a dict mapping each reachable concept name to a ReachQuery whose
    SQL subquery yields the ID column(s) of matching instances for that concept.
    The anchor concept's reach is a simple WHERE filter; every other concept's
    reach is derived by joining through relationship tables.
    """
    concept = graph.concepts.get(filter_spec.concept)
    if not concept or not concept.base_table or not concept.id_columns:
        return {}

    id_col_sql = ", ".join(f'"{c}"' for c in concept.id_columns)
    col_quoted = f'"{filter_spec.column}"'
    filter_sql = (
        f"SELECT DISTINCT {id_col_sql} "
        f"FROM {concept.base_table} "
        f"WHERE {col_quoted} = {_sql_literal(filter_spec.value)}"
    )

    reach: Dict[str, ReachQuery] = {
        filter_spec.concept: ReachQuery(sql=filter_sql, cols=list(concept.id_columns))
    }
    queue: List[str] = [filter_spec.concept]

    while queue:
        current = queue.pop(0)
        current_reach = reach[current]
        for rel in graph.relationships:
            if current not in (rel.source, rel.target):
                continue
            neighbor = rel.target if current == rel.source else rel.source
            new_reach = _build_reach_sql(rel, current, current_reach)
            if not new_reach:
                continue
            if neighbor in reach:
                if set(new_reach.cols) == set(reach[neighbor].cols):
                    union_sql = (
                        f"SELECT DISTINCT * FROM ({reach[neighbor].sql}) "
                        f"UNION SELECT DISTINCT * FROM ({new_reach.sql})"
                    )
                    reach[neighbor] = ReachQuery(
                        sql=union_sql, cols=reach[neighbor].cols
                    )
                continue
            reach[neighbor] = new_reach
            queue.append(neighbor)

    return reach


def compute_activity(
    graph: SemanticGraph,
    sf_client,
    filter_spec: Optional[FilterSpec],
) -> Tuple[Dict[str, bool], Dict[int, bool]]:
    node_active: Dict[str, bool] = {name: True for name in graph.concepts}
    edge_active: Dict[int, bool] = {idx: True for idx, _ in enumerate(graph.relationships)}

    if not filter_spec:
        return node_active, edge_active

    reach = compute_reach(graph, filter_spec)
    if not reach:
        return node_active, edge_active

    for concept_name in graph.concepts:
        if concept_name not in reach:
            node_active[concept_name] = False
            continue
        try:
            node_active[concept_name] = sf_client.exists(
                f"SELECT 1 FROM ({reach[concept_name].sql}) LIMIT 1"
            )
        except Exception:
            node_active[concept_name] = False

    for idx, rel in enumerate(graph.relationships):
        if rel.source not in reach or rel.target not in reach:
            edge_active[idx] = False
            continue
        if not rel.rel_table:
            edge_active[idx] = False
            continue
        reach_a = reach[rel.source]
        reach_b = reach[rel.target]
        cond_a = _join_condition("r", "a", rel.source_join, reach_a.cols)
        cond_b = _join_condition("r", "b", rel.target_join, reach_b.cols)
        if not cond_a or not cond_b:
            edge_active[idx] = False
            continue
        sql = (
            f"SELECT 1 FROM {rel.rel_table} r "
            f"JOIN ({reach_a.sql}) a ON {cond_a} "
            f"JOIN ({reach_b.sql}) b ON {cond_b} "
            f"LIMIT 1"
        )
        try:
            edge_active[idx] = sf_client.exists(sql)
        except Exception:
            edge_active[idx] = False

    return node_active, edge_active
