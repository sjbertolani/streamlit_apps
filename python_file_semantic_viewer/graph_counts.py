from __future__ import annotations

from typing import Dict, Optional, Tuple

import pandas as pd

from semantic_parser import ConceptInfo, SemanticGraph

INSTANCE_LIMIT = 15
_SEP = "::"  # separates concept name from pk value in instance node IDs


def make_instance_id(concept: str, pk_val) -> str:
    """Build a vis.js node ID for a data instance."""
    return f"{concept}{_SEP}{pk_val}"


def parse_instance_id(node_id: str) -> Optional[Tuple[str, str]]:
    """Return (concept_name, pk_val) or None if this is a schema node ID."""
    parts = node_id.split(_SEP, 1)
    return (parts[0], parts[1]) if len(parts) == 2 else None


def is_instance_node(node_id: str, graph: SemanticGraph) -> bool:
    """True when node_id does not correspond to a concept type node."""
    return node_id not in graph.concepts


def fmt_count(n: int) -> str:
    if n < 0:
        return "?"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def fetch_schema_counts(sf_client, graph: SemanticGraph) -> Dict[str, int]:
    """Return {concept_name: row_count} for all concepts with a base table."""
    counts: Dict[str, int] = {}
    for name, concept in graph.concepts.items():
        if not concept.base_table:
            continue
        try:
            df = sf_client.query_df(f"SELECT COUNT(*) AS n FROM {concept.base_table}")
            counts[name] = int(df.iloc[0, 0])
        except Exception:
            counts[name] = -1
    return counts


def fetch_edge_counts(sf_client, graph: SemanticGraph) -> Dict[int, int]:
    """Return {rel_index: row_count}. Deduplicates queries for shared rel_tables."""
    seen: Dict[str, int] = {}
    counts: Dict[int, int] = {}
    for idx, rel in enumerate(graph.relationships):
        if not rel.rel_table:
            continue
        if rel.rel_table not in seen:
            try:
                df = sf_client.query_df(f"SELECT COUNT(*) AS n FROM {rel.rel_table}")
                seen[rel.rel_table] = int(df.iloc[0, 0])
            except Exception:
                seen[rel.rel_table] = -1
        counts[idx] = seen[rel.rel_table]
    return counts


def fetch_instances(
    sf_client, concept: ConceptInfo, limit: int = INSTANCE_LIMIT
) -> Optional[pd.DataFrame]:
    """Fetch sample rows for a concept. Returns None on any failure."""
    if not concept.base_table:
        return None
    try:
        return sf_client.query_df(f"SELECT * FROM {concept.base_table} LIMIT {limit}")
    except Exception:
        return None


def fetch_filtered_instances(
    sf_client,
    concept: ConceptInfo,
    reach_sql: str,
    reach_cols: list,
    limit: int = INSTANCE_LIMIT,
) -> Optional[pd.DataFrame]:
    """
    Fetch only the instances of `concept` that appear in the BFS reach subquery.

    Uses WHERE id IN (subquery) — avoids DISTINCT t.* JOIN ambiguity in Snowflake.
    Falls back to unfiltered fetch if no ID column can be matched.
    """
    if not concept.base_table or not concept.id_columns:
        return None

    # Find the first concept ID column that has a matching reach column
    id_col = None
    reach_col = None
    for ic in concept.id_columns:
        match = next((rc for rc in reach_cols if rc.upper() == ic.upper()), None)
        if match:
            id_col, reach_col = ic, match
            break

    if not id_col:
        return fetch_instances(sf_client, concept, limit)

    sql = (
        f'SELECT * FROM {concept.base_table} '
        f'WHERE "{id_col}" IN ('
        f'  SELECT "{reach_col}" FROM ({reach_sql})'
        f') LIMIT {limit}'
    )
    try:
        return sf_client.query_df(sql)
    except Exception:
        return fetch_instances(sf_client, concept, limit)
