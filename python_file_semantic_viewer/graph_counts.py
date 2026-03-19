from __future__ import annotations

from typing import Dict, Optional, Tuple

import pandas as pd

from semantic_parser import ConceptInfo, SemanticGraph

INSTANCE_LIMIT = 15
_SEP = "::"  # separates concept name from pk value in instance node IDs


def make_instance_id(concept: str, pk_val) -> str:
    return f"{concept}{_SEP}{pk_val}"


def parse_instance_id(node_id: str) -> Optional[Tuple[str, str]]:
    parts = node_id.split(_SEP, 1)
    return (parts[0], parts[1]) if len(parts) == 2 else None


def is_instance_node(node_id: str, graph: SemanticGraph) -> bool:
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
    """
    Return {concept_name: row_count} for all concepts with a base table.

    Tries a single UNION ALL query first (one Snowflake round trip for all
    concepts). Falls back to serial queries if the batch fails.
    """
    targets = [(name, c.base_table) for name, c in graph.concepts.items() if c.base_table]
    if not targets:
        return {}

    # Batch: one round trip instead of N
    parts = [
        f"SELECT {i} AS _idx, COUNT(*) AS _n FROM {table}"
        for i, (_, table) in enumerate(targets)
    ]
    try:
        df = sf_client.query_df(" UNION ALL ".join(parts))
        idx_col = next(c for c in df.columns if c.upper() == "_IDX")
        n_col = next(c for c in df.columns if c.upper() == "_N")
        return {targets[int(row[idx_col])][0]: int(row[n_col]) for _, row in df.iterrows()}
    except Exception:
        pass

    # Serial fallback (e.g. if one table lacks permissions)
    counts: Dict[str, int] = {}
    for name, table in targets:
        try:
            df = sf_client.query_df(f"SELECT COUNT(*) AS n FROM {table}")
            counts[name] = int(df.iloc[0, 0])
        except Exception:
            counts[name] = -1
    return counts


def fetch_edge_counts(sf_client, graph: SemanticGraph) -> Dict[int, int]:
    """
    Return {rel_index: row_count}.

    Deduplicates queries for shared rel_tables and batches all unique tables
    into a single UNION ALL query.
    """
    # Collect unique rel_tables in traversal order
    unique_tables: list[str] = []
    seen: set[str] = set()
    for rel in graph.relationships:
        if rel.rel_table and rel.rel_table not in seen:
            unique_tables.append(rel.rel_table)
            seen.add(rel.rel_table)

    if not unique_tables:
        return {}

    # Batch: one round trip for all unique tables
    parts = [
        f"SELECT {i} AS _idx, COUNT(*) AS _n FROM {table}"
        for i, table in enumerate(unique_tables)
    ]
    table_counts: Dict[str, int] = {}
    try:
        df = sf_client.query_df(" UNION ALL ".join(parts))
        idx_col = next(c for c in df.columns if c.upper() == "_IDX")
        n_col = next(c for c in df.columns if c.upper() == "_N")
        for _, row in df.iterrows():
            table_counts[unique_tables[int(row[idx_col])]] = int(row[n_col])
    except Exception:
        # Serial fallback
        for table in unique_tables:
            try:
                df = sf_client.query_df(f"SELECT COUNT(*) AS n FROM {table}")
                table_counts[table] = int(df.iloc[0, 0])
            except Exception:
                table_counts[table] = -1

    return {
        idx: table_counts[rel.rel_table]
        for idx, rel in enumerate(graph.relationships)
        if rel.rel_table and rel.rel_table in table_counts
    }


def fetch_instances(
    sf_client, concept: ConceptInfo, limit: int = INSTANCE_LIMIT
) -> Optional[pd.DataFrame]:
    if not concept.base_table:
        return None
    try:
        return sf_client.query_df(f"SELECT * FROM {concept.base_table} LIMIT {limit}")
    except Exception:
        return None
