from __future__ import annotations

import os
from typing import List, Optional

import pandas as pd
import streamlit as st
from streamlit_agraph import Config, Edge, Node, agraph

from graph_counts import (
    INSTANCE_LIMIT,
    fetch_edge_counts,
    fetch_filtered_instances,
    fetch_instances,
    fetch_schema_counts,
    fmt_count,
    is_instance_node,
    make_instance_id,
    parse_instance_id,
)
from graph_filter import FilterSpec, compute_activity, compute_reach
from semantic_parser import SemanticGraph, parse_semantic_file, parse_semantic_text
from snowflake_client import SnowflakeClient


st.set_page_config(page_title="Semantic Graph Explorer", layout="wide")


def _apply_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;600&display=swap');
        :root { --accent: #0e5a6f; --accent-soft: #e0f1f4; --ink: #1e1e1e; }
        html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; color: var(--ink); }
        .stApp { background: radial-gradient(1200px 600px at 10% 0%, #e7f0f2, #f4f1ec); }
        .block-container { padding-top: 1.2rem; }
        .stButton>button {
            border-radius: 999px; border: 1px solid var(--accent);
            color: var(--accent); background: white;
        }
        .stButton>button:hover { background: var(--accent-soft); }
        </style>
        """,
        unsafe_allow_html=True,
    )


_apply_styles()
st.title("Semantic Graph Explorer")


# ── Session state init ────────────────────────────────────────────────────────
for _key, _default in [
    ("expanded_concepts", set()),
    ("instance_cache", {}),
    ("schema_counts", {}),
    ("edge_counts", {}),
    ("filter_specs", []),          # List[FilterSpec] — commutative set semantics
    ("selected_node", None),
    ("validation_results", None),
]:
    if _key not in st.session_state:
        st.session_state[_key] = _default


@st.cache_data(show_spinner=False)
def _load_graph(text: str) -> SemanticGraph:
    return parse_semantic_text(text)


def _get_graph(uploaded_file) -> SemanticGraph:
    if uploaded_file is not None:
        return _load_graph(uploaded_file.getvalue().decode("utf-8", errors="ignore"))
    raise FileNotFoundError("No semantic layer file provided.")


def _get_selected_node(result) -> Optional[str]:
    if result is None:
        return None
    if isinstance(result, str):
        return result
    if isinstance(result, dict):
        for key in ("selected_node", "node"):
            if result.get(key):
                return result[key]
        nodes = result.get("selected_nodes")
        if nodes:
            return nodes[0]
    if hasattr(result, "id"):
        return getattr(result, "id")
    return None


def _find_col(df: pd.DataFrame, col_name: str) -> Optional[str]:
    for c in df.columns:
        if c.upper() == col_name.upper():
            return c
    return None


def _filter_key(specs: List[FilterSpec]) -> str:
    return "|".join(
        f"{s.concept}:{s.column}:{s.value}"
        for s in sorted(specs, key=lambda x: f"{x.concept}{x.column}{x.value}")
    )


def _combined_reach(all_reaches, concept_name: str) -> Optional[object]:
    """Union reach queries from multiple filter specs for one concept."""
    matching = [r[concept_name] for r in all_reaches if concept_name in r]
    if not matching:
        return None
    if len(matching) == 1:
        return matching[0]
    from graph_filter import ReachQuery
    union_sql = " UNION ALL ".join(f"SELECT * FROM ({r.sql})" for r in matching)
    return ReachQuery(sql=union_sql, cols=matching[0].cols)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Inputs")
    uploaded_file = st.file_uploader("Semantic layer Python file", type=["py"])

    # Flush all graph-specific caches when file changes
    graph_id = hash(uploaded_file.getvalue()) if uploaded_file is not None else "none"
    if st.session_state.get("graph_id") != graph_id:
        st.session_state.graph_id = graph_id
        st.session_state.schema_counts = {}
        st.session_state.edge_counts = {}
        st.session_state.instance_cache = {}
        st.session_state.expanded_concepts = set()
        st.session_state.filter_specs = []
        st.session_state.selected_node = None
        st.session_state.validation_results = None
        for k in [k for k in list(st.session_state.keys()) if k.startswith("cols_")]:
            del st.session_state[k]

    ca, cb = st.columns(2)
    if uploaded_file is None:
        ca.metric("Concepts", 0)
        cb.metric("Relationships", 0)
        st.caption("Upload a semantic layer file to begin.")
        st.stop()

    try:
        graph = _get_graph(uploaded_file)
        ca.metric("Concepts", len(graph.concepts))
        cb.metric("Relationships", len(graph.relationships))
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    st.divider()
    st.subheader("Snowflake")
    detected = "/Users/stevebertolani/software/field-pov/Demos/demo_code_assist/config/raiconfig.toml"
    config_path = st.text_input(
        "raiconfig.toml path",
        value=detected if os.path.exists(detected) else "./raiconfig.toml",
    )
    if st.button("Connect", key="btn_connect"):
        with st.spinner("Connecting…"):
            try:
                st.session_state.sf_client = SnowflakeClient.from_raiconfig(config_path)
                st.session_state.sf_connected = True
                for k in ["schema_counts", "edge_counts", "instance_cache"]:
                    st.session_state[k] = {}
                st.session_state.validation_results = None
                st.success("Connected!")
            except Exception as exc:
                st.session_state.sf_client = None
                st.session_state.sf_connected = False
                st.error(f"Connection failed: {exc}")

    if st.session_state.get("sf_connected"):
        st.success("✓ Snowflake connected")
        if st.button("Test connection", key="btn_test"):
            try:
                st.json(st.session_state.sf_client.test_connection())
            except Exception as exc:
                st.error(str(exc))

    st.divider()
    st.subheader("Source Tables")
    if st.session_state.get("sf_client"):
        if st.button("Validate Tables", key="btn_validate"):
            with st.spinner("Validating…"):
                st.session_state.validation_results = (
                    st.session_state.sf_client.validate_tables(graph.tables)
                )
        if st.session_state.validation_results:
            for r in st.session_state.validation_results:
                icon = "✅" if r.has_data else ("⚠️" if r.exists else "❌")
                st.caption(f"{icon} {r.table.split('.')[-1]}")
    else:
        st.caption("Connect to Snowflake to validate tables.")

    st.divider()
    st.subheader("Graph Layout")
    spring_length = st.slider("Spring length", 50, 500, 150, 10)
    central_gravity = st.slider("Gravity", 0.0, 1.0, 0.3, 0.05)

    st.divider()
    if st.session_state.get("sf_client"):
        if st.button("Load Counts", key="btn_counts"):
            with st.spinner("Counting rows…"):
                st.session_state.schema_counts = fetch_schema_counts(
                    st.session_state.sf_client, graph
                )
                st.session_state.edge_counts = fetch_edge_counts(
                    st.session_state.sf_client, graph
                )
            st.success("Counts loaded.")

    # Active filters display
    filter_specs: List[FilterSpec] = st.session_state.filter_specs
    if filter_specs:
        st.divider()
        st.subheader(f"Active Filters ({len(filter_specs)})")
        for i, fs in enumerate(filter_specs):
            col_pill, col_x = st.columns([4, 1])
            col_pill.caption(f"**{fs.concept}** · {fs.column} = `{fs.value}`")
            if col_x.button("✕", key=f"btn_rm_filter_{i}"):
                st.session_state.filter_specs = [
                    s for j, s in enumerate(filter_specs) if j != i
                ]
                st.rerun()
        if st.button("Reset All Filters", key="btn_reset_all"):
            st.session_state.filter_specs = []
            st.rerun()


# ── Main area ─────────────────────────────────────────────────────────────────
sf_client: Optional[SnowflakeClient] = st.session_state.get("sf_client")
filter_specs: List[FilterSpec] = st.session_state.filter_specs
expanded_concepts: set = st.session_state.expanded_concepts
schema_counts: dict = st.session_state.schema_counts
edge_counts: dict = st.session_state.edge_counts
instance_cache: dict = st.session_state.instance_cache

# Invalidate instance cache when filter set changes (forces re-fetch with new filter)
current_fkey = _filter_key(filter_specs)
if st.session_state.get("_last_filter_key", "") != current_fkey:
    st.session_state["_last_filter_key"] = current_fkey
    st.session_state.instance_cache = {}
    st.session_state.expanded_concepts = set()
    instance_cache = {}
    expanded_concepts = set()

# BFS reach per filter spec — pure SQL, no Snowflake queries
all_reaches = [compute_reach(graph, fs) for fs in filter_specs]

# Union activity: node/edge is active if reachable from ANY filter spec
if filter_specs and sf_client:
    node_active: dict = {name: False for name in graph.concepts}
    edge_active: dict = {idx: False for idx, _ in enumerate(graph.relationships)}
    for fs in filter_specs:
        na, ea = compute_activity(graph, sf_client, fs)
        for k, v in na.items():
            node_active[k] = node_active[k] or v
        for k, v in ea.items():
            edge_active[k] = edge_active[k] or v
else:
    node_active = {}
    edge_active = {}

# Root concepts = anchor of any active filter spec
root_concepts = {fs.concept for fs in filter_specs}

# ── Build graph elements ───────────────────────────────────────────────────────
nodes: list[Node] = []
edges: list[Edge] = []

for concept in graph.concepts.values():
    active = node_active.get(concept.name, True)
    is_root = concept.name in root_concepts
    is_expanded = concept.name in expanded_concepts

    color = "#E4A951" if is_root else ("#c9c9c9" if not active else "#0e5a6f")
    count = schema_counts.get(concept.name)
    label = f"{concept.name} ({fmt_count(count)})" if count is not None else concept.name

    tip_lines = [f"Table: {concept.base_table or '?'}", f"ID cols: {', '.join(concept.id_columns) or '?'}"]
    if count is not None:
        tip_lines.insert(0, f"Rows: {fmt_count(count)}")
    if is_expanded:
        tip_lines.append("[expanded — click an instance to add as filter]")

    nodes.append(Node(
        id=concept.name, label=label, size=32 if is_expanded else 26,
        color=color, title="\n".join(tip_lines), shape="dot",
    ))

    if is_expanded:
        inst_df: Optional[pd.DataFrame] = instance_cache.get(concept.name)
        if inst_df is not None and not inst_df.empty:
            pk_col = concept.id_columns[0] if concept.id_columns else None
            actual_pk = _find_col(inst_df, pk_col) if pk_col else None
            inst_color = "#5fa8c0" if active else "#c9c9c9"

            if actual_pk:
                display_col = next(
                    (c for c in inst_df.columns if c != actual_pk and inst_df[c].dtype == object),
                    None,
                )
                for _, row in inst_df.iterrows():
                    pk_val = str(row[actual_pk])
                    inst_id = make_instance_id(concept.name, pk_val)
                    short = f"#{pk_val}"
                    if display_col:
                        dv = str(row[display_col])[:18]
                        if dv not in ("None", "nan", ""):
                            short = f"#{pk_val} {dv}"
                    tooltip = "\n".join(f"{c}: {row[c]}" for c in inst_df.columns)
                    nodes.append(Node(
                        id=inst_id, label=short, size=14,
                        color=inst_color, shape="square", title=tooltip,
                    ))
                    edges.append(Edge(
                        source=inst_id, target=concept.name,
                        color="#a8c8d8", width=1, dashes=True,
                    ))

for idx, rel in enumerate(graph.relationships):
    active = edge_active.get(idx, True)
    count = edge_counts.get(idx)
    label = f"{rel.name} ({fmt_count(count)})" if count is not None else rel.name
    edges.append(Edge(
        source=rel.source, target=rel.target, label=label,
        color="#3c3c3c" if active else "#d0d0d0",
        width=2 if active else 1, smooth=True,
    ))

config = Config(
    width="100%", height=620, directed=True, physics=True,
    hierarchical=False, nodeHighlightBehavior=True,
    highlightColor="#E4A951", collapsible=False, maxZoom=4, minZoom=0.2,
)
config.physics["barnesHut"] = {
    "springLength": spring_length, "springConstant": 0.04,
    "centralGravity": central_gravity, "damping": 0.09, "avoidOverlap": 0.5,
}

# Banner
if filter_specs:
    active_cnt = sum(1 for v in node_active.values() if v)
    parts = [f"**{fs.concept}** `{fs.column}`=`{fs.value}`" for fs in filter_specs]
    st.info(
        f"🔵 {' ∪ '.join(parts)} — {active_cnt}/{len(graph.concepts)} concepts reachable"
    )

raw = agraph(nodes=nodes, edges=edges, config=config)
clicked = _get_selected_node(raw)

if clicked:
    st.session_state.selected_node = clicked
elif raw is not None and not clicked:
    st.session_state.selected_node = None

display_node: Optional[str] = st.session_state.selected_node
node_is_instance = display_node and is_instance_node(display_node, graph)

st.caption(
    "💡 **Circles** = concept types. **Squares** = data instances (click to add as filter). "
    "Filters combine with union (∪) — a node stays active if reachable from any filter anchor."
)

st.divider()

# ── Bottom section ────────────────────────────────────────────────────────────
col_ctrl, col_data = st.columns([1, 2])

with col_ctrl:
    # ── INSTANCE NODE selected ────────────────────────────────────────────
    if node_is_instance:
        parsed = parse_instance_id(display_node)
        if parsed:
            concept_name, pk_val = parsed
            concept_info = graph.concepts.get(concept_name)
            pk_col = concept_info.id_columns[0] if concept_info and concept_info.id_columns else None

            st.subheader(f"{concept_name} instance")
            st.write(f"**ID:** `{pk_val}`")
            if concept_info:
                st.caption(f"Table: `{concept_info.base_table}`")

            if sf_client and pk_col:
                new_fs = FilterSpec(concept=concept_name, column=pk_col, value=pk_val)
                already = any(
                    s.concept == new_fs.concept and s.column == new_fs.column and s.value == new_fs.value
                    for s in filter_specs
                )
                if already:
                    st.info("This instance is already an active filter.")
                else:
                    st.write("**Add to filter set:**")
                    st.caption(f"{concept_name} · `{pk_col}` = `{pk_val}`")
                    if st.button("➕ Add filter", key="btn_inst_filter", type="primary"):
                        st.session_state.filter_specs = filter_specs + [new_fs]
                        st.rerun()
            elif not sf_client:
                st.caption("Connect to Snowflake to enable filtering.")

            if st.button(f"← Back to {concept_name}", key="btn_back_to_schema"):
                st.session_state.selected_node = concept_name
                st.rerun()

    # ── SCHEMA NODE selected ─────────────────────────────────────────────
    elif display_node:
        concept = graph.concepts.get(display_node)
        st.subheader(f"Concept: {display_node}")

        if concept:
            is_expanded = display_node in expanded_concepts
            count = schema_counts.get(display_node)

            st.write(f"**Table:** `{concept.base_table or 'unknown'}`")
            st.write(f"**ID columns:** `{', '.join(concept.id_columns) or 'none'}`")
            if count is not None:
                st.write(f"**Row count:** {fmt_count(count)}")

            rels = [r for r in graph.relationships if r.source == display_node or r.target == display_node]
            if rels:
                with st.expander(f"Relationships ({len(rels)})", expanded=False):
                    for r in rels:
                        other = r.target if r.source == display_node else r.source
                        ec = edge_counts.get(graph.relationships.index(r))
                        ec_str = f" ({fmt_count(ec)})" if ec is not None else ""
                        st.write(f"· `{r.name}{ec_str}` → {other}")

            st.divider()

            # ── Expand / collapse ────────────────────────────────────────
            if sf_client and concept.base_table:
                combined_rq = _combined_reach(all_reaches, display_node)
                is_filtered_expand = bool(filter_specs and combined_rq)

                if is_expanded:
                    cached_df = instance_cache.get(display_node)
                    n_inst = len(cached_df) if cached_df is not None else 0
                    note = " · filtered to active filters" if is_filtered_expand else ""
                    st.write(f"**{n_inst} instances shown{note}**")
                    if st.button("▲ Collapse", key="btn_collapse"):
                        st.session_state.expanded_concepts.discard(display_node)
                        st.rerun()
                else:
                    label = f"▼ Expand instances {'(filtered) ' if is_filtered_expand else ''}(up to {INSTANCE_LIMIT})"
                    if st.button(label, key="btn_expand", type="primary"):
                        if display_node not in instance_cache:
                            with st.spinner("Fetching instances…"):
                                if is_filtered_expand:
                                    df = fetch_filtered_instances(
                                        sf_client, concept, combined_rq.sql, combined_rq.cols
                                    )
                                else:
                                    df = fetch_instances(sf_client, concept)
                                st.session_state.instance_cache[display_node] = df
                        st.session_state.expanded_concepts.add(display_node)
                        st.rerun()
            elif sf_client and not concept.base_table:
                st.caption("No base table — cannot expand.")
            else:
                st.caption("Connect to Snowflake to expand instances.")

            # ── Manual filter form ────────────────────────────────────────
            st.divider()
            st.write("**Add manual filter:**")
            if sf_client:
                cols_key = f"cols_{display_node}"
                if cols_key not in st.session_state and concept.base_table:
                    try:
                        st.session_state[cols_key] = sf_client.get_columns(concept.base_table)
                    except Exception:
                        st.session_state[cols_key] = []
                columns = st.session_state.get(cols_key, [])

                filter_col = st.selectbox("Column", options=columns or ["(no columns)"], key="filter_col_select")
                filter_val = st.text_input("Value", key="filter_val_input")
                fa, fb = st.columns(2)
                if fa.button("➕ Add", key="btn_apply", type="primary"):
                    if filter_col and filter_col != "(no columns)" and filter_val:
                        new_fs = FilterSpec(concept=display_node, column=filter_col, value=filter_val)
                        if not any(
                            s.concept == new_fs.concept and s.column == new_fs.column and s.value == new_fs.value
                            for s in filter_specs
                        ):
                            st.session_state.filter_specs = filter_specs + [new_fs]
                            st.rerun()
                if fb.button("Reset All", key="btn_reset_main"):
                    st.session_state.filter_specs = []
                    st.rerun()
            else:
                st.caption("Connect to Snowflake to enable filters.")

    else:
        st.info("Click a concept node to inspect it, expand its instances, and add filters.")

with col_data:
    if node_is_instance:
        parsed = parse_instance_id(display_node)
        if parsed:
            concept_name, pk_val = parsed
            concept_info = graph.concepts.get(concept_name)
            inst_df = instance_cache.get(concept_name)

            st.subheader("Instance data")
            if inst_df is not None and concept_info:
                pk_col = concept_info.id_columns[0] if concept_info.id_columns else None
                if pk_col:
                    actual_pk = _find_col(inst_df, pk_col)
                    if actual_pk:
                        row = inst_df[inst_df[actual_pk].astype(str) == pk_val]
                        if not row.empty:
                            kv = row.iloc[0].to_frame("Value").reset_index()
                            kv.columns = ["Column", "Value"]
                            st.dataframe(kv, width="stretch")
                        else:
                            st.info("Row not found in cached data.")
            else:
                st.info("Instance data not cached — expand the parent concept first.")

    elif display_node:
        concept = graph.concepts.get(display_node)
        st.subheader(f"Sample data: {display_node}")

        if concept:
            st.caption(f"Table: `{concept.base_table}` · ID cols: `{', '.join(concept.id_columns)}`")
            if sf_client and concept.base_table:
                try:
                    # If this concept is a filter anchor, show filtered rows
                    my_filters = [fs for fs in filter_specs if fs.concept == concept.name]
                    if my_filters:
                        fs = my_filters[0]
                        literal = fs.value.replace("'", "''")
                        sql = (
                            f"SELECT * FROM {concept.base_table} "
                            f'WHERE "{fs.column}" = \'{literal}\' LIMIT 25'
                        )
                    else:
                        sql = f"SELECT * FROM {concept.base_table} LIMIT 25"

                    with st.spinner("Loading…"):
                        df = sf_client.query_df(sql)

                    if df.empty:
                        st.info("No rows returned.")
                    else:
                        st.dataframe(df, width="stretch")
                except Exception as exc:
                    st.error(f"Query failed: {exc}")
            elif not sf_client:
                st.info("Connect to Snowflake to view sample data.")

    else:
        st.info("Select a concept node above to see its data here.")
