from __future__ import annotations

import os
import tempfile
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st
from streamlit_agraph import Config, Edge, Node, agraph

from graph_counts import (
    INSTANCE_LIMIT,
    fetch_edge_counts,
    fetch_instances,
    fetch_schema_counts,
    fmt_count,
    is_instance_node,
    make_instance_id,
    parse_instance_id,
)
from semantic_parser import SemanticGraph, parse_semantic_text
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
    ("sample_cache", {}),   # concept_name → DataFrame (persists across reruns)
    ("schema_counts", {}),
    ("edge_counts", {}),
    ("selected_node", None),
    ("validation_results", None),
]:
    if _key not in st.session_state:
        st.session_state[_key] = _default


def _relationalai_available() -> bool:
    try:
        import relationalai  # noqa: F401
        return True
    except ImportError:
        return False


@st.cache_data(show_spinner=False)
def _load_graph(text: str) -> Tuple[SemanticGraph, List[str]]:
    return parse_semantic_text(text)


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


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Inputs")

    if not _relationalai_available():
        st.warning(
            "⚠️ `relationalai` is not installed — using the regex fallback parser. "
            "Results may be incomplete for non-standard file formats. "
            "Install with `pip install relationalai` for full accuracy.",
            icon=None,
        )

    uploaded_file = st.file_uploader("Semantic layer Python file", type=["py"])

    graph_id = hash(uploaded_file.getvalue()) if uploaded_file is not None else "none"
    if st.session_state.get("graph_id") != graph_id:
        st.session_state.graph_id = graph_id
        st.session_state.schema_counts = {}
        st.session_state.edge_counts = {}
        st.session_state.instance_cache = {}
        st.session_state.sample_cache = {}
        st.session_state.expanded_concepts = set()
        st.session_state.selected_node = None
        st.session_state.validation_results = None

    ca, cb = st.columns(2)
    if uploaded_file is None:
        ca.metric("Concepts", 0)
        cb.metric("Relationships", 0)
        st.caption("Upload a semantic layer file to begin.")
        st.stop()

    try:
        graph, parse_diags = _load_graph(uploaded_file.getvalue().decode("utf-8", errors="ignore"))
        ca.metric("Concepts", len(graph.concepts))
        cb.metric("Relationships", len(graph.relationships))
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    if not graph.concepts:
        st.warning("No concepts found — the file may use an unsupported format.")
    if not graph.relationships:
        st.warning("No relationships found.")
    if not graph.tables:
        st.warning("No source tables found — Validate Tables will have nothing to check.")

    with st.expander("Parse diagnostics", expanded=False):
        st.markdown("**Concepts**")
        if graph.concepts:
            for name, c in graph.concepts.items():
                st.caption(f"· {name} — table: `{c.base_table or '?'}` id_cols: `{c.id_columns or []}`")
        else:
            st.caption("none")
        st.markdown("**Relationships**")
        if graph.relationships:
            for r in graph.relationships:
                st.caption(f"· `{r.name}` {r.source} → {r.target} (rel_table: `{r.rel_table or '?'}`)")
        else:
            st.caption("none")
        st.markdown("**Source tables**")
        if graph.tables:
            for t in graph.tables:
                st.caption(f"· `{t}`")
        else:
            st.caption("none")
        st.divider()
        st.markdown("**Full parser log**")
        st.code("\n".join(parse_diags), language=None)

    st.divider()
    st.subheader("Snowflake")
    raiconfig_file = st.file_uploader(
        "raiconfig (.toml or .yaml)", type=["toml", "yaml", "yml"], key="raiconfig_upload"
    )
    if st.button("Connect", key="btn_connect", disabled=raiconfig_file is None):
        with st.spinner("Connecting…"):
            try:
                suffix = os.path.splitext(raiconfig_file.name)[1]
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(raiconfig_file.getvalue())
                    tmp_path = tmp.name
                st.session_state.sf_client = SnowflakeClient.from_raiconfig(tmp_path)
                os.unlink(tmp_path)
                st.session_state.sf_connected = True
                for k in ["schema_counts", "edge_counts", "instance_cache", "sample_cache"]:
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


# ── Main area ─────────────────────────────────────────────────────────────────
schema_counts: dict = st.session_state.schema_counts
edge_counts: dict = st.session_state.edge_counts
expanded_concepts: set = st.session_state.expanded_concepts
instance_cache: dict = st.session_state.instance_cache

# ── Build graph elements ───────────────────────────────────────────────────────
nodes: list[Node] = []
edges: list[Edge] = []

for concept in graph.concepts.values():
    is_expanded = concept.name in expanded_concepts
    count = schema_counts.get(concept.name)
    label = f"{concept.name} ({fmt_count(count)})" if count is not None else concept.name

    tip_lines = [
        f"Table: {concept.base_table or '?'}",
        f"ID cols: {', '.join(concept.id_columns) or '?'}",
    ]
    if count is not None:
        tip_lines.insert(0, f"Rows: {fmt_count(count)}")
    if is_expanded:
        tip_lines.append("[expanded — click an instance to view its data]")

    nodes.append(Node(
        id=concept.name, label=label, size=32 if is_expanded else 26,
        color="#0e5a6f", title="\n".join(tip_lines), shape="dot",
    ))

    if is_expanded:
        inst_df: Optional[pd.DataFrame] = instance_cache.get(concept.name)
        if inst_df is not None and not inst_df.empty:
            pk_col = concept.id_columns[0] if concept.id_columns else None
            actual_pk = _find_col(inst_df, pk_col) if pk_col else None

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
                        color="#5fa8c0", shape="square", title=tooltip,
                    ))
                    edges.append(Edge(
                        source=inst_id, target=concept.name,
                        color="#a8c8d8", width=1, dashes=True,
                    ))

for idx, rel in enumerate(graph.relationships):
    count = edge_counts.get(idx)
    label = f"{rel.name} ({fmt_count(count)})" if count is not None else rel.name
    edges.append(Edge(
        source=rel.source, target=rel.target, label=label,
        color="#3c3c3c", width=2, smooth=True,
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

raw = agraph(nodes=nodes, edges=edges, config=config)
clicked = _get_selected_node(raw)

if clicked:
    st.session_state.selected_node = clicked
elif raw is not None and not clicked:
    st.session_state.selected_node = None

st.caption("💡 **Circles** = concept types · **Squares** = data instances (click to view row data)")

st.divider()


# ── Bottom panel (fragment) ───────────────────────────────────────────────────
# @st.fragment means button clicks here rerun only this function, not the full
# app (and therefore not the agraph). Expand/collapse still use
# st.rerun(scope="app") because those must update the graph node list.

@st.fragment
def _bottom_panel(graph: SemanticGraph) -> None:
    sf_client: Optional[SnowflakeClient] = st.session_state.get("sf_client")
    expanded_concepts: set = st.session_state.expanded_concepts
    schema_counts: dict = st.session_state.schema_counts
    edge_counts: dict = st.session_state.edge_counts
    instance_cache: dict = st.session_state.instance_cache
    sample_cache: dict = st.session_state.sample_cache

    display_node: Optional[str] = st.session_state.selected_node
    node_is_instance = bool(display_node and is_instance_node(display_node, graph))

    col_ctrl, col_data = st.columns([1, 2])

    with col_ctrl:
        # ── INSTANCE NODE selected ────────────────────────────────────────
        if node_is_instance:
            parsed = parse_instance_id(display_node)
            if parsed:
                concept_name, pk_val = parsed
                concept_info = graph.concepts.get(concept_name)

                st.subheader(f"{concept_name} instance")
                st.write(f"**ID:** `{pk_val}`")
                if concept_info:
                    st.caption(f"Table: `{concept_info.base_table}`")

                # Panel-only navigation: no full app rerun needed
                if st.button(f"← Back to {concept_name}", key="btn_back_to_schema"):
                    st.session_state.selected_node = concept_name

        # ── SCHEMA NODE selected ──────────────────────────────────────────
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

                rels = [
                    r for r in graph.relationships
                    if r.source == display_node or r.target == display_node
                ]
                if rels:
                    with st.expander(f"Relationships ({len(rels)})", expanded=False):
                        for r in rels:
                            other = r.target if r.source == display_node else r.source
                            ec = edge_counts.get(graph.relationships.index(r))
                            ec_str = f" ({fmt_count(ec)})" if ec is not None else ""
                            st.write(f"· `{r.name}{ec_str}` → {other}")

                st.divider()

                if sf_client and concept.base_table:
                    if is_expanded:
                        cached_df = instance_cache.get(display_node)
                        n_inst = len(cached_df) if cached_df is not None else 0
                        st.write(f"**{n_inst} instances shown**")
                        if st.button("▲ Collapse", key="btn_collapse"):
                            st.session_state.expanded_concepts.discard(display_node)
                            st.rerun(scope="app")  # must update graph
                    else:
                        if st.button(
                            f"▼ Expand instances (up to {INSTANCE_LIMIT})",
                            key="btn_expand", type="primary",
                        ):
                            if display_node not in instance_cache:
                                with st.spinner("Fetching instances…"):
                                    st.session_state.instance_cache[display_node] = (
                                        fetch_instances(sf_client, concept)
                                    )
                            st.session_state.expanded_concepts.add(display_node)
                            st.rerun(scope="app")  # must update graph
                elif sf_client and not concept.base_table:
                    st.caption("No base table — cannot expand.")
                else:
                    st.caption("Connect to Snowflake to expand instances.")

        else:
            st.info("Click a concept node to inspect it and expand its instances.")

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
                st.caption(
                    f"Table: `{concept.base_table}` · "
                    f"ID cols: `{', '.join(concept.id_columns)}`"
                )
                if sf_client and concept.base_table:
                    # Fetch once and cache — avoids a Snowflake round trip on
                    # every rerun (e.g. expand/collapse, back button, etc.)
                    if display_node not in sample_cache:
                        try:
                            with st.spinner("Loading…"):
                                df = sf_client.query_df(
                                    f"SELECT * FROM {concept.base_table} LIMIT 25"
                                )
                            sample_cache[display_node] = df
                            st.session_state.sample_cache = sample_cache
                        except Exception as exc:
                            st.error(f"Query failed: {exc}")
                            sample_cache[display_node] = None

                    df = sample_cache.get(display_node)
                    if df is None:
                        pass  # error already shown above
                    elif df.empty:
                        st.info("No rows returned.")
                    else:
                        st.dataframe(df, width="stretch")
                elif not sf_client:
                    st.info("Connect to Snowflake to view sample data.")

        else:
            st.info("Select a concept node above to see its data here.")


_bottom_panel(graph)
