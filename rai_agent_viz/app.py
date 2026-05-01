"""RAI Cortex Agent Chat + GPU Wafer Zone Analysis prototype."""
import streamlit as st
import streamlit.components.v1 as components

import pandas as pd
import numpy as np
import networkx as nx
import gravis as gv
import matplotlib.pyplot as plt
import matplotlib.patches as patches

st.set_page_config(
    page_title="RAI Agent + Wafer Analysis",
    layout="wide",
    page_icon="🤖",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    demo_mode = st.toggle("Demo Mode (no Snowflake)", value=True)
    st.caption("Demo mode uses mock agent responses and synthetic graph data.")

    st.divider()
    st.header("Agent Configuration")
    agent_name   = st.text_input("Agent Name", value="", placeholder="my_cortex_agent", disabled=demo_mode)
    agent_db     = st.text_input("Database",   value="SNOWFLAKE_INTELLIGENCE", disabled=demo_mode)
    agent_schema = st.text_input("Schema",     value="AGENTS", disabled=demo_mode)

    st.divider()
    st.header("Graph Tables")
    st.caption("Populated by the agent after each response." if not demo_mode else "Mock graph auto-generated in demo mode.")
    node_table      = st.text_input("Node Table", value="", placeholder="DB.SCHEMA.NODES", disabled=demo_mode)
    edge_table      = st.text_input("Edge Table", value="", placeholder="DB.SCHEMA.EDGES", disabled=demo_mode)
    node_id_col     = st.text_input("Node ID column",    value="NODE_HASH", disabled=demo_mode)
    node_label_col  = st.text_input("Node label column", value="SN", disabled=demo_mode)
    node_type_col   = st.text_input("Node type column",  value="NODE_TYPE", disabled=demo_mode)
    nhops = st.slider("N hops to display", 1, 5, 2)

    st.divider()
    if st.button("Clear Conversation", use_container_width=True):
        for k in ["messages", "_chat_obj", "_sf_conn", "_http_client", "_mock_turn"]:
            st.session_state.pop(k, None)
        st.cache_data.clear()
        st.rerun()


# ── Mock data ─────────────────────────────────────────────────────────────────
_MOCK_RESPONSES = [
    (
        "I found **3 GPU OAM assemblies** in the knowledge graph. Each OAM contains one ASIC die, "
        "four HBM stacks, a substrate, stiffener, and heatsink. I've written the component graph "
        "to the node and edge tables — you can explore relationships in the graph viewer below.",
        "query_rai_model",
    ),
    (
        "The ASIC die **AMD-MI300X-004** shows a yield flag from wafer zone analysis. "
        "It was sourced from the **Edge zone** of wafer W2024-0312, which historically has a "
        "25% higher defect rate than the center zone. Two of its four HBM stacks share the same "
        "wafer lot. I've updated the graph to highlight these components.",
        "query_rai_model",
    ),
    (
        "Tracing the supply chain for **OAM-UBB8-007**: the ASIC was fabricated on 2024-02-14 "
        "at TSMC N5, passed final test, and was assembled into UBB board UBB8-007 on 2024-03-01. "
        "No open quality escapes are linked to this serial number.",
        "query_rai_model",
    ),
    (
        "I found **12 components** with unresolved yield flags across 3 wafer lots. "
        "The most common failure mode is **HBM interface margin** — present in 8 of 12 cases. "
        "Graph updated to show affected assemblies.",
        "query_rai_model",
    ),
]

def _mock_graph() -> tuple[nx.Graph, dict, dict]:
    """Generate a synthetic GPU component hierarchy graph."""
    G = nx.Graph()
    nodes = [
        # (id, label, type)
        ("OAM-001", "OAM-001", "OAM"),
        ("OAM-002", "OAM-002", "OAM"),
        ("OAM-003", "OAM-003", "OAM"),
        ("UBB-001", "UBB-001", "UBB0"),
        ("UBB-002", "UBB-002", "UBB8"),
        ("UBB-003", "UBB-003", "UBB0"),
        ("ASIC-001", "MI300X-001", "ASIC"),
        ("ASIC-002", "MI300X-002", "ASIC"),
        ("ASIC-003", "MI300X-003", "ASIC"),
        ("HBM-001",  "HBM-S0-001", "HBM"),
        ("HBM-002",  "HBM-S1-001", "HBM"),
        ("HBM-003",  "HBM-S2-001", "HBM"),
        ("HBM-004",  "HBM-S3-001", "HBM"),
        ("HBM-005",  "HBM-S0-002", "HBM"),
        ("HBM-006",  "HBM-S1-002", "HBM"),
        ("HBM-007",  "HBM-S2-002", "HBM"),
        ("HBM-008",  "HBM-S3-002", "HBM"),
        ("HBM-009",  "HBM-S0-003", "HBM"),
        ("HBM-010",  "HBM-S1-003", "HBM"),
        ("HBM-011",  "HBM-S2-003", "HBM"),
        ("HBM-012",  "HBM-S3-003", "HBM"),
        ("SUB-001", "SUBSTRATE-001", "SUBSTRATE"),
        ("SUB-002", "SUBSTRATE-002", "SUBSTRATE"),
        ("SUB-003", "SUBSTRATE-003", "SUBSTRATE"),
        ("STF-001", "STIFFENER-001", "STIFFENER"),
        ("STF-002", "STIFFENER-002", "STIFFENER"),
        ("HSK-001", "HEATSINK-001", "HEATSINK"),
        ("HSK-002", "HEATSINK-002", "HEATSINK"),
        ("HSK-003", "HEATSINK-003", "HEATSINK"),
        ("RECON-001", "RECON-BATCH-2024-03", "RECON"),
    ]
    edges = [
        # OAM → UBB
        ("OAM-001", "UBB-001"), ("OAM-002", "UBB-002"), ("OAM-003", "UBB-003"),
        # UBB → ASIC
        ("UBB-001", "ASIC-001"), ("UBB-002", "ASIC-002"), ("UBB-003", "ASIC-003"),
        # UBB → HBM
        ("UBB-001", "HBM-001"), ("UBB-001", "HBM-002"), ("UBB-001", "HBM-003"), ("UBB-001", "HBM-004"),
        ("UBB-002", "HBM-005"), ("UBB-002", "HBM-006"), ("UBB-002", "HBM-007"), ("UBB-002", "HBM-008"),
        ("UBB-003", "HBM-009"), ("UBB-003", "HBM-010"), ("UBB-003", "HBM-011"), ("UBB-003", "HBM-012"),
        # OAM → substrate/stiffener/heatsink
        ("OAM-001", "SUB-001"), ("OAM-001", "STF-001"), ("OAM-001", "HSK-001"),
        ("OAM-002", "SUB-002"), ("OAM-002", "STF-002"), ("OAM-002", "HSK-002"),
        ("OAM-003", "SUB-003"), ("OAM-003", "HSK-003"),
        # RECON batch links
        ("RECON-001", "ASIC-001"), ("RECON-001", "ASIC-002"),
    ]

    id_to_label = {}
    for nid, label, ntype in nodes:
        G.add_node(nid, label=label, color=_node_color(ntype), size=12)
        id_to_label[nid] = label
    for a, b in edges:
        G.add_edge(a, b)

    label_to_id = {v: k for k, v in id_to_label.items()}
    return G, id_to_label, label_to_id


# ── Snowflake connection ──────────────────────────────────────────────────────
def _get_sf_conn():
    if "_sf_conn" not in st.session_state:
        import snowflake.connector
        cfg = st.secrets["snowflake"]
        kw = dict(
            user=cfg["user"],
            password=cfg["password"],
            account=cfg["account"],
            warehouse=cfg["warehouse"],
            role=cfg.get("role", "ACCOUNTADMIN"),
        )
        if cfg.get("authenticator"):
            kw["authenticator"] = cfg["authenticator"]
        if cfg.get("passcode"):
            kw["passcode"] = cfg["passcode"]
        st.session_state["_sf_conn"] = snowflake.connector.connect(**kw)
    return st.session_state["_sf_conn"]


# ── Agent helpers ─────────────────────────────────────────────────────────────
def _get_chat(name: str, db: str, schema: str):
    if "_chat_obj" not in st.session_state:
        try:
            from relationalai.agent.cortex.chat import CortexAgentChat
            from relationalai.agent.cortex.api.client import http_client
            conn = _get_sf_conn()
            client = http_client(conn)
            st.session_state["_http_client"] = client
            st.session_state["_chat_obj"] = CortexAgentChat(
                client=client, name=name, db=db, schema=schema
            )
        except Exception as exc:
            st.error(f"Failed to initialize agent: {exc}")
            return None
    return st.session_state["_chat_obj"]


# ── Graph helpers ─────────────────────────────────────────────────────────────
_NODE_COLORS = {
    "RECON": "purple",      "ASIC": "#27ae60",       "HBM": "#90EE90",
    "DIE": "orange",        "OAM": "orange",          "HEATSINK": "#888",
    "SMC": "#888",          "UBB0": "#4a9eff",        "UBB8": "royalblue",
    "SUBSTRATE": "#90D5FF", "STIFFENER": "#b0d0ff",
}

def _node_color(node_type: str) -> str:
    return _NODE_COLORS.get(str(node_type).upper(), "gray")


@st.cache_data(show_spinner=False)
def _load_graph_data(node_tbl: str, edge_tbl: str, id_col: str, label_col: str, type_col: str):
    conn = _get_sf_conn()
    nodes_df = pd.read_sql(f"SELECT * FROM {node_tbl} LIMIT 5000", conn)
    edges_df = pd.read_sql(f"SELECT * FROM {edge_tbl} LIMIT 20000", conn)
    nodes_df.columns = nodes_df.columns.str.upper()
    edges_df.columns = edges_df.columns.str.upper()
    id_col    = id_col.upper()
    label_col = label_col.upper()
    type_col  = type_col.upper() if type_col else ""

    from_col = next((c for c in edges_df.columns if "FROM" in c), edges_df.columns[0])
    to_col   = next((c for c in edges_df.columns if "TO" in c and c != from_col), edges_df.columns[1])

    G = nx.Graph()
    for _, row in edges_df.iterrows():
        G.add_edge(row[from_col], row[to_col])
    for _, row in nodes_df.iterrows():
        nid  = row[id_col]
        lbl  = str(row[label_col]) if label_col in row else str(nid)
        color = _node_color(row[type_col]) if type_col and type_col in nodes_df.columns else "gray"
        G.nodes[nid].update({"label": lbl, "size": 10, "color": color})

    id_to_label = (
        dict(zip(nodes_df[id_col], nodes_df[label_col]))
        if label_col in nodes_df.columns else {}
    )
    label_to_id = {v: k for k, v in id_to_label.items()}
    return G, id_to_label, label_to_id


def _render_graph(G, id_to_label, label_to_id, selected_label: str, n_hops: int):
    selected_id = label_to_id.get(selected_label)
    if selected_id is None:
        st.warning("Selected node not found in graph.")
        return
    lengths = nx.single_source_shortest_path_length(G, selected_id, cutoff=n_hops)
    subG = G.subgraph(list(lengths.keys())).copy()
    fig = gv.d3(
        subG,
        node_hover_neighborhood=True,
        show_node_label=True,
        node_label_data_source="label",
        graph_height=520,
    )
    components.html(fig.to_html(), height=560)


# ── Wafer helpers ─────────────────────────────────────────────────────────────
def _wafer_zone(r: float, wafer_r: float) -> str:
    if r < wafer_r * 0.33:
        return "Center"
    elif r < wafer_r * 0.67:
        return "Middle"
    return "Edge"


@st.cache_data(show_spinner=False)
def _make_wafer_df(seed, wafer_r, die_w, die_h, edge_excl, fc, fm, fe):
    rng = np.random.default_rng(seed)
    xs = np.arange(-wafer_r + die_w / 2, wafer_r, die_w)
    ys = np.arange(-wafer_r + die_h / 2, wafer_r, die_h)
    dies = []
    for x in xs:
        for y in ys:
            corners = [(x + dx, y + dy) for dx in [-die_w/2, die_w/2] for dy in [-die_h/2, die_h/2]]
            if all(cx**2 + cy**2 <= (wafer_r - edge_excl)**2 for cx, cy in corners):
                r = float(np.sqrt(x**2 + y**2))
                zone = _wafer_zone(r, wafer_r)
                fp = {"Center": fc, "Middle": fm, "Edge": fe}[zone]
                status = "FAIL" if rng.random() < fp else "PASS"
                dies.append({"x": x, "y": y, "r": round(r, 1), "zone": zone, "status": status})
    return pd.DataFrame(dies)


def _plot_wafer(df: pd.DataFrame, wafer_r: float, die_w: float, die_h: float) -> plt.Figure:
    fig, (ax_map, ax_bar) = plt.subplots(1, 2, figsize=(14, 7))
    fig.patch.set_facecolor("#0e1117")
    for ax in (ax_map, ax_bar):
        ax.set_facecolor("#0e1117")
        ax.tick_params(colors="white")
        ax.xaxis.label.set_color("white")
        ax.yaxis.label.set_color("white")
        ax.title.set_color("white")
        for spine in ax.spines.values():
            spine.set_edgecolor("#444")

    ax_map.set_aspect("equal")
    ax_map.add_patch(plt.Circle((0, 0), wafer_r, fill=False, color="white", lw=2))
    for ring_r, ring_c in [(wafer_r * 0.33, "#4a9eff"), (wafer_r * 0.67, "#ff9f40")]:
        ax_map.add_patch(plt.Circle((0, 0), ring_r, fill=False, color=ring_c, lw=1.2, ls="--", alpha=0.7))

    color_map = {"PASS": "#27ae60", "FAIL": "#e74c3c"}
    for _, row in df.iterrows():
        ax_map.add_patch(patches.Rectangle(
            (row["x"] - die_w / 2, row["y"] - die_h / 2), die_w, die_h,
            linewidth=0.2, edgecolor="#0e1117",
            facecolor=color_map[row["status"]], alpha=0.9,
        ))

    ax_map.add_patch(patches.Rectangle((-8, -wafer_r - 4), 16, 4, color="white"))
    ax_map.set_xlim(-wafer_r * 1.15, wafer_r * 1.15)
    ax_map.set_ylim(-wafer_r * 1.2,  wafer_r * 1.15)
    ax_map.set_title("Wafer Die Map", fontsize=13, fontweight="bold")
    ax_map.set_xlabel("X (mm)")
    ax_map.set_ylabel("Y (mm)")

    from matplotlib.patches import Patch
    ax_map.legend(
        handles=[
            Patch(fc="#27ae60", label="PASS"),
            Patch(fc="#e74c3c", label="FAIL"),
            Patch(fc="none", ec="#4a9eff", ls="--", lw=1.5, label="Center boundary"),
            Patch(fc="none", ec="#ff9f40", ls="--", lw=1.5, label="Middle boundary"),
        ],
        loc="upper right", fontsize=8,
        facecolor="#1e2130", labelcolor="white", framealpha=0.8,
    )

    zone_order = ["Center", "Middle", "Edge"]
    zone_yield  = df.groupby("zone")["status"].apply(lambda s: (s == "PASS").mean() * 100).reindex(zone_order)
    zone_counts = df.groupby("zone").size().reindex(zone_order)
    bars = ax_bar.bar(zone_yield.index, zone_yield.values,
                      color=["#4a9eff", "#ff9f40", "#e74c3c"], edgecolor="#0e1117", width=0.5)
    for bar, count, yld in zip(bars, zone_counts.values, zone_yield.values):
        ax_bar.text(
            bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.8,
            f"{yld:.1f}%\n({int(count)} dies)",
            ha="center", va="bottom", fontsize=9, color="white",
        )
    avg = zone_yield.mean()
    ax_bar.axhline(y=avg, color="#aaa", ls="--", lw=1.2, label=f"Avg {avg:.1f}%")
    ax_bar.set_ylim(0, 115)
    ax_bar.set_ylabel("Yield (%)")
    ax_bar.set_title("Zone Yield Breakdown", fontsize=13, fontweight="bold")
    ax_bar.legend(fontsize=9, facecolor="#1e2130", labelcolor="white", framealpha=0.8)

    plt.tight_layout(pad=2)
    return fig


# ── App layout ────────────────────────────────────────────────────────────────
st.title("RAI Agent Chat + GPU Wafer Analysis")
if demo_mode:
    st.info("**Demo Mode** — mock agent responses and synthetic graph data. Toggle off in the sidebar to connect to Snowflake.", icon="🧪")

tab_chat, tab_wafer = st.tabs(["🤖 Agent Chat", "🔬 Wafer Zone Analysis"])


# ── Tab 1: Agent Chat ─────────────────────────────────────────────────────────
with tab_chat:
    if "messages" not in st.session_state:
        st.session_state["messages"] = []
    if "_mock_turn" not in st.session_state:
        st.session_state["_mock_turn"] = 0

    # Render history
    for msg in st.session_state["messages"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("tool_name"):
                with st.expander(f"Tool call: `{msg['tool_name']}`", expanded=False):
                    st.json({"tool": msg["tool_name"], "status": "success"})

    # Graph visualization
    show_graph = demo_mode or (node_table and edge_table)
    if show_graph and st.session_state["messages"]:
        st.divider()
        st.subheader("Graph Visualization")
        try:
            if demo_mode:
                G, id_to_label, label_to_id = _mock_graph()
            else:
                with st.spinner("Loading graph…"):
                    G, id_to_label, label_to_id = _load_graph_data(
                        node_table, edge_table, node_id_col, node_label_col, node_type_col
                    )
            labels = sorted(id_to_label.values())
            if labels:
                selected = st.selectbox("Center node", labels, key="graph_center_node")
                _render_graph(G, id_to_label, label_to_id, selected, nhops)
        except Exception as exc:
            st.warning(f"Graph unavailable: {exc}")

    # Chat input
    if prompt := st.chat_input("Ask the agent…"):
        st.session_state["messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            if demo_mode:
                idx = st.session_state["_mock_turn"] % len(_MOCK_RESPONSES)
                text, tool_name = _MOCK_RESPONSES[idx]
                st.session_state["_mock_turn"] += 1
                with st.spinner("Thinking…"):
                    import time; time.sleep(0.6)
                st.markdown(text)
                with st.expander(f"Tool call: `{tool_name}`", expanded=False):
                    st.json({"tool": tool_name, "status": "success"})
                st.session_state["messages"].append({
                    "role": "assistant", "content": text, "tool_name": tool_name,
                })
                st.rerun()
            else:
                if not agent_name.strip():
                    st.warning("Enter an **Agent Name** in the sidebar first.")
                else:
                    with st.spinner("Thinking…"):
                        try:
                            chat = _get_chat(agent_name.strip(), agent_db.strip(), agent_schema.strip())
                            if chat:
                                response = chat.send(prompt)
                                text = response.full_text() or "_No text response._"
                                tool_calls = response.tool_calls()
                                st.markdown(text)
                                if tool_calls:
                                    with st.expander(f"Tool calls ({len(tool_calls)})", expanded=False):
                                        for tc in tool_calls:
                                            st.json({"tool": tc.name, "arguments": tc.arguments})
                                st.session_state["messages"].append({
                                    "role": "assistant", "content": text,
                                    "tool_name": tool_calls[0].name if tool_calls else None,
                                })
                                st.cache_data.clear()
                                st.rerun()
                        except Exception as exc:
                            st.error(f"Agent error: {exc}")


# ── Tab 2: Wafer Zone Analysis ────────────────────────────────────────────────
with tab_wafer:
    st.subheader("GPU Wafer Zone Yield Analysis")
    st.caption(
        "Synthetic die-level data for visualization. Adjust parameters to explore zone effects — "
        "connect to real fab data by replacing `_make_wafer_df` with a Snowflake query."
    )

    col_a, col_b, col_c = st.columns([1, 1, 1])
    with col_a:
        st.markdown("**Wafer Geometry**")
        wafer_r   = st.selectbox("Wafer radius", [150, 100], format_func=lambda x: f"{x*2} mm", index=0)
        die_w     = st.number_input("Die width (mm)",   min_value=2.0, max_value=60.0, value=26.0, step=1.0)
        die_h     = st.number_input("Die height (mm)",  min_value=2.0, max_value=60.0, value=33.0, step=1.0)
        edge_excl = st.number_input("Edge exclusion (mm)", min_value=0.5, max_value=15.0, value=3.0, step=0.5)
    with col_b:
        st.markdown("**Zone Defect Rates**")
        fail_center = st.slider("Center fail rate", 0.0, 0.50, 0.03, 0.01, format="%.2f")
        fail_mid    = st.slider("Middle fail rate",  0.0, 0.50, 0.08, 0.01, format="%.2f")
        fail_edge   = st.slider("Edge fail rate",    0.0, 0.50, 0.25, 0.01, format="%.2f")
    with col_c:
        st.markdown("**Simulation**")
        seed = st.number_input("Random seed", min_value=0, max_value=9999, value=42, step=1)

    df_wafer = _make_wafer_df(
        int(seed), float(wafer_r), float(die_w), float(die_h),
        float(edge_excl), fail_center, fail_mid, fail_edge,
    )
    total   = len(df_wafer)
    passing = int((df_wafer["status"] == "PASS").sum())
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Dies",    total)
    m2.metric("Passing Dies",  passing)
    m3.metric("Failing Dies",  total - passing)
    m4.metric("Overall Yield", f"{passing / total * 100:.1f}%" if total else "—")

    fig = _plot_wafer(df_wafer, float(wafer_r), float(die_w), float(die_h))
    st.pyplot(fig)
    plt.close(fig)

    with st.expander("Die-level data table"):
        st.dataframe(df_wafer, use_container_width=True, hide_index=True)
