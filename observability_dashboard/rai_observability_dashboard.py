"""
RelationalAI Observability Dashboard — single-file version
─────────────────────────────────────────────────────────────────────────────
Monitors RelationalAI Native App resource consumption on Snowflake.

Setup:
  1. pip install streamlit snowflake-connector-python pandas plotly
  2. Create .streamlit/secrets.toml with your Snowflake credentials (see bottom
     of this file for the required format).
  3. streamlit run rai_observability_dashboard.py

Deploy to Streamlit in Snowflake (SiS):
  Upload BOTH this file AND environment.yml to the same stage location, then
  create the app via Snowsight → Streamlit → New app (point it at this file).
  The environment.yml pins the streamlit/pandas versions to avoid a conda
  solver conflict (streamlit 1.22 vs pandas 3). The app auto-detects the SiS
  environment and uses the built-in session — no secrets.toml needed.
"""
from __future__ import annotations

import os
import datetime as dt

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# ══════════════════════════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════════════════════════

def _is_snowflake_environment() -> bool:
    return os.environ.get("SNOWFLAKE_HOST") is not None


def _get_snowpark_session():
    from snowflake.snowpark.context import get_active_session  # type: ignore
    return get_active_session()


def _get_connection():
    import snowflake.connector as sf
    s = st.secrets["snowflake"]
    kwargs = dict(
        account=s["account"],
        user=s["user"],
        password=s.get("password"),
        authenticator=s.get("authenticator", "snowflake"),
        warehouse=s.get("warehouse"),
        database=s.get("database"),
        schema=s.get("schema"),
        role=s.get("role"),
    )
    if s.get("passcode"):
        kwargs["passcode"] = s["passcode"]
    if s.get("private_key"):
        kwargs["private_key"] = s["private_key"]
    return sf.connect(**kwargs)


def run_query(sql: str) -> pd.DataFrame:
    if _is_snowflake_environment():
        return _get_snowpark_session().sql(sql).to_pandas()
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# SQL QUERIES
# ══════════════════════════════════════════════════════════════════════════════

_Q_MEMORY_REALTIME = """
SELECT REASONER_NAME, MEMORY_UTILIZATION, TIMESTAMP
FROM {db}.OBSERVABILITY_PREVIEW.LOGIC_REASONER__MEMORY_UTILIZATION
WHERE TIMESTAMP >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC
"""

_Q_MEMORY_HOURLY = """
SELECT REASONER_NAME,
    DATE_TRUNC('hour', TIMESTAMP) AS HOUR,
    AVG(MEMORY_UTILIZATION)       AS AVG_MEMORY,
    MAX(MEMORY_UTILIZATION)       AS PEAK_MEMORY
FROM {db}.OBSERVABILITY_PREVIEW.LOGIC_REASONER__MEMORY_UTILIZATION
WHERE TIMESTAMP >= DATEADD(hour, -{lookback_hours}, CURRENT_TIMESTAMP())
GROUP BY REASONER_NAME, HOUR
ORDER BY HOUR DESC
"""

_Q_MEMORY_DAILY = """
SELECT REASONER_NAME,
    DATE_TRUNC('day', TIMESTAMP) AS DAY,
    AVG(MEMORY_UTILIZATION)      AS AVG_MEMORY,
    MAX(MEMORY_UTILIZATION)      AS PEAK_MEMORY
FROM {db}.OBSERVABILITY_PREVIEW.LOGIC_REASONER__MEMORY_UTILIZATION
WHERE TIMESTAMP >= DATEADD(day, -{lookback_days}, CURRENT_TIMESTAMP())
GROUP BY REASONER_NAME, DAY
ORDER BY DAY DESC
"""

_Q_CPU_REALTIME = """
SELECT REASONER_NAME, CPU_UTILIZATION, TIMESTAMP
FROM {db}.OBSERVABILITY_PREVIEW.LOGIC_REASONER__CPU_UTILIZATION
WHERE TIMESTAMP >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC
"""

_Q_CPU_HOURLY = """
SELECT REASONER_NAME,
    DATE_TRUNC('hour', TIMESTAMP) AS HOUR,
    AVG(CPU_UTILIZATION)          AS AVG_CPU,
    MAX(CPU_UTILIZATION)          AS MAX_CPU
FROM {db}.OBSERVABILITY_PREVIEW.LOGIC_REASONER__CPU_UTILIZATION
WHERE TIMESTAMP >= DATEADD(hour, -{lookback_hours}, CURRENT_TIMESTAMP())
GROUP BY REASONER_NAME, HOUR
ORDER BY HOUR DESC
"""

_Q_DEMAND_REALTIME = """
SELECT REASONER_NAME, DEMAND, REASONER_CAPACITY, TIMESTAMP
FROM {db}.OBSERVABILITY_PREVIEW.LOGIC_REASONER__DEMAND
WHERE TIMESTAMP >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
ORDER BY TIMESTAMP DESC
"""

_Q_DEMAND_HOURLY = """
SELECT REASONER_NAME, REASONER_CAPACITY,
    DATE_TRUNC('hour', TIMESTAMP) AS HOUR,
    AVG(DEMAND)                   AS AVG_DEMAND,
    MAX(DEMAND)                   AS MAX_DEMAND
FROM {db}.OBSERVABILITY_PREVIEW.LOGIC_REASONER__DEMAND
WHERE TIMESTAMP >= DATEADD(hour, -{lookback_hours}, CURRENT_TIMESTAMP())
GROUP BY REASONER_NAME, REASONER_CAPACITY, HOUR
ORDER BY HOUR DESC
"""

_Q_DEMAND_DAILY = """
SELECT REASONER_NAME, REASONER_CAPACITY,
    DATE_TRUNC('day', TIMESTAMP) AS DAY,
    AVG(DEMAND)                  AS AVG_DEMAND,
    MAX(DEMAND)                  AS MAX_DEMAND
FROM {db}.OBSERVABILITY_PREVIEW.LOGIC_REASONER__DEMAND
WHERE TIMESTAMP >= DATEADD(day, -{lookback_days}, CURRENT_TIMESTAMP())
GROUP BY REASONER_NAME, REASONER_CAPACITY, DAY
ORDER BY DAY DESC
"""

_Q_ACTIVE_REASONERS = """
SELECT DATE_TRUNC('hour', TIMESTAMP) AS HOUR,
    COUNT(DISTINCT REASONER_ID)      AS ACTIVE_REASONERS
FROM {db}.OBSERVABILITY_PREVIEW.LOGIC_REASONER__DEMAND
WHERE TIMESTAMP >= DATEADD(hour, -{lookback_hours}, CURRENT_TIMESTAMP())
GROUP BY HOUR
ORDER BY HOUR ASC
"""

_Q_CREDITS_TOTAL = """
SELECT COMPUTE_POOL_NAME, SUM(CREDITS_USED) AS CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY
WHERE APPLICATION_NAME = 'RELATIONALAI'
  AND START_TIME >= '{date_from}'
  AND START_TIME <  DATEADD(day, 1, '{date_to}')
GROUP BY COMPUTE_POOL_NAME
ORDER BY CREDITS_USED DESC
"""

_Q_CREDITS_DAILY = """
SELECT DATE_TRUNC('day', START_TIME) AS DAY,
    COMPUTE_POOL_NAME,
    SUM(CREDITS_USED)                AS CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY
WHERE APPLICATION_NAME = 'RELATIONALAI'
  AND START_TIME >= '{date_from}'
  AND START_TIME <  DATEADD(day, 1, '{date_to}')
GROUP BY DAY, COMPUTE_POOL_NAME
ORDER BY DAY ASC
"""


# ══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG & HELPERS
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="RelationalAI Observability Dashboard",
    page_icon="🔍",
    layout="wide",
)

try:
    RAI_DB = st.secrets.get("snowflake", {}).get("rai_app_db", "RELATIONALAI")
except Exception:
    RAI_DB = "RELATIONALAI"


def _q(template: str, **kwargs) -> str:
    return template.format(db=RAI_DB, **kwargs)


@st.cache_data(ttl=120, show_spinner=False)
def fetch(sql: str) -> pd.DataFrame:
    try:
        return run_query(sql)
    except Exception as exc:
        st.warning(f"Query error: {exc}")
        return pd.DataFrame()


def _pct(val) -> str:
    try:
        return f"{float(val) * 100:.3f}%"
    except Exception:
        return "—"


def _demand_icon(val) -> str:
    try:
        v = float(val)
        return "🔴" if v > 1.0 else ("🟡" if v > 0.8 else "🟢")
    except Exception:
        return "—"


def _filter(df: pd.DataFrame, selected: list[str]) -> pd.DataFrame:
    if not selected or "REASONER_NAME" not in df.columns:
        return df
    return df[df["REASONER_NAME"].isin(selected)]


def _map_pool_name(name: str) -> str:
    """Map raw Snowflake compute pool names to human-readable RAI pool categories."""
    if name in ("RELATIONAL_AI_ERP_COMPUTE_POOL", "RELATIONAL_AI_COMPILE_CACHE_SPCS"):
        return "RELATIONALAI_INTERNAL_POOL"
    if name.endswith("_SOLVER"):
        return "PRESCRIPTIVE_REASONER_POOL"
    if name.endswith("_MODELER"):
        return "RELATIONALAI_UI_POOL"
    if "HIGH_MEM_X64" in name and not name.endswith("_SOLVER"):
        return "RELATIONALAI_LOGIC_GRAPH_REASONER"
    return name


def _apply_pool_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate credits after mapping compute pool names, summing any pools that merge."""
    if df.empty or "COMPUTE_POOL_NAME" not in df.columns:
        return df
    df = df.copy()
    df["COMPUTE_POOL_NAME"] = df["COMPUTE_POOL_NAME"].map(_map_pool_name)
    group_cols = [c for c in df.columns if c != "CREDITS_USED"]
    return df.groupby(group_cols, as_index=False)["CREDITS_USED"].sum()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("RelationalAI Observability")
    st.caption("RelationalAI Native App — resource monitoring")
    st.divider()

    # Time window
    time_window = st.selectbox(
        "Time window",
        ["Last 24 hours", "Last 7 days", "Last 30 days", "Custom dates"],
        index=0,
    )
    _today = dt.date.today()

    if time_window == "Custom dates":
        DATE_FROM: dt.date = st.date_input("From", value=_today - dt.timedelta(days=30), max_value=_today)
        DATE_TO:   dt.date = st.date_input("To",   value=_today, min_value=DATE_FROM, max_value=_today)
        _delta = (DATE_TO - DATE_FROM).days + 1
        LOOKBACK_HOURS = _delta * 24
        LOOKBACK_DAYS  = _delta
    else:
        _lk_map = {
            "Last 24 hours": (24,  1),
            "Last 7 days":   (168, 7),
            "Last 30 days":  (720, 30),
        }
        LOOKBACK_HOURS, LOOKBACK_DAYS = _lk_map[time_window]
        DATE_FROM = _today - dt.timedelta(days=LOOKBACK_DAYS - 1)
        DATE_TO   = _today

    st.divider()

    # Reasoner filter — populated from live demand snapshot
    _all_reasoners: list[str] = []
    _df_filter = fetch(_q(_Q_DEMAND_REALTIME))
    if not _df_filter.empty and "REASONER_NAME" in _df_filter.columns:
        _all_reasoners = sorted(_df_filter["REASONER_NAME"].unique().tolist())

    SELECTED: list[str] = st.multiselect(
        "Filter reasoners",
        options=_all_reasoners,
        default=_all_reasoners,
        help="Leave empty to show all.",
    )
    if not SELECTED:
        SELECTED = _all_reasoners

    st.divider()

    if st.button("Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"RelationalAI app DB: `{RAI_DB}`")


# ══════════════════════════════════════════════════════════════════════════════
# TITLE
# ══════════════════════════════════════════════════════════════════════════════

st.title("RelationalAI Observability Dashboard")
st.caption(
    "Monitor RelationalAI reasoner performance and Snowflake resource consumption. "
    "Data sourced from `RELATIONALAI.OBSERVABILITY_PREVIEW` and Snowflake Account Usage."
)

tab_overview, tab_memory, tab_cpu, tab_demand, tab_credits = st.tabs([
    "Overview", "Memory", "CPU", "Demand", "Credits",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

with tab_overview:
    st.subheader(f"Snapshot — {time_window}")

    col_mem, col_cpu, col_demand, col_cred = st.columns(4)

    df_mem_ov  = _filter(fetch(_q(_Q_MEMORY_HOURLY, lookback_hours=LOOKBACK_HOURS)), SELECTED)
    df_cpu_ov  = _filter(fetch(_q(_Q_CPU_HOURLY,    lookback_hours=LOOKBACK_HOURS)), SELECTED)
    df_dem_ov  = _filter(fetch(_q(_Q_DEMAND_HOURLY, lookback_hours=LOOKBACK_HOURS)), SELECTED)
    df_cred_ov = _apply_pool_mapping(fetch(_q(_Q_CREDITS_TOTAL, date_from=DATE_FROM, date_to=DATE_TO)))

    with col_mem:
        if not df_mem_ov.empty and "AVG_MEMORY" in df_mem_ov.columns:
            st.metric("Avg Memory Util", _pct(df_mem_ov["AVG_MEMORY"].mean()))
        else:
            st.metric("Avg Memory Util", "No data")

    with col_cpu:
        if not df_cpu_ov.empty and "AVG_CPU" in df_cpu_ov.columns:
            st.metric("Avg CPU Util", _pct(df_cpu_ov["AVG_CPU"].mean()))
        else:
            st.metric("Avg CPU Util", "No data")

    with col_demand:
        if not df_dem_ov.empty and "MAX_DEMAND" in df_dem_ov.columns:
            peak = df_dem_ov["MAX_DEMAND"].max()
            st.metric("Peak Demand", f"{_demand_icon(peak)} {peak:.3f}")
        else:
            st.metric("Peak Demand", "No data")

    with col_cred:
        if not df_cred_ov.empty and "CREDITS_USED" in df_cred_ov.columns:
            st.metric("Total Credits Used", f"{df_cred_ov['CREDITS_USED'].sum():,.2f}")
        else:
            st.metric("Total Credits Used", "No data")

    st.divider()
    st.subheader("Active Reasoners Over Time")

    df_active = fetch(_q(_Q_ACTIVE_REASONERS, lookback_hours=LOOKBACK_HOURS))
    if df_active.empty:
        st.info("No data available. Ensure the observability view is registered.")
    else:
        fig = px.bar(
            df_active, x="HOUR", y="ACTIVE_REASONERS",
            labels={"ACTIVE_REASONERS": "Active Reasoners", "HOUR": "Time"},
            title=f"Active Reasoners — {time_window}",
        )
        fig.update_yaxes(dtick=1)
        st.plotly_chart(fig, width="stretch")


# ══════════════════════════════════════════════════════════════════════════════
# TAB: MEMORY
# ══════════════════════════════════════════════════════════════════════════════

with tab_memory:
    st.subheader("Memory Utilization")
    sub_rt, sub_hourly, sub_daily = st.tabs(["Real-Time (5 min)", "Hourly", "Daily Trend"])

    with sub_rt:
        df = _filter(fetch(_q(_Q_MEMORY_REALTIME)), SELECTED)
        if df.empty:
            st.info("No data in the last 5 minutes.")
        else:
            fig = px.line(
                df, x="TIMESTAMP", y="MEMORY_UTILIZATION", color="REASONER_NAME",
                labels={"MEMORY_UTILIZATION": "Memory Utilization", "TIMESTAMP": "Time"},
                title="Memory Utilization — Last 5 Minutes",
                render_mode="svg",
            )
            fig.add_hline(y=0.8, line_dash="dash", line_color="red", annotation_text="80% threshold")
            fig.update_yaxes(tickformat=".0%", range=[0, 1])
            fig.update_xaxes(range=[df["TIMESTAMP"].max() - pd.Timedelta(minutes=5), df["TIMESTAMP"].max()])
            st.plotly_chart(fig, width="stretch")
            st.dataframe(df, width="stretch")

    with sub_hourly:
        df = _filter(fetch(_q(_Q_MEMORY_HOURLY, lookback_hours=LOOKBACK_HOURS)), SELECTED)
        if df.empty:
            st.info("No hourly data available.")
        else:
            df_long = df.melt(id_vars=["REASONER_NAME", "HOUR"], value_vars=["AVG_MEMORY", "PEAK_MEMORY"],
                              var_name="Metric", value_name="Value")
            fig = px.line(df_long, x="HOUR", y="Value", color="REASONER_NAME", line_dash="Metric",
                          labels={"Value": "Memory Utilization", "HOUR": "Hour", "REASONER_NAME": "Reasoner"},
                          title=f"Memory Utilization (Hourly Avg/Peak) — Last {LOOKBACK_HOURS}h")
            fig.update_yaxes(tickformat=".0%")
            st.plotly_chart(fig, width="stretch")
            st.dataframe(df, width="stretch")

    with sub_daily:
        df = _filter(fetch(_q(_Q_MEMORY_DAILY, lookback_days=LOOKBACK_DAYS)), SELECTED)
        if df.empty:
            st.info("No daily data available.")
        else:
            fig = px.bar(df, x="DAY", y="AVG_MEMORY", color="REASONER_NAME", barmode="group",
                         error_y=df["PEAK_MEMORY"] - df["AVG_MEMORY"] if "PEAK_MEMORY" in df.columns else None,
                         labels={"AVG_MEMORY": "Avg Memory", "DAY": "Day"},
                         title=f"Memory Utilization (Daily Avg) — Last {LOOKBACK_DAYS} day(s)")
            fig.update_yaxes(tickformat=".0%")
            st.plotly_chart(fig, width="stretch")
            st.dataframe(df, width="stretch")


# ══════════════════════════════════════════════════════════════════════════════
# TAB: CPU
# ══════════════════════════════════════════════════════════════════════════════

with tab_cpu:
    st.subheader("CPU Utilization")
    sub_rt, sub_hourly = st.tabs(["Real-Time (5 min)", "Hourly"])

    with sub_rt:
        df = _filter(fetch(_q(_Q_CPU_REALTIME)), SELECTED)
        if df.empty:
            st.info("No data in the last 5 minutes.")
        else:
            fig = px.line(df, x="TIMESTAMP", y="CPU_UTILIZATION", color="REASONER_NAME",
                          labels={"CPU_UTILIZATION": "CPU Utilization", "TIMESTAMP": "Time"},
                          title="CPU Utilization — Last 5 Minutes",
                          render_mode="svg")
            fig.add_hline(y=0.95, line_dash="dash", line_color="red", annotation_text="95% critical")
            fig.add_hline(y=0.85, line_dash="dot", line_color="orange", annotation_text="85% warning")
            fig.update_yaxes(tickformat=".0%", range=[0, 1])
            fig.update_xaxes(range=[df["TIMESTAMP"].max() - pd.Timedelta(minutes=5), df["TIMESTAMP"].max()])
            st.plotly_chart(fig, width="stretch")
            st.dataframe(df, width="stretch")

    with sub_hourly:
        df = _filter(fetch(_q(_Q_CPU_HOURLY, lookback_hours=LOOKBACK_HOURS)), SELECTED)
        if df.empty:
            st.info("No hourly data available.")
        else:
            df_long = df.melt(id_vars=["REASONER_NAME", "HOUR"], value_vars=["AVG_CPU", "MAX_CPU"],
                              var_name="Metric", value_name="Value")
            fig = px.line(df_long, x="HOUR", y="Value", color="REASONER_NAME", line_dash="Metric",
                          labels={"Value": "CPU Utilization", "HOUR": "Hour", "REASONER_NAME": "Reasoner"},
                          title=f"CPU Utilization (Hourly Avg/Max) — Last {LOOKBACK_HOURS}h")
            fig.update_yaxes(tickformat=".0%")
            st.plotly_chart(fig, width="stretch")
            st.dataframe(df, width="stretch")


# ══════════════════════════════════════════════════════════════════════════════
# TAB: DEMAND
# ══════════════════════════════════════════════════════════════════════════════

with tab_demand:
    st.subheader("Reasoner Demand")
    st.caption("Demand > 1.0 indicates jobs are queuing beyond available capacity.")
    sub_rt, sub_hourly, sub_daily = st.tabs(["Real-Time (5 min)", "Hourly", "Daily Trend"])

    with sub_rt:
        df = _filter(fetch(_q(_Q_DEMAND_REALTIME)), SELECTED)
        if df.empty:
            st.info("No demand data in the last 5 minutes.")
        else:
            fig = px.line(df, x="TIMESTAMP", y="DEMAND", color="REASONER_NAME",
                          labels={"DEMAND": "Demand", "TIMESTAMP": "Time"},
                          title="Reasoner Demand — Last 5 Minutes",
                          render_mode="svg")
            fig.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="Queuing threshold")
            fig.add_hline(y=0.8, line_dash="dot", line_color="orange", annotation_text="80% warning")
            fig.update_xaxes(range=[df["TIMESTAMP"].max() - pd.Timedelta(minutes=5), df["TIMESTAMP"].max()])
            st.plotly_chart(fig, width="stretch")
            latest = df.sort_values("TIMESTAMP", ascending=False).drop_duplicates("REASONER_NAME").copy()
            latest["Status"] = latest["DEMAND"].apply(
                lambda v: "Queuing" if v > 1.0 else ("High" if v > 0.8 else "OK")
            )
            st.dataframe(latest[["REASONER_NAME", "REASONER_CAPACITY", "DEMAND", "Status", "TIMESTAMP"]], width="stretch")

    with sub_hourly:
        df = _filter(fetch(_q(_Q_DEMAND_HOURLY, lookback_hours=LOOKBACK_HOURS)), SELECTED)
        if df.empty:
            st.info("No hourly demand data available.")
        else:
            df_long = df.melt(id_vars=["REASONER_NAME", "HOUR"], value_vars=["AVG_DEMAND", "MAX_DEMAND"],
                              var_name="Metric", value_name="Value")
            fig = px.line(df_long, x="HOUR", y="Value", color="REASONER_NAME", line_dash="Metric",
                          labels={"Value": "Demand", "HOUR": "Hour", "REASONER_NAME": "Reasoner"},
                          title=f"Demand (Hourly Avg/Max) — Last {LOOKBACK_HOURS}h")
            fig.add_hline(y=1.0, line_dash="dash", line_color="red", opacity=0.6)
            st.plotly_chart(fig, width="stretch")
            st.dataframe(df, width="stretch")

    with sub_daily:
        df = _filter(fetch(_q(_Q_DEMAND_DAILY, lookback_days=LOOKBACK_DAYS)), SELECTED)
        if df.empty:
            st.info("No daily demand data available.")
        else:
            fig = px.bar(df, x="DAY", y="AVG_DEMAND", color="REASONER_NAME", barmode="group",
                         error_y=df["MAX_DEMAND"] - df["AVG_DEMAND"] if "MAX_DEMAND" in df.columns else None,
                         labels={"AVG_DEMAND": "Avg Demand", "DAY": "Day"},
                         title=f"Demand (Daily Avg) — Last {LOOKBACK_DAYS} day(s)")
            fig.add_hline(y=1.0, line_dash="dash", line_color="red", opacity=0.6, annotation_text="Queuing threshold")
            st.plotly_chart(fig, width="stretch")
            st.dataframe(df, width="stretch")


# ══════════════════════════════════════════════════════════════════════════════
# TAB: CREDITS
# ══════════════════════════════════════════════════════════════════════════════

with tab_credits:
    st.subheader("Snowflake Credits — Compute Pools (RelationalAI)")
    st.caption(
        f"Source: `SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY` | "
        f"Date range: **{DATE_FROM}** → **{DATE_TO}**"
    )

    _df = {"date_from": str(DATE_FROM), "date_to": str(DATE_TO)}
    df_credits_total = _apply_pool_mapping(fetch(_q(_Q_CREDITS_TOTAL, **_df)))
    df_credits_daily = _apply_pool_mapping(fetch(_q(_Q_CREDITS_DAILY, **_df)))

    if df_credits_total.empty:
        st.info("No compute pool credit data found. Ensure your role has access to `SNOWFLAKE.ACCOUNT_USAGE`.")
    else:
        k1, k2, k3 = st.columns(3)
        k1.metric("Total Credits", f"{df_credits_total['CREDITS_USED'].sum():,.2f}")
        k2.metric("Compute Pools", df_credits_total["COMPUTE_POOL_NAME"].nunique())
        k3.metric("Highest Consumer",
                  df_credits_total.loc[df_credits_total["CREDITS_USED"].idxmax(), "COMPUTE_POOL_NAME"])

        st.divider()

        if not df_credits_daily.empty and "DAY" in df_credits_daily.columns:
            fig_trend = px.bar(df_credits_daily, x="DAY", y="CREDITS_USED", color="COMPUTE_POOL_NAME",
                               barmode="stack",
                               labels={"CREDITS_USED": "Credits Used", "DAY": "Date", "COMPUTE_POOL_NAME": "Compute Pool"},
                               title="Daily Credit Consumption by Compute Pool")
            daily_total = df_credits_daily.groupby("DAY")["CREDITS_USED"].sum().reset_index()
            fig_trend.add_trace(go.Scatter(
                x=daily_total["DAY"], y=daily_total["CREDITS_USED"].cumsum(),
                name="Cumulative Total", mode="lines+markers",
                line=dict(color="black", width=2, dash="dot"), yaxis="y2",
            ))
            fig_trend.update_layout(
                yaxis2=dict(overlaying="y", side="right", title="Cumulative Credits", showgrid=False),
                legend_title="Compute Pool",
            )
            st.plotly_chart(fig_trend, width="stretch")

        fig_bar = px.bar(df_credits_total, x="COMPUTE_POOL_NAME", y="CREDITS_USED",
                         labels={"CREDITS_USED": "Credits Used", "COMPUTE_POOL_NAME": "Compute Pool"},
                         title="Total Credits by Compute Pool",
                         color="CREDITS_USED", color_continuous_scale="Blues")
        st.plotly_chart(fig_bar, width="stretch")
        st.dataframe(df_credits_total, width="stretch")


# ══════════════════════════════════════════════════════════════════════════════
# SECRETS TEMPLATE
# ══════════════════════════════════════════════════════════════════════════════
# Create .streamlit/secrets.toml with the following content:
#
# [snowflake]
# account       = "your_org-your_account"
# user          = "your_username"
# password      = "your_password"
# authenticator = "snowflake"          # or "username_password_mfa" / "externalbrowser"
# passcode      = ""                   # required for username_password_mfa only
# warehouse     = "YOUR_WAREHOUSE"
# role          = "YOUR_ROLE"
# rai_app_db    = "RELATIONALAI"       # name of your RelationalAI Native App database
