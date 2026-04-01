from __future__ import annotations

import pandas as pd
import streamlit as st

st.set_page_config(page_title="RAI Observability", layout="wide")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;600&display=swap');
    :root { --accent: #0e5a6f; --accent-soft: #e0f1f4; --ink: #1e1e1e; }
    html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; color: var(--ink); }
    .stApp { background: radial-gradient(1200px 600px at 10% 0%, #e7f0f2, #f4f1ec); }
    .block-container { padding-top: 1.2rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("RAI Observability")

sf_client = st.session_state.get("sf_client")
if not sf_client:
    st.warning("Connect to Snowflake on the main page first.")
    st.stop()


# ── RAI surcharge rates keyed on SPCS instance_family ────────────────────────
# credits_per_hour: Snowflake SPCS list price (Service Consumption Table, Apr 2026)
# surcharge_per_hour: RAI software surcharge in USD/node-hour (NULL = not applicable)
_SURCHARGE_RATES: dict[str, tuple[float, float | None]] = {
    "HIGHMEM_X64_L":  (4.44, 124),
    "HIGHMEM_X64_SL": (2.93,  92),
    "HIGHMEM_X64_M":  (1.11,  28),
    "HIGHMEM_X64_S":  (0.28,   6),
    "GPU_NV_S":       (0.57,  18),
    # CPU pools: Snowflake credits known; RAI surcharge not applicable
    "CPU_X64_M":      (0.22, None),
    "CPU_X64_S":      (0.11, None),
    "CPU_X64_XS":     (0.06, None),
}


def _build_cost_query(pool_families: pd.DataFrame, days: int) -> str:
    """
    Build the cost-estimate query dynamically using live pool→instance_family
    data from SHOW COMPUTE POOLS, keyed on instance_family rates.
    Only includes pools owned by the RELATIONALAI application.
    """
    rai_pools = pool_families[pool_families["application"] == "RELATIONALAI"]

    # Build VALUES rows: (pool_name, credits_per_hour, surcharge_per_hour)
    values_rows = []
    for _, row in rai_pools.iterrows():
        pool = row["name"]
        family = row["instance_family"]
        if family not in _SURCHARGE_RATES:
            continue
        credits, surcharge = _SURCHARGE_RATES[family]
        surcharge_sql = str(surcharge) if surcharge is not None else "NULL"
        values_rows.append(f"        ('{pool}', {credits}, {surcharge_sql})")

    if not values_rows:
        return ""

    values_clause = ",\n".join(values_rows)

    return f"""
WITH pool_rates AS (
    SELECT * FROM (VALUES
{values_clause}
    ) AS t(pool_name, credits_per_hour, surcharge_per_hour)
),
consumption AS (
    SELECT
        h.START_TIME,
        h.COMPUTE_POOL_NAME,
        h.APPLICATION_NAME,
        p.instance_family,
        h.CREDITS_USED,
        r.credits_per_hour,
        r.surcharge_per_hour,
        h.CREDITS_USED / NULLIF(r.credits_per_hour, 0)             AS node_hours,
        (h.CREDITS_USED / NULLIF(r.credits_per_hour, 0))
            * r.surcharge_per_hour                                  AS projected_surcharge_usd
    FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY h
    JOIN pool_rates r ON h.COMPUTE_POOL_NAME = r.pool_name
    LEFT JOIN (VALUES
{chr(10).join(f"        ('{row['name']}', '{row['instance_family']}')" for _, row in rai_pools.iterrows())}
    ) AS p(pool_name, instance_family) ON h.COMPUTE_POOL_NAME = p.pool_name
    WHERE h.START_TIME >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
      AND h.APPLICATION_NAME = 'RELATIONALAI'
)
SELECT
    DATE_TRUNC('day', START_TIME)       AS usage_date,
    COMPUTE_POOL_NAME,
    instance_family,
    SUM(CREDITS_USED)                   AS total_credits,
    SUM(node_hours)                     AS total_node_hours,
    SUM(projected_surcharge_usd)        AS total_projected_surcharge_usd
FROM consumption
GROUP BY 1, 2, 3
ORDER BY usage_date DESC, total_projected_surcharge_usd DESC NULLS LAST
"""


_Q_MARKETPLACE_PAID = """
SELECT TOP 100 * FROM snowflake.data_sharing_usage.marketplace_paid_usage_daily
"""

_Q_CREDITS_PER_POOL = """
SELECT COMPUTE_POOL_NAME, SUM(CREDITS_USED) AS TOTAL_CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY
WHERE APPLICATION_NAME = 'RELATIONALAI'
GROUP BY COMPUTE_POOL_NAME
ORDER BY TOTAL_CREDITS_USED DESC
"""

_Q_CREDITS_BREAKDOWN = """
SELECT
    APPLICATION_NAME,
    USAGE_DATE,
    CREDITS_USED AS TOTAL_CREDITS,
    SUM(CASE WHEN b.value:"serviceType"::STRING = 'WAREHOUSE_METERING'
             THEN b.value:"credits"::NUMBER(38,9) ELSE 0 END) AS WAREHOUSE_METERING_CREDITS,
    SUM(CASE WHEN b.value:"serviceType"::STRING = 'SERVERLESS_TASK'
             THEN b.value:"credits"::NUMBER(38,9) ELSE 0 END) AS SERVERLESS_TASK_CREDITS,
    SUM(CASE WHEN b.value:"serviceType"::STRING = 'SNOWPARK_CONTAINER_SERVICES'
             THEN b.value:"credits"::NUMBER(38,9) ELSE 0 END) AS SNOWPARK_CONTAINER_SERVICES_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.APPLICATION_DAILY_USAGE_HISTORY,
    LATERAL FLATTEN(INPUT => CREDITS_USED_BREAKDOWN, OUTER => TRUE) b
WHERE APPLICATION_NAME = 'RELATIONALAI'
GROUP BY APPLICATION_NAME, USAGE_DATE, CREDITS_USED
ORDER BY USAGE_DATE DESC, APPLICATION_NAME
"""


def _run(query: str) -> None:
    try:
        df = sf_client.query_df(query)
        if df.empty:
            st.info("No rows returned.")
        else:
            st.dataframe(df, width="stretch")
    except Exception as exc:
        st.error(str(exc))


# ── Section 1: Cost estimate ──────────────────────────────────────────────────
st.header("Estimated SPCS Cost (last N days)")
st.caption(
    "Only includes pools owned by the RELATIONALAI application. "
    "Uses standard list-price surcharge rates — projected USD will differ from "
    "invoiced amounts if your account has negotiated pricing. "
    "RAI surcharge is not applicable to CPU pools (compile cache, modeler, service)."
)

days = st.slider("Look-back window (days)", 7, 90, 30, step=7)

if st.button("Run cost estimate", key="btn_cost"):
    with st.spinner("Fetching compute pool info…"):
        try:
            pool_families = sf_client.show_compute_pools()
        except Exception as exc:
            st.error(f"SHOW COMPUTE POOLS failed: {exc}")
            st.stop()

    rai_pool_count = (pool_families["application"] == "RELATIONALAI").sum()
    st.caption(f"Found {rai_pool_count} RAI-owned compute pools.")

    query = _build_cost_query(pool_families, days)
    if not query:
        st.warning("No RAI pools with known rates found.")
    else:
        with st.spinner("Querying usage history…"):
            _run(query)

st.divider()

# ── Section 2: Marketplace paid usage ─────────────────────────────────────────
st.header("Marketplace Paid Usage")
st.caption(
    "Total RAI usage billed through the Snowflake Marketplace. "
    "Only populated on paid accounts."
)

if st.button("Run marketplace usage", key="btn_marketplace"):
    with st.spinner("Querying…"):
        _run(_Q_MARKETPLACE_PAID)

st.divider()

# ── Section 3: Credits per compute pool ───────────────────────────────────────
st.header("Credits per Compute Pool")
st.caption("Snowflake credits consumed by each RAI compute pool.")

if st.button("Run credits per pool", key="btn_pool"):
    with st.spinner("Querying…"):
        _run(_Q_CREDITS_PER_POOL)

st.divider()

# ── Section 4: Credits breakdown by service type ──────────────────────────────
st.header("Credits Breakdown by Service Type")
st.caption(
    "Daily credit usage split across Warehouse Metering, Serverless Tasks, "
    "and Snowpark Container Services for the RELATIONALAI application."
)

if st.button("Run credits breakdown", key="btn_breakdown"):
    with st.spinner("Querying…"):
        _run(_Q_CREDITS_BREAKDOWN)
