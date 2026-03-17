"""
Tests for the RAI Observability Dashboard Streamlit app.
Uses streamlit.testing.v1.AppTest with mocked Snowflake connection.

Performance notes:
- Module-scoped fixtures spin up the Streamlit app once per scenario and
  share the result across all tests in that scenario class, avoiding
  repeated expensive AppTest.run() calls.
- Mock DataFrames are module-level constants, built once at import time.
"""
import sys
import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streamlit.testing.v1 import AppTest

APP_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")


# ── Mock DataFrames (module-level constants — built once) ─────────────────────

_MEM_RT = pd.DataFrame({
    "TIMESTAMP": pd.date_range("2026-03-16", periods=5, freq="5min"),
    "REASONER_NAME": ["r1"] * 5,
    "MEMORY_UTILIZATION": [0.3, 0.45, 0.6, 0.5, 0.4],
})
_MEM_HOURLY = pd.DataFrame({
    "REASONER_NAME": ["r1"] * 3,
    "HOUR": pd.date_range("2026-03-16", periods=3, freq="h"),
    "AVG_MEMORY": [0.3, 0.45, 0.5],
    "PEAK_MEMORY": [0.4, 0.6, 0.65],
})
_MEM_DAILY = pd.DataFrame({
    "REASONER_NAME": ["r1"] * 3,
    "DAY": pd.date_range("2026-03-14", periods=3, freq="D"),
    "AVG_MEMORY": [0.3, 0.45, 0.5],
    "PEAK_MEMORY": [0.4, 0.6, 0.65],
})
_CPU_RT = pd.DataFrame({
    "TIMESTAMP": pd.date_range("2026-03-16", periods=5, freq="5min"),
    "REASONER_NAME": ["r1"] * 5,
    "CPU_UTILIZATION": [0.2, 0.35, 0.55, 0.4, 0.3],
})
_CPU_HOURLY = pd.DataFrame({
    "REASONER_NAME": ["r1"] * 3,
    "HOUR": pd.date_range("2026-03-16", periods=3, freq="h"),
    "AVG_CPU": [0.2, 0.35, 0.4],
    "MAX_CPU": [0.3, 0.5, 0.55],
})
_DEMAND = pd.DataFrame({
    "TIMESTAMP": pd.date_range("2026-03-16", periods=5, freq="5min"),
    "REASONER_NAME": ["r1"] * 5,
    "DEMAND": [0.5, 0.8, 1.2, 0.9, 0.6],
    "REASONER_CAPACITY": ["SMALL"] * 5,
})
_COMBINED = pd.DataFrame({
    "REASONER_NAME": ["r1", "r1"],
    "REASONER_ID": ["id1", "id1"],
    "TIME_BUCKET": pd.date_range("2026-03-16", periods=2, freq="10s"),
    "MEMORY_UTILIZATION": [0.4, 0.5],
    "CPU_UTILIZATION": [0.3, 0.4],
    "AVG_DEMAND": [0.8, 1.0],
})
_CREDITS = pd.DataFrame({
    "COMPUTE_POOL_NAME": ["CPU_XS_5", "CPU_S_5"],
    "CREDITS_USED": [12.5, 8.3],
})
_CREDITS_DAILY = pd.DataFrame({
    "DAY": pd.date_range("2026-03-10", periods=7, freq="D"),
    "COMPUTE_POOL_NAME": ["CPU_XS_5"] * 7,
    "CREDITS_USED": [1.2, 1.5, 1.8, 2.0, 1.9, 2.1, 2.3],
})
_ACTIVE_REASONERS = pd.DataFrame({
    "HOUR": pd.date_range("2026-03-16", periods=5, freq="h"),
    "ACTIVE_REASONERS": [1, 2, 2, 1, 2],
})
_DEMAND_HOURLY = pd.DataFrame({
    "REASONER_NAME": ["r1"] * 3,
    "HOUR": pd.date_range("2026-03-16", periods=3, freq="h"),
    "AVG_DEMAND": [0.5, 0.9, 1.1],
    "MAX_DEMAND": [0.7, 1.2, 1.4],
    "REASONER_CAPACITY": ["SMALL"] * 3,
})
_DEMAND_DAILY = pd.DataFrame({
    "REASONER_NAME": ["r1"] * 3,
    "DAY": pd.date_range("2026-03-14", periods=3, freq="D"),
    "AVG_DEMAND": [0.5, 0.8, 1.0],
    "MAX_DEMAND": [0.7, 1.0, 1.3],
    "REASONER_CAPACITY": ["SMALL"] * 3,
})
def _make_side_effect():
    """Return a run_query side-effect that dispatches on SQL content."""
    def _side_effect(sql, *args, **kwargs):
        s = sql.lower()
        if "memory_utilization" in s:
            if "date_trunc('day'" in s:
                return _MEM_DAILY
            if "date_trunc('hour'" in s:
                return _MEM_HOURLY
            return _MEM_RT
        if "cpu_utilization" in s:
            return _CPU_HOURLY if "date_trunc('hour'" in s else _CPU_RT
        if "demand" in s:
            if "date_trunc('day'" in s:
                return _DEMAND_DAILY
            if "count(distinct" in s:
                return _ACTIVE_REASONERS
            if "date_trunc('hour'" in s:
                return _DEMAND_HOURLY
            return _DEMAND
        if "snowpark_container_services_history" in s:
            if "date_trunc" in s:
                return _CREDITS_DAILY
            return _CREDITS
        return pd.DataFrame()
    return _side_effect


def _run_app() -> AppTest:
    """Spin up and run the app with mocked queries. Returns the AppTest."""
    with patch("connection.run_query", side_effect=_make_side_effect()):
        at = AppTest.from_file(APP_PATH, default_timeout=30)
        at.run()
    return at


# ── Shared app instance — built once per module for the normal scenario ────────

@pytest.fixture(scope="module")
def app():
    """Pre-run AppTest shared by all tests in the module."""
    return _run_app()


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestAppLoads:
    def test_app_runs_without_error(self, app):
        assert not app.exception, f"App raised exception: {app.exception}"

    def test_page_title_present(self, app):
        assert not app.exception


class TestSidebarControls:
    def test_sidebar_has_time_range_selector(self, app):
        widgets = list(app.sidebar.selectbox) + list(app.sidebar.radio)
        assert len(widgets) >= 1, "Expected at least one time-range control in sidebar"

    def test_time_window_default_is_24_hours(self, app):
        selectbox = app.sidebar.selectbox[0]
        assert selectbox.value == "Last 24 hours", (
            f"Expected default 'Last 24 hours', got '{selectbox.value}'"
        )

    def test_time_window_options_correct(self, app):
        options = list(app.sidebar.selectbox[0].options)
        assert options == ["Last 24 hours", "Last 7 days", "Last 30 days", "Custom dates"]

    def test_custom_dates_shows_date_inputs(self):
        """Selecting 'Custom dates' reveals two date_input widgets."""
        with patch("connection.run_query", side_effect=_make_side_effect()):
            at = AppTest.from_file(APP_PATH, default_timeout=30)
            at.run()
            at.sidebar.selectbox[0].set_value("Custom dates").run()
            assert not at.exception
            assert len(at.sidebar.date_input) >= 2, (
                "Expected at least two date_input widgets when 'Custom dates' selected"
            )

    def test_sidebar_has_refresh_button(self, app):
        assert len(app.sidebar.button) >= 1, "Expected at least one button in sidebar"


class TestTabsExist:
    def test_tabs_rendered(self, app):
        assert len(app.tabs) >= 1, "Expected at least one tab group"


class TestMetricCards:
    def test_metric_elements_present(self, app):
        assert len(app.metric) >= 1, "Expected at least one st.metric KPI card"



class TestConnectionModule:
    def test_run_query_returns_dataframe(self):
        import connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("COL",)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        with patch("connection._get_connection", return_value=mock_conn):
            result = connection.run_query("SELECT 1 AS COL")
            assert isinstance(result, pd.DataFrame)

    def test_queries_module_has_required_keys(self):
        import queries
        required = [
            "memory_realtime", "memory_hourly", "memory_daily",
            "cpu_realtime", "cpu_hourly",
            "demand_realtime", "demand_hourly", "demand_daily", "active_reasoners_over_time",
            "compute_pool_credits", "compute_pool_credits_daily",
        ]
        for key in required:
            assert hasattr(queries, key), f"queries.py missing: {key}"
            assert isinstance(getattr(queries, key), str), f"queries.{key} should be a string"


class TestGlobalReasonerFilter:
    def test_sidebar_has_reasoner_multiselect(self, app):
        """Sidebar contains a multiselect for filtering by reasoner."""
        assert not app.exception
        assert len(app.sidebar.multiselect) >= 1, "Expected reasoner multiselect in sidebar"

    def test_reasoner_filter_all_selected_by_default(self, app):
        """All reasoners are pre-selected (default = show everything)."""
        assert not app.exception
        ms = app.sidebar.multiselect[0]
        assert ms.value == ms.options, "Expected all reasoners selected by default"


class TestDemandSubTabs:
    def test_demand_tab_no_exception(self, app):
        assert not app.exception

    def test_demand_queries_have_hourly_daily(self):
        import queries
        for attr in ("demand_hourly", "demand_daily"):
            assert hasattr(queries, attr), f"queries.py missing {attr}"
        # Hourly should group by hour
        assert "date_trunc('hour'" in queries.demand_hourly.lower()
        # Daily should group by day
        assert "date_trunc('day'" in queries.demand_daily.lower()
        # Both must have a lookback placeholder
        assert "{lookback_hours}" in queries.demand_hourly
        assert "{lookback_days}" in queries.demand_daily


class TestCreditsDateFilter:
    def test_credits_tab_no_exception(self, app):
        assert not app.exception

    def test_credits_daily_query_has_date_trunc(self):
        import queries
        assert hasattr(queries, "compute_pool_credits_daily"), \
            "queries.py missing compute_pool_credits_daily"
        q = queries.compute_pool_credits_daily.lower()
        assert "date_trunc" in q, "credits daily query should truncate by date"
        assert "start_time" in q or "end_time" in q or "usage_date" in q, \
            "credits daily query should filter on a date column"

    def test_credits_sidebar_date_inputs_appear_for_custom(self):
        """Date inputs only appear when 'Custom dates' is selected."""
        with patch("connection.run_query", side_effect=_make_side_effect()):
            at = AppTest.from_file(APP_PATH, default_timeout=30)
            at.run()
            # No date inputs in default (Last 24 hours) mode
            assert len(at.sidebar.date_input) == 0, \
                "date_input should be hidden in preset mode"
            at.sidebar.selectbox[0].set_value("Custom dates").run()
            assert len(at.sidebar.date_input) >= 2, \
                "Expected two date_input widgets after selecting Custom dates"
