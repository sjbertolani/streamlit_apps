# RAI Observability Dashboard

A Streamlit dashboard for monitoring [RelationalAI](https://relational.ai) Native App resource consumption on Snowflake.

## Features

- **Memory utilization** — real-time, hourly, and daily views per reasoner
- **CPU utilization** — real-time and hourly trends
- **Demand** — real-time, hourly, and daily demand per reasoner with capacity context
- **Compute credits** — credit consumption by compute pool over a configurable date range
- **Global filters** — filter by reasoner and time window (last 24 h / 7 d / 30 d / custom dates)
- **Auto-refresh** — sidebar refresh button clears cached queries

## Setup

### Local

```bash
pip install -r requirements.txt
```

Create `.streamlit/secrets.toml` (see `.streamlit/secrets.toml.example` for the required format), then run:

```bash
streamlit run rai_observability_dashboard.py
```

### Snowflake (Streamlit in Snowflake)

Upload `rai_observability_dashboard.py` and `environment.yml` to the same stage, then create the app via Snowsight → Streamlit → New app. No `secrets.toml` needed — the app auto-detects the Snowflake environment and uses the active session.

## Tests

```bash
python -m pytest tests/ -v
```
