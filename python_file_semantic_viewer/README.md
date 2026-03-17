# Semantic Graph Explorer

A Streamlit app to visualize a RelationalAI-style semantic layer, validate Snowflake tables, and explore sample data with interactive filtering.

## Features
- Upload or use a local semantic layer Python file.
- Interactive concept graph with zoom and click-to-inspect.
- Snowflake connectivity for table validation and sample data.
- Filters that grey out concepts/relationships with no data.
- Reset filters in one click.

## Local Run
1. Create a virtualenv and install requirements:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the app:
   ```bash
   streamlit run app.py
   ```

## Snowflake Connection
The app expects a `raiconfig.toml` path in the sidebar. It uses the active profile from that file.

Important:
- Do **not** commit credentials.
- For Snowflake-native Streamlit, you can wire credentials via `st.secrets["snowflake"]` and swap the connection logic in `snowflake_client.py` to use those configs.

## Notes
- The semantic file is parsed as text (not executed). This allows support for generated PyRel files that contain non-Python identifiers in `filter_by` clauses.
- Filtering is best-effort based on join keys inferred from the semantic file.

# streamlit_apps
