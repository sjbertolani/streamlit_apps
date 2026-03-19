# Semantic Graph Explorer

A Streamlit app to visualize a RelationalAI PyRel semantic layer, validate Snowflake tables, and explore sample data.

## Features
- Upload a PyRel semantic layer Python file — parsed via the metamodel IR when `relationalai` is available, with a regex fallback for generated files.
- Interactive concept graph with zoom and click-to-inspect.
- Snowflake connectivity for table validation, row counts, and sample data.
- Expand any concept node to browse individual data instances.

## Local Setup (uv — recommended)

Requires [uv](https://github.com/astral-sh/uv) and a Python interpreter that has `relationalai` installed (e.g. the RAI conda env or anaconda3 base).

```bash
cd python_file_semantic_viewer

# Create a venv using the Python that has relationalai
uv venv --python /Users/stevebertolani/anaconda3/bin/python

# Install all dependencies
uv pip install -r requirements.txt

# Run the app (use python -m to ensure the venv's streamlit is used, not any
# globally installed one)
uv run python -m streamlit run app.py
```

The app opens at http://localhost:8501.

## Local Setup (pip fallback)

```bash
cd python_file_semantic_viewer
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Testing without Snowflake

Upload `example_semantic_layer.py` from the repo root. The graph renders immediately — no Snowflake connection is needed to see concepts and relationships. Counts and sample data activate only after connecting.

## Snowflake Connection

The app expects a `raiconfig.toml` path (sidebar input). It reads the `active_profile` from that file for credentials.

- Do **not** commit credentials or `raiconfig.toml`.
- For Snowflake-native Streamlit deployment, swap `SnowflakeClient.from_raiconfig` for `st.secrets["snowflake"]`-based connection logic in `snowflake_client.py`.

## Notes

- The semantic parser tries to `exec` the uploaded file to get a live `Model` object and introspect it via `model.to_metamodel()`. If the file uses non-standard syntax (e.g. space-containing keyword names in `filter_by` calls) or `relationalai` is not installed, it automatically falls back to regex text parsing.
- Schema and edge counts are fetched in a single `UNION ALL` query per button click, not one query per table.
- Table validation runs in parallel (up to 8 threads).
