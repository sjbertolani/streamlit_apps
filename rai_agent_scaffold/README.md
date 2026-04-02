# Snowflake Intelligence Agent — RelationalAI Knowledge Graph

A template for packaging a [RelationalAI](https://relational.ai) knowledge graph as a Snowflake Cortex agent and exposing it through Snowflake Intelligence.

## What it does

RelationalAI lets you map your Snowflake data into a semantic layer — concepts, properties, and relationships — that is executable directly inside Snowflake. That model can be queried programmatically, verbalized for an LLM, or exposed as a set of curated analytical queries.

This template wires that semantic layer into a Snowflake Intelligence agent. Once deployed, users can ask natural-language questions about your data from anywhere in Snowflake Intelligence, and the agent translates those questions into executed knowledge graph queries.

---

## File layout

```
agent/
├── rai-agent-config.example.yaml  # Template — copy to rai-agent-config.yaml and fill in
├── ontology.example.py            # Template — copy to ontology.py and customize
├── queries.example.py             # Template — copy to queries.py and customize
├── test_queries.example.py        # Template — copy to test_queries.py and customize
├── si_agent.py                    # Deployment CLI: deploy / update / status / chat / teardown
└── raiconfig.yaml                 # (gitignored) Snowflake credentials
```

The `*.example.*` files are public templates. Your actual implementations (`ontology.py`, `queries.py`, `test_queries.py`, `rai-agent-config.yaml`, `raiconfig.yaml`) are gitignored.

**When adding or changing something, the file to edit is:**

- **Retarget to a different database, schema, warehouse, or source table** → `rai-agent-config.yaml`
- **Model concepts, properties, or data loading** → `ontology.py`
- **Questions the agent can answer** → `queries.py`
- **Snowflake credentials or account** → `raiconfig.yaml`

---

## Getting started

```bash
# 1. Copy the templates
cp rai-agent-config.example.yaml rai-agent-config.yaml
cp ontology.example.py ontology.py
cp queries.example.py queries.py
cp test_queries.example.py test_queries.py

# 2. Fill in rai-agent-config.yaml with your values
# 3. Define your model in ontology.py
# 4. Define your queries in queries.py
# 5. Test locally before deploying
RAI_CONFIG_FILE_PATH=raiconfig.yaml python test_queries.py
```

---

## Prerequisites

- Python venv with `relationalai>=1.0.12` and `httpx` installed
- `ACCOUNTADMIN` role (or equivalent) on the target Snowflake account
- The target schema must exist before deploying (see Deployment below)
- The source table must have change tracking enabled:
  ```sql
  ALTER TABLE <YOUR_DB>.<YOUR_SCHEMA>.<YOUR_TABLE> SET CHANGE_TRACKING = TRUE;
  ```

---

## Configuration

### rai-agent-config.yaml

Single source of truth for all instance-specific values. Copy from the example and fill in:

```yaml
agent:
  name: MY_AGENT_NAME               # Name shown in Snowflake Cortex Agents UI
  database: MY_DATABASE             # Snowflake database to deploy into
  schema: MY_SCHEMA                 # Schema to deploy into (must already exist)
  warehouse: MY_WAREHOUSE           # Warehouse for sproc execution
  model_name: MY_MODEL_NAME         # RAI Model name used internally in each sproc

model:
  source_table: MY_DB.MY_SCHEMA.MY_TABLE  # Fully-qualified source table
```

### raiconfig.yaml

Snowflake connection credentials used locally and during deployment:

```yaml
connections:
  sf:
    account:       # Snowflake account identifier
    user:          # Your Snowflake username
    password:      # Your password
    warehouse:     # Warehouse for query execution
    role:          # Must have CREATE PROCEDURE, CREATE STAGE, CREATE AGENT on the schema
    rai_app_name:  # Name of the RelationalAI native app (usually RELATIONALAI)
```

---

## Deployment

Run all commands from the `agent/` directory with `raiconfig.yaml` present.

```bash
# First-time setup: create the target schema if it doesn't exist
python -c "
from relationalai.config import create_config, SnowflakeConnection
s = create_config().get_session(SnowflakeConnection)
s.sql('CREATE SCHEMA IF NOT EXISTS <YOUR_DB>.<YOUR_SCHEMA>').collect()
print('done')
"

# Deploy the agent (creates schema objects, stage, sprocs, and registers the agent)
python si_agent.py deploy

# Check everything is registered
python si_agent.py status

# Test a question
python si_agent.py chat "what can I ask about?"
python si_agent.py chat "tell me about the ontology"

# After editing ontology.py or queries.py, push changes without re-registering the agent
python si_agent.py update

# Remove all agent resources (permanent — deletes SI conversation history)
python si_agent.py teardown
```

---

## Adding a new query

1. Write the query function in `queries.py`. It must return a `rai.Fragment` and have a clear docstring — the docstring is what the LLM sees to decide when to call the query.

   ```python
   def my_new_query(model: Model, Node, Edge) -> rai.Fragment:
       """
       Describe what question this answers and when the agent should use it.
       """
       n = Node.ref()
       return model.select(n.id.alias("id"), n.category.alias("category"))
   ```

2. Bind and register it in `build_tool_registry` inside `queries.py`:

   ```python
   queries=QueryCatalog(
       _bind(count_by_category, model, Node, Edge),
       _bind(label_trace,       model, Node, Edge),
       _bind(my_new_query,      model, Node, Edge),   # add here
   ),
   ```

3. Test locally before deploying:

   ```bash
   python test_queries.py
   ```

4. Push to Snowflake:

   ```bash
   python si_agent.py update
   ```

---

## Seeding labels

Labels are defined in `ontology.py` inside `initialize()`. To tag a specific record:

```python
node = Node.ref()
model.define(node.label("MY_LABEL")).where(node.id == "some-identifier")
```

After editing, run `python si_agent.py update` to push the change.

---

## Promoting to Snowflake Intelligence

After a successful deploy:

1. In the Snowflake UI, go to **AI & ML → Cortex Agents**
2. Find your agent under the configured database and schema
3. Click **Add to Snowflake Intelligence** on the agent detail page

Once promoted, the agent is discoverable by all Snowflake Intelligence users in the account.

---

## Known workarounds

| Issue | Workaround applied |
|-------|--------------------|
| `snowflake-telemetry-python` conflicts with `relationalai>=1.0.x` opentelemetry dependency | Removed `snowflake-telemetry-python` from the sproc package list in the installed library (`relationalai/agent/cortex/cortex_tool_resources.py` line ~278). Re-apply after upgrading relationalai. |
| `httpx` not auto-installed as a transitive dependency in Snowflake sprocs | Added `"httpx"` to `_EXTRA_PACKAGES` in `si_agent.py`. |
| Local `.to_df()` returns one row per node (not per category) for count queries | Added `.drop_duplicates()` in `test_queries.py`. The deployed sproc in Snowflake returns correctly aggregated results. |
