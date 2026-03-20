# confluent-sql Examples

Examples demonstrating how to use `confluent-sql` with popular Python data tools.

## Prerequisites

All examples require Confluent Cloud credentials set as environment variables:

```bash
export CONFLUENT_FLINK_API_KEY="your-key"
export CONFLUENT_FLINK_API_SECRET="your-secret"
export CONFLUENT_ENV_ID="env-123456"
export CONFLUENT_ORG_ID="org-123456"
export CONFLUENT_COMPUTE_POOL_ID="lfcp-789012"
export CONFLUENT_CLOUD_PROVIDER="aws"
export CONFLUENT_CLOUD_REGION="us-east-2"
export CONFLUENT_TEST_DBNAME="your-database"    # optional
```

## Examples

### Basic Usage

| Example | Description |
|---------|-------------|
| [snapshot_mode_tuple_cursor_simple_example.py](snapshot_mode_tuple_cursor_simple_example.py) | Snapshot queries with tuple cursors |
| [simple_append_only_streaming_query_example.py](simple_append_only_streaming_query_example.py) | Streaming queries with dict cursors |
| [errors.py](errors.py) | Error handling patterns |

### pandas

| Example | Description |
|---------|-------------|
| [pandas/read_flink_table.py](pandas/read_flink_table.py) | Read Flink tables into DataFrames with `pd.read_sql()` |
| [pandas/streaming_to_dataframe.py](pandas/streaming_to_dataframe.py) | Batch streaming results into periodic DataFrames |

```bash
uv add pandas confluent-sql
python pandas/read_flink_table.py
```

### Polars

| Example | Description |
|---------|-------------|
| [polars/read_flink_table.py](polars/read_flink_table.py) | Read Flink tables into Polars DataFrames |

```bash
uv add polars confluent-sql
python polars/read_flink_table.py
```

### Jupyter

| Example | Description |
|---------|-------------|
| [jupyter/flink_exploration.ipynb](jupyter/flink_exploration.ipynb) | Interactive notebook for exploring Flink tables |

```bash
uv add jupyterlab pandas confluent-sql
jupyter lab jupyter/flink_exploration.ipynb
```

### Streamlit

| Example | Description |
|---------|-------------|
| [streamlit/flink_dashboard.py](streamlit/flink_dashboard.py) | Live dashboard with query editor and charts |

```bash
uv add streamlit pandas confluent-sql
streamlit run streamlit/flink_dashboard.py
```

### LangChain

| Example | Description |
|---------|-------------|
| [langchain/flink_sql_agent.py](langchain/flink_sql_agent.py) | Natural language SQL agent over Flink tables |

```bash
uv add langchain langchain-community langchain-anthropic langgraph confluent-sql
export ANTHROPIC_API_KEY="your-key"
python langchain/flink_sql_agent.py
```
