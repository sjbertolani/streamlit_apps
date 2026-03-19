from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
import os
import tomllib
import yaml

import pandas as pd
import snowflake.connector


@dataclass
class TableStatus:
    table: str
    exists: bool
    has_data: bool
    error: Optional[str] = None


class SnowflakeClient:
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self._conn = connection

    @classmethod
    def from_raiconfig(cls, path: str) -> "SnowflakeClient":
        if path.endswith((".yaml", ".yml")):
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        else:
            with open(path, "rb") as f:
                data = tomllib.load(f)
        # Support two YAML/TOML layouts:
        #   RAI layout:      active_profile / profile.<name>
        #   Snowflake CLI:   default_connection / connections.<name>
        profile_name = data.get("active_profile") or data.get("default_connection", "default")
        profile = (
            data.get("profile", {}).get(profile_name)
            or data.get("connections", {}).get(profile_name)
        )
        if not profile:
            raise ValueError(f"Profile '{profile_name}' not found in raiconfig")

        conn = snowflake.connector.connect(
            user=profile.get("user"),
            password=profile.get("password"),
            account=profile.get("account"),
            role=profile.get("role"),
            warehouse=profile.get("warehouse"),
            database=profile.get("database"),
            schema=profile.get("schema"),
            authenticator=profile.get("authenticator"),
            passcode=profile.get("passcode"),
        )
        return cls(conn)

    @classmethod
    def from_env(cls) -> "SnowflakeClient":
        conn = snowflake.connector.connect(
            user=os.environ.get("SNOWFLAKE_USER"),
            password=os.environ.get("SNOWFLAKE_PASSWORD"),
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            role=os.environ.get("SNOWFLAKE_ROLE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA"),
            authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR"),
        )
        return cls(conn)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def query_df(self, sql: str) -> pd.DataFrame:
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetch_pandas_all()

    def exists(self, sql: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return row is not None

    def get_columns(self, table: str) -> List[str]:
        with self._conn.cursor() as cur:
            cur.execute(f"DESCRIBE TABLE {table}")
            rows = cur.fetchall()
        cols = [row[0] for row in rows if row and isinstance(row[0], str)]
        return cols

    def validate_tables(self, tables: Iterable[str]) -> List[TableStatus]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        table_list = list(tables)

        def _check(table: str) -> TableStatus:
            # Each thread creates its own cursor from the shared connection.
            # Read-only concurrent cursor usage is safe in practice.
            try:
                has_data = self.exists(f"SELECT 1 FROM {table} LIMIT 1")
                return TableStatus(table=table, exists=True, has_data=has_data)
            except Exception as exc:
                return TableStatus(table=table, exists=False, has_data=False, error=str(exc))

        if not table_list:
            return []

        results_map: Dict[str, TableStatus] = {}
        with ThreadPoolExecutor(max_workers=min(len(table_list), 8)) as pool:
            futures = {pool.submit(_check, t): t for t in table_list}
            for fut in as_completed(futures):
                s = fut.result()
                results_map[s.table] = s

        return [results_map[t] for t in table_list]

    def test_connection(self) -> Dict[str, str]:
        sql = "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        return {
            "user": row[0] if row else "",
            "role": row[1] if row else "",
            "database": row[2] if row else "",
            "schema": row[3] if row else "",
        }

