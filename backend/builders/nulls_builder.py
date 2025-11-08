# backend/builders/nulls_builder.py
import re
from typing import Any, Dict, Optional, List

from utils.schema_utils import get_table_schemas, _extract_sql, _try_parse_json
from sqlalchemy import inspect

from config import SYSTEM_PROMPT, NULLS_PROMPT
import sqlparse


class NullsQueryBuilder:
    """
    Build null-count SQL using schema + user_text + optional LLM assistance.
    """

    def __init__(self, llm_client, engine):
        """
        :param llm_client: wrapper with .invoke(prompt) or .predict(...) returning str
        :param engine: SQLAlchemy engine
        """
        self.llm = llm_client
        self.engine = engine
        self.inspector = inspect(engine)

    # -----------------------
    # Public
    # -----------------------
    def generate(self, user_text: str, dialect: str = "sqlite", table_hint: Optional[str] = None) -> Dict[str, Any]:
        """
        Main entry:
        - attempt to extract table from user_text (LLM or simple heuristics)
        - fetch schema
        - build prompt from config.NULLS_PROMPT
        - call LLM to produce SQL
        - validate SQL
        returns {"sql": str, "table": str, "schema": str, "raw_llm": str, "errors": []}
        """
        errors: List[str] = []
        params = self._extract_params(user_text)
        table = table_hint or params.get("table")
        if not table:
            errors.append("No table found in user_text and no table_hint provided.")
            return {"sql": None, "table": None, "schema": None, "raw_llm": None, "errors": errors, "params": params}

        schema_map = get_table_schemas([table], self.engine)
        schema_text = schema_map.get(table, "(unknown schema)")
        prompt = SYSTEM_PROMPT + "\n" + NULLS_PROMPT.format(
            dialect=dialect,
            table=table,
            schema=schema_text,
            user_text=user_text
        )

        raw = ""
        try:
            raw = self.llm.invoke(prompt)
        except Exception as e:
            errors.append(f"LLM invoke failed: {e}")
            raw = ""

        sql = _extract_sql(raw.content)
        if not sql:
            errors.append("LLM did not produce valid SQL. Falling back to deterministic generator.")
            # fallback deterministic query
            try:
                sql = self._fallback_sql(table)
            except Exception as e:
                errors.append(f"Fallback generation failed: {e}")
                sql = None

        # Validate SQL
        if sql:
            try:
                parsed = sqlparse.parse(sql)
                if not parsed:
                    errors.append("sqlparse could not parse SQL.")
            except Exception as e:
                errors.append(f"sqlparse error: {e}")

        return {"sql": sql, "table": table, "schema": schema_text, "raw_llm": raw, "errors": errors, "params": params}

    # -----------------------
    # Helpers
    # -----------------------
    def _extract_params(self, user_text: str) -> Dict[str, Any]:
        """
        Quick heuristic to find a table name; fall back to LLM extraction if helpful.
        1) simple regex 'from <tablename>' or 'for <tablename>'
        2) else call LLM to extract JSON {table: ..., filters: ...}
        """
        ut = (user_text or "").strip()
        # regex heuristics
        m = re.search(r"from\s+([A-Za-z0-9_]+)", ut, re.IGNORECASE)
        if not m:
            m = re.search(r"for\s+([A-Za-z0-9_]+)", ut, re.IGNORECASE)
        if m:
            return {"table": m.group(1), "filters": None}

        # LLM extraction fallback
        try:
            prompt = (
                "Extract table and filters from the user text. Return JSON like "
                '{"table":"<name>","filters":"<where clause or empty>"}\n'
                f'User text: """{ut}"""'
            )
            raw = self.llm.invoke(prompt)
            parsed = _try_parse_json(raw)
            return {"table": parsed.get("table"), "filters": parsed.get("filters")}
        except Exception:
            return {"table": None, "filters": None}


    def _fallback_sql(self, table: str) -> str:
        # deterministic: inspect columns and build SUM(CASE WHEN ... IS NULL THEN 1 ELSE 0 END)
        cols = [c["name"] for c in self.inspector.get_columns(table)]
        if not cols:
            raise RuntimeError("No columns found for fallback generation.")
        parts = [f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls" for col in cols]
        return f"SELECT {', '.join(parts)} FROM {table};"
