# backend/builders/distincts_builder.py
import json
import re
from typing import Any, Dict, Optional, List

import sqlparse
from sqlalchemy import inspect

from config import SYSTEM_PROMPT, DISTINCTS_PROMPT
from utils.schema_utils import get_table_schemas, _extract_sql, _try_parse_json


class DistinctsQueryBuilder:
    """
    Build distinct-count SQL using schema + user_text + optional LLM assistance.
    """

    def __init__(self, llm_client, engine):
        self.llm = llm_client
        self.engine = engine
        self.inspector = inspect(engine)

    def generate(self, user_text: str, dialect: str = "sqlite", table_hint: Optional[str] = None) -> Dict[str, Any]:
        errors: List[str] = []
        params = self._extract_params(user_text)
        table = table_hint or params.get("table")
        if not table:
            errors.append("No table found in user_text and no table_hint provided.")
            return {"sql": None, "table": None, "schema": None, "raw_llm": None, "errors": errors, "params": params}

        schema_map = get_table_schemas([table], self.engine)
        schema_text = schema_map.get(table, "(unknown schema)")
        prompt = SYSTEM_PROMPT + "\n" + DISTINCTS_PROMPT.format(
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
            errors.append("LLM did not produce valid SQL. Using fallback.")
            try:
                sql = self._fallback_sql(table)
            except Exception as e:
                errors.append(f"Fallback failed: {e}")
                sql = None

        if sql:
            try:
                parsed = sqlparse.parse(sql)
                if not parsed:
                    errors.append("sqlparse could not parse SQL.")
            except Exception as e:
                errors.append(f"sqlparse error: {e}")

        return {"sql": sql, "table": table, "schema": schema_text, "raw_llm": raw, "errors": errors, "params": params}

    def _extract_params(self, user_text: str) -> Dict[str, Any]:
        ut = (user_text or "").strip()
        m = re.search(r"from\s+([A-Za-z0-9_]+)", ut, re.IGNORECASE)
        if not m:
            m = re.search(r"for\s+([A-Za-z0-9_]+)", ut, re.IGNORECASE)
        if m:
            return {"table": m.group(1), "filters": None}
        # LLM fallback
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
        cols = [c["name"] for c in self.inspector.get_columns(table)]
        if not cols:
            raise RuntimeError("No columns found for fallback generation.")
        parts = [f"COUNT(DISTINCT {col}) AS {col}_distinct" for col in cols]
        return f"SELECT {', '.join(parts)} FROM {table};"
