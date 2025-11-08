# backend/utils/schema_utils.py
"""
Schema and SQL utility functions shared by profiler builders.
"""

import re
import json
from typing import Any, Dict, List, Optional
from sqlalchemy import inspect


def get_table_schemas(tables: List[str], engine) -> Dict[str, str]:
    """
    Fetch schema text for one or multiple tables using SQLAlchemy inspector.
    Returns dict {table_name: formatted_schema_text}
    Example:
        {
          "employees": "- id (INTEGER)\n- name (TEXT)\n- department (TEXT)",
          "departments": "- id (INTEGER)\n- dept_name (TEXT)"
        }
    """
    inspector = inspect(engine)
    result: Dict[str, str] = {}

    for table in tables or []:
        if not table:
            continue
        try:
            cols = inspector.get_columns(table)
            if not cols:
                result[table] = "(no columns found)"
                continue
            formatted = "\n".join([f"- {c['name']} ({c.get('type')})" for c in cols])
            result[table] = formatted
        except Exception as e:
            result[table] = f"(schema fetch failed: {e})"

    return result


def _extract_sql(text: str) -> str:
    """
    Extract SQL code from outputs that may include markdown fences or commentary.
    Returns empty string if nothing SQL-like found.
    """
    if not text:
        return ""
    # prefer fenced code-block
    m = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # find first SELECT ... ; block
    m2 = re.search(r"(select[\s\S]*?;)", text, re.IGNORECASE)
    if m2:
        return m2.group(1).strip()
    # if starts with select, return whole
    if text.strip().lower().startswith("select"):
        return text.strip()
    return ""

# def _extract_sql(text: Optional[str]) -> Optional[str]:
#     """
#     Extract SQL query from LLM output text.
#     Supports triple backticks, or direct SELECT statements.
#     Returns only the SQL string or None.
#     """
#     if not text:
#         return None

#     # Case 1: code block fenced with triple backticks
#     match = re.search(r"```(?:sql)?\n([\s\S]*?)```", text, re.IGNORECASE)
#     if match:
#         return match.group(1).strip()

#     # Case 2: first SELECT statement
#     match2 = re.search(r"(select[\s\S]*?;)", text, re.IGNORECASE)
#     if match2:
#         return match2.group(1).strip()

#     # Case 3: entire text starts with SELECT
#     if text.strip().lower().startswith("select"):
#         return text.strip()

#     # Case 4: fallback - nothing found
#     return None


def _try_parse_json(text: str) -> Dict[str, Any]:
    """
    Attempt to extract JSON from LLM text.
    Finds the first {...} block and parses it.
    Returns {} if parsing fails.
    """
    if not text:
        return {}
    try:
        match = re.search(r"\{[\s\S]*\}", text)
        if not match:
            return {}
        return json.loads(match.group(0))
    except Exception:
        return {}
