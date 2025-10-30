# agents/profiling/base_node.py
from typing import Any, Dict
from utils.helpers import extract_table_from_text, strip_sql_blocks
from utils.sql_generator import generate_sql_for_intent
from utils.sql_executor import execute_sql
import time
import uuid
from datetime import datetime

class BaseProfilingNode:
    """
    Base class providing common utilities for profiling nodes.
    Subclasses implement `run_profiling(self, state)` and return updated state dict.
    """

    def get_table(self, state: Dict[str, Any]) -> str:
        ctx = state.get("context") or {}
        table = None
        if isinstance(ctx, dict):
            table = ctx.get("table")
        if not table:
            table = extract_table_from_text(state.get("userText", "") or "") or "employees"
        return table

    def generate_sql(self, intent: str, table: str, user_text: str, system_prompt: str) -> tuple[str | None, dict]:
        return generate_sql_for_intent(intent, table, user_text, system_prompt)

    def execute_sql(self, sql: str):
        return execute_sql(sql)

    def build_core_payload(self,
                           summary: str,
                           table: str,
                           sql: str,
                           chart_type: str,
                           chart_data: list,
                           insights: list,
                           diagnostics: dict,
                           provenance: dict,
                           sample_rows: list | None = None,
                           rows_scanned: int | None = None) -> dict:
        return {
            "summary": summary,
            "metadata": {
                "table": table,
                "rows_scanned": rows_scanned,
                "columns_profiled": len(chart_data) if chart_data is not None else None,
                "execution_time_ms": round((time.time() - provenance.get("_start_ts", time.time())) * 1000, 2) if provenance.get("_start_ts") else None,
                "sql": sql,
                "data_source": provenance.get("engine") or provenance.get("source")
            },
            "sample": {
                "sample_type": "first_n",
                "sample_size": len(sample_rows) if sample_rows is not None else 0,
                "total_available": rows_scanned,
                "rows": sample_rows or []
            },
            "visualization": {"chart_type": chart_type, "chart_data": chart_data or []},
            "insights": insights or [],
            "llm_commentary": None,
            "provenance": provenance,
            "diagnostics": diagnostics or {}
        }
