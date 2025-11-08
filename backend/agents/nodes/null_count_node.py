# backend/agents/null_count_node.py

import os
import re
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, inspect
from models import AgentState
from services.sql_executor import execute_sql
from agents.nodes.clarification_node import ClarificationNode
from services.langchain_sql_agent import create_langchain_sql_query
from services.llm_client import get_openai_llm
from builders.nulls_builder import NullsQueryBuilder


class NullCountNode:
    """
    Computes null counts per column for a given table.
    Conversationally requests missing table name if not provided.
    """

    def __init__(self):
        self.clarifier = ClarificationNode()

    # ----------------------------
    # Main run method
    # ----------------------------
    def run(self, state: AgentState) -> AgentState:
        start = time.time()
        llm = get_openai_llm()  # returns an object with .invoke(prompt) -> str
        db_url = os.getenv("DATABASE_URL", "sqlite:///./demo.db")
        engine = create_engine(db_url)

        # For nulls
        nb = NullsQueryBuilder(llm, engine)
        ctx = state.context or {}

        # STEP 1 — Ensure table name is available
        table = ctx.get("table")
        user_text = state.combined_user_text()
        if not table:
            # Attempt to extract from user text if possible
            params = nb._extract_params(user_text=user_text)
            maybe_table = params["table"]
            filters = params["filters"]
            if maybe_table:
                ctx["table"] = maybe_table
                ctx["filters"] = filters
                table = params
            else:
                # If still missing, delegate to clarification node
                state.intent = "nulls"  # ensure intent persists for routing
                clarified = self.clarifier.run(state)
                clarified.awaiting_input = True
                return clarified

        sql_used = None
        diagnostics = {"warnings": [], "errors": []}
        provenance = {"engine": db_url.split("://")[0], "llm_used_for": []}


        res = nb.generate(user_text, dialect="sqlite")
        sql_used = res["sql"]        

        # STEP 2 — Generate SQL query using LLM
        # if create_langchain_sql_query:
        #     try:
        #         prompt = f"Generate a single SQL query that counts NULL values per column in table `{table}`. Return only SQL."
        #         agent = create_langchain_sql_query(prompt=prompt)
        #         output = agent.invoke({"question": prompt})
        #         sql_used = self._extract_sql_from_text(output)
        #         provenance["llm_used_for"].append("sql_generation")
        #     except Exception as e:
        #         diagnostics["warnings"].append(f"LLM SQL generation failed: {e}")

        # STEP 3 — Fallback SQL if LLM fails
        if not sql_used:
            try:
                inspector = inspect(engine)
                cols = [c["name"] for c in inspector.get_columns(table)]
                if not cols:
                    raise Exception("No columns found in table.")
                sql_parts = [
                    f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls"
                    for col in cols
                ]
                sql_used = f"SELECT {', '.join(sql_parts)} FROM {table};"
                provenance["llm_used_for"].append("fallback_sql_generation")
            except Exception as e:
                diagnostics["errors"].append(str(e))
                state.message = f"Could not generate SQL for {table}: {e}"
                return state

        # STEP 4 — Execute query
        try:
            df = execute_sql(sql_used)
            if df.empty:
                raise Exception("Query returned no rows.")
        except Exception as e:
            diagnostics["errors"].append(f"Execution failed: {e}")
            state.message = f"Error executing SQL: {e}"
            return state

        # STEP 5 — Prepare chart + insights
        chart_data = []
        insights = []
        sample_rows = []
        try:
            first_row = df.iloc[0]
            for col, val in first_row.items():
                clean_col = re.sub(r"_nulls$", "", col)
                null_count = int(val or 0)
                chart_data.append({"column": clean_col, "null_count": null_count})
                if null_count > 0:
                    insights.append({
                        "id": str(uuid.uuid4()),
                        "type": "missing_values",
                        "severity": "warning",
                        "metric": "null_count",
                        "description": f"Column '{clean_col}' has {null_count} NULL values.",
                        "columns": [clean_col],
                        "value": {"null_count": null_count},
                        "evidence": {"sql": sql_used},
                        "actionable": True,
                        "suggested_actions": [f"Filter rows WHERE {clean_col} IS NULL"]
                    })

            if not insights:
                insights.append({
                    "id": str(uuid.uuid4()),
                    "type": "missing_values",
                    "severity": "info",
                    "metric": "null_count",
                    "description": "No missing values detected in this table.",
                    "columns": [],
                    "value": {"null_count": 0},
                    "evidence": {"sql": sql_used},
                    "actionable": False,
                })
        except Exception as e:
            diagnostics["warnings"].append(f"Insight generation failed: {e}")

        # STEP 6 — Build response payload
        payload = {
            "summary": f"Computed null counts for {len(chart_data)} columns in table {table}.",
            "metadata": {
                "table": table,
                "execution_time_ms": round((time.time() - start) * 1000, 2),
                "sql": sql_used
            },
            "visualization": {
                "chart_type": "bar",
                "chart_data": chart_data
            },
            "insights": insights,
            "provenance": provenance,
            "diagnostics": diagnostics
        }

        # STEP 7 — Update state
        state.context = ctx
        state.payload = payload
        state.message = payload["summary"]
        state.awaiting_input = False
        state.missing_params = []
        state.next_prompt = "Would you like to see distinct counts next?"
        return state

    # ----------------------------
    # Helpers
    # ----------------------------
    def _extract_sql_from_text(self, text: str) -> Optional[str]:
        """Extract SQL between triple backticks or detect SELECT."""
        if not text:
            return None
        m = re.search(r"```(?:sql)?\n([\s\S]*?)```", text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        if text.strip().lower().startswith("select"):
            return text.strip()
        return None

    def _extract_table_from_text(self, text: Optional[str]) -> Optional[str]:
        """Heuristic: extract table name from plain text."""
        if not text:
            return None
        text = text.lower()
        match = re.search(r"from\s+(\w+)", text)
        if match:
            return match.group(1)
        match2 = re.search(r"for\s+(\w+)", text)
        if match2:
            return match2.group(1)
        return None
