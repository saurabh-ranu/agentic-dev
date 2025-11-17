# backend/agents/distinct_count_node.py

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
from builders.distinct_builders import DistinctsQueryBuilder
from services.llm_client import get_openai_llm


class DistinctCountNode:
    """
    Computes distinct value counts per column for a given table.
    Conversationally requests missing table name if not provided.
    """

    def __init__(self):
        self.clarifier = ClarificationNode()

    def run(self, state: AgentState) -> AgentState:
        start = time.time()
        ctx = state.context or {}

        llm = get_openai_llm()  # returns an object with .invoke(prompt) -> str
        db_url = os.getenv("DATABASE_URL", "sqlite:///./demo.db")
        engine = create_engine(db_url)
        # For nulls
        db = DistinctsQueryBuilder(llm, engine)
        ctx = state.context or {}        

        # STEP 1 — Ensure table name is available
        table = ctx.get("table")
        user_text = state.combined_user_text()
        if not table:
            params = db._extract_params(user_text=user_text)
            maybe_table = params["table"]
            inspector = inspect(engine)
            if not inspector.has_table(maybe_table):
                maybe_table = ""
                ctx["table"] = ""
            filters = params["filters"]
            if maybe_table:
                ctx["table"] = maybe_table
                ctx["filters"] = filters
                table = maybe_table
            else:
                clarified = self.clarifier.run(state)
                return clarified

        sql_used = None
        diagnostics = {"warnings": [], "errors": []}
        provenance = {"engine": db_url.split("://")[0], "llm_used_for": []}

        res = db.generate(user_text, dialect="sqlite")
        sql_used = res["sql"]  

        # STEP 2 — Generate SQL query using LLM
        # if create_langchain_sql_query:
        #     try:
        #         agent = create_langchain_sql_query()
        #         prompt = (
        #             f"Generate a SQL query that returns the distinct count for each column "
        #             f"in the table `{table}`. Return only SQL."
        #         )
        #         output = agent.invoke({"question": prompt})
        #         sql_used = self._extract_sql_from_text(output)
        #         provenance["llm_used_for"].append("sql_generation")
        #     except Exception as e:
        #         diagnostics["warnings"].append(f"LLM SQL generation failed: {e}")

        # STEP 3 — Fallback SQL generation
        if not sql_used:
            try:
                inspector = inspect(engine)
                cols = [c["name"] for c in inspector.get_columns(table)]
                if not cols:
                    raise Exception("No columns found in table.")
                sql_parts = [f"COUNT(DISTINCT {col}) AS {col}_distincts" for col in cols]
                sql_used = f"SELECT {', '.join(sql_parts)} FROM {table};"
                provenance["llm_used_for"].append("fallback_sql_generation")
            except Exception as e:
                diagnostics["errors"].append(str(e))
                state.message = f"Could not generate SQL for {table}: {e}"
                ctx["table"] = ""
                return state

        # STEP 4 — Execute SQL
        try:
            df = execute_sql(sql_used)
            if df.empty:
                raise Exception("Query returned no rows.")
        except Exception as e:
            diagnostics["errors"].append(f"Execution failed: {e}")
            state.message = f"Error executing SQL: {e}"
            return state

        # STEP 5 — Build visualization and insights
        chart_data = []
        insights = []
        try:
            first_row = df.iloc[0]
            for col, val in first_row.items():
                clean_col = re.sub(r"_distincts$", "", col)
                distinct_count = int(val or 0)
                chart_data.append({"column": clean_col, "distinct_count": distinct_count})
                insights.append({
                    "id": str(uuid.uuid4()),
                    "type": "distinct_values",
                    "severity": "info",
                    "metric": "distinct_count",
                    "description": f"Column '{clean_col}' has {distinct_count} distinct values.",
                    "columns": [clean_col],
                    "value": {"distinct_count": distinct_count},
                    "evidence": {"sql": sql_used},
                    "actionable": False,
                })
        except Exception as e:
            diagnostics["warnings"].append(f"Insight generation failed: {e}")

        # STEP 6 — Build payload
        payload = {
            "summary": f"Computed distinct counts for {len(chart_data)} columns in table {table}.",
            "metadata": {
                "table": table,
                "execution_time_ms": round((time.time() - start) * 1000, 2),
                "sql": sql_used,
            },
            "visualization": {
                "chart_type": "bar",
                "chart_data": chart_data,
            },
            "insights": insights,
            "provenance": provenance,
            "diagnostics": diagnostics,
        }

        # STEP 7 — Update state
        state.context = ctx
        state.payload = payload
        state.message = payload["summary"]
        state.awaiting_input = False
        state.missing_params = []
        state.next_prompt = "Would you like to see null counts or full profile next?"
        return state

    # ----------------------------
    # Helpers
    # ----------------------------
    def _extract_sql_from_text(self, text: str) -> Optional[str]:
        """Extract SQL from markdown or direct SELECT statement."""
        if not text:
            return None
        m = re.search(r"```(?:sql)?\n([\s\S]*?)```", text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        if text.strip().lower().startswith("select"):
            return text.strip()
        return None

    def _extract_table_from_text(self, text: Optional[str]) -> Optional[str]:
        """Simple heuristic to find table name."""
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
