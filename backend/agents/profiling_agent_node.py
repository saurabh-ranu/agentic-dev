# agents/profiling_agent_node.py
import os
import re
import uuid
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain
from services.langchain_sql_agent import create_langchain_sql_query

# Import helpers from your services module(s).
# - execute_sql(query) -> pd.DataFrame
# - create_langchain_sql_agent() -> LangChain SQL agent (optional)
from services.sql_executor import execute_sql
try:
    from services.langchain_sql_agent import create_langchain_sql_agent
except Exception:
    create_langchain_sql_agent = None  # optional; fallback used if None

# Utility: sanitize identifiers for SQL (basic)
def quote_ident(identifier: str) -> str:
    # For SQLite / most engines, safe quoting using double quotes.
    # In production, prefer SQLAlchemy text() with bind params or engine-specific quoting.
    if '"' in identifier or "'" in identifier:
        return identifier  # leave as-is (best effort)
    return f'"{identifier}"'


class ProfilingAgentNode:
    def __init__(self):
        # Map intents to handlers; we'll implement nulls here and keep placeholders for others.
        self.ACTIONS = {
            "nulls": self.handle_null_counts,
            # other actions can map here: "distinct": self.handle_distinct_counts, ...
        }

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Entrypoint called by LangGraph. Expects 'intent' to be present in state (e.g., 'nulls').
        This method will call the appropriate handler and return the updated state.
        """
        intent = state.get("intent", "profile")
        handler = self.ACTIONS.get(intent)
        if not handler:
            state["message"] = f"Intent '{intent}' not implemented in profiling agent."
            state["payload"] = {}
            return state

        try:
            return handler(state)
        except Exception as e:
            # Defensive: return diagnostics in payload if something went wrong
            state["message"] = f"Error during profiling: {str(e)}"
            state["payload"] = {
                "summary": "Agent failed to run profiling.",
                "metadata": {"table": state.get("context", {}).get("table") or "unknown"},
                "diagnostics": {"errors": [str(e)]}
            }
            return state

    # ----------------------------
    # Null count handler (main implementation)
    # ----------------------------
    def handle_null_counts(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compute null counts for each column in the requested table.
        Steps:
          1. Determine table name (from state.context.table or parse userText)
          2. Try to request SQL from LangChain SQL Agent (schema-aware)
          3. Fallback: build deterministic SQL using SQLAlchemy inspector
          4. Execute SQL -> DataFrame
          5. Build sample, visualization, insights, metadata, provenance
        """
        start_ts = time.time()
        ctx = state.get("context") or {}
        table = (ctx.get("table") if isinstance(ctx, dict) else None) or self._extract_table_from_text(state.get("userText", "")) or ""
        db_url = os.getenv("DATABASE_URL", "sqlite:///./demo.db")
        engine = create_engine(db_url)

        sql_used = None
        diagnostics = {"warnings": [], "errors": []}
        #provenance = {"engine": db_url.split("://")[0] if db_url else "unknown", "executor": "sql_agent" if create_langchain_sql_agent else "sql_executor", "llm_used_for": []}

        provenance = {
        "engine": db_url.split("://")[0] if db_url else "unknown",
        "executor": "sql_query_chain" if create_langchain_sql_query() else "sql_executor",
        "llm_used_for": []
        }

        # Step A: Attempt to ask LangChain SQL Agent / Query Chain to generate SQL
        agent_sql = None
        if create_langchain_sql_query:
            try:
                agent = create_langchain_sql_query()

                user_text = state.get("userText", "").strip()
                # Improved prompt leveraging userText + intent + schema context
                prompt = f"""
                You are a SQL generation assistant.

                The user said:
                \"\"\"{user_text}\"\"\"
                Intent: "nulls"
                Table: `{table}`

                Goal:
                Generate a single SQL query that counts NULL values for each relevant column in `{table}`.
                If the user provided filters (e.g., WHERE conditions), apply them.
                If specific columns are mentioned, restrict to those.
                If unclear, include all columns.

                Return only SQL — no commentary, no markdown.
                """

                agent_output = agent.invoke({"question": prompt})

                # Extract SQL string from agent output
                agent_sql = self._strip_sql_blocks(agent_output)
                if agent_sql:
                    sql_used = agent_sql
                    provenance["llm_used_for"].append("sql_generation")
                else:
                    diagnostics["warnings"].append("SQL agent returned no parsable SQL; using fallback.")
            except Exception as e:
                diagnostics["warnings"].append(f"SQL agent generation failed: {str(e)}")
                agent_sql = None
        
        
        # Step A: Attempt to ask langchain sql agent to generate SQL
        #agent_sql = None
        #if create_langchain_sql_query:
        #    try:
        #        agent = create_langchain_sql_query()
                # Prompt: request SQL only (no explanation)
        #        prompt = f"Generate a single SQL query that returns the null count for each column in the table `{table}`. " \
        #                 f"Return only SQL - no surrounding text or explanation."
                # The agent.run may return text containing SQL. We attempt to extract SQL.
        #        agent_output = agent.invoke({"question": prompt} )
                # Attempt to extract SQL block from agent_output
                #agent_sql = self._extract_sql_from_text(agent_output)
        #        agent_sql = self._strip_sql_blocks(agent_output)
        #        if agent_sql:
        #            sql_used = agent_sql
        #            provenance["llm_used_for"].append("sql_generation")
        #        else:
        #            diagnostics["warnings"].append("SQL agent returned no parsable SQL; using fallback.")
        #    except Exception as e:
        #        diagnostics["warnings"].append(f"SQL agent generation failed: {str(e)}")
        #        agent_sql = None

        # Step B: Fallback - deterministic SQL built from reflected columns
        if not sql_used:
            try:
                inspector = inspect(engine)
                # handle schemas: detect if table is in a specific schema (not implemented deeply here)
                if table not in inspector.get_table_names():
                    # attempt lower/upper or warn
                    table_candidates = inspector.get_table_names()
                    if table.lower() in [t.lower() for t in table_candidates]:
                        # find exact matching name
                        exact = next(t for t in table_candidates if t.lower() == table.lower())
                        table = exact
                    else:
                        diagnostics["warnings"].append(f"Table '{table}' not found via inspector; attempting PRAGMA introspect (SQLite fallback).")
                # get columns using inspector
                try:
                    columns_info = inspector.get_columns(table)
                    columns = [c["name"] for c in columns_info]
                except Exception:
                    # SQLite fallback: PRAGMA table_info
                    try:
                        cols_df = execute_sql(f"PRAGMA table_info({table})")
                        columns = [r.get("name") for r in cols_df.to_dict(orient="records")]
                    except Exception:
                        # Give up gracefully
                        raise RuntimeError(f"Unable to introspect table '{table}' columns.")
                # Build SQL aggregating nulls
                if not columns:
                    raise RuntimeError(f"No columns found for table '{table}'.")

                null_agg_cols = []
                for col in columns:
                    # Use CASE WHEN ... IS NULL THEN 1 ELSE 0 END to count nulls
                    # For cross-db safety we avoid quoting strategy complexity here; rely on simple identifier usage
                    null_agg_cols.append(f"SUM(CASE WHEN {quote_ident(col)} IS NULL THEN 1 ELSE 0 END) AS {quote_ident(col + '_nulls')}")
                generated_sql = f"SELECT {', '.join(null_agg_cols)} FROM {quote_ident(table)};"
                sql_used = generated_sql
                provenance["llm_used_for"].append("fallback_sql_generation")
            except Exception as e:
                diagnostics["errors"].append(str(e))
                state["message"] = "Failed to build null count query due to schema introspection error."
                state["payload"] = {
                    "summary": state["message"],
                    "metadata": {"table": table},
                    "diagnostics": diagnostics,
                    "provenance": provenance,
                }
                return state

        # Step C: Execute SQL and fetch results
        try:
            df_counts = execute_sql(sql_used)
            # Expect a single-row result with columns like "<col>_nulls"
            if df_counts.empty:
                # if empty, maybe agent produced multiple rows (unlikely). Treat as error for now.
                diagnostics["warnings"].append("Null-count SQL returned no rows.")
            # normalize column names and values
            # if SQL returns a single aggregated row, take first row
            row_values = {}
            if not df_counts.empty:
                first_row = df_counts.iloc[0]
                for colname in df_counts.columns:
                    # remove trailing _nulls suffix if present
                    pretty = colname
                    if pretty.endswith("_nulls") or pretty.endswith('"_nulls"') or pretty.endswith("'_nulls'"):
                        # handle quoted columns
                        pretty = re.sub(r'(_nulls("|\'?)$)', '', pretty)
                        pretty = pretty.replace('"', '').replace("'", "")
                        # ensure clean column name
                    # attempt to unify the "pretty" name
                    if pretty.endswith("_nulls"):
                        pretty = pretty[:-7]
                    row_values[pretty] = int(first_row[colname]) if pd.notnull(first_row[colname]) else 0
            else:
                # if no rows, set all zero? fallback empty
                row_values = {}

        except Exception as e:
            diagnostics["errors"].append(f"Execution error: {str(e)}")
            state["message"] = f"Error executing null-count SQL: {str(e)}"
            state["payload"] = {
                "summary": state["message"],
                "metadata": {"table": table, "sql": sql_used},
                "diagnostics": diagnostics,
                "provenance": provenance
            }
            return state

        # Step D: Prepare visualization data (chart_data), insights, metadata, sample rows
        # Build chart_data: list of {"column": name, "null_count": count}
        chart_data = [{"column": col, "null_count": cnt} for col, cnt in row_values.items()]

        # Rows scanned: attempt to get row count if feasible (cheap count query)
        rows_scanned = None
        try:
            # Try a quick COUNT(*) - if table is large this could be expensive; but it's informative
            # For production you may want to use estimated row counts instead of COUNT(*)
            count_df = execute_sql(f"SELECT COUNT(*) AS __cnt FROM {quote_ident(table)};")
            rows_scanned = int(count_df.iloc[0]["__cnt"]) if not count_df.empty else None
        except Exception:
            # silently ignore heavy counting failures
            diagnostics["warnings"].append("Could not compute total row count (COUNT(*) failed or expensive).")

        # Sample rows - fetch limited sample for evidence
        sample_rows = []
        sample_size = 10
        try:
            sample_df = execute_sql(f"SELECT * FROM {quote_ident(table)} LIMIT {sample_size};")
            sample_rows = sample_df.to_dict(orient="records") if not sample_df.empty else []
        except Exception as e:
            diagnostics["warnings"].append(f"Failed to fetch sample rows: {str(e)}")

        # insights: create a deterministic insight per column that has nulls > 0
        insights: List[Dict[str, Any]] = []
        total_nulls = sum([v for v in row_values.values()]) if row_values else 0
        cols_with_nulls = [c for c, v in row_values.items() if v and v > 0]

        for col, cnt in row_values.items():
            if cnt > 0:
                pct = None
                if rows_scanned:
                    try:
                        pct = (cnt / rows_scanned) * 100 if rows_scanned and rows_scanned > 0 else None
                    except Exception:
                        pct = None
                description = f"Column '{col}' has {cnt} null value{'s' if cnt!=1 else ''}"
                if pct is not None:
                    description += f" ({pct:.2f}% of {rows_scanned} rows)"
                insight = {
                    "id": str(uuid.uuid4()),
                    "type": "missing_values",
                    "severity": "warning" if (pct is not None and pct > 0) else "info",
                    "metric": "null_count",
                    "value": {"null_count": cnt, "null_pct": pct},
                    "columns": [col],
                    "description": description,
                    "evidence": {
                        "sample_rows": sample_rows[:3] if sample_rows else [],
                        "sql": sql_used
                    },
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "actionable": True,
                    "suggested_actions": [f"Filter rows WHERE {col} IS NULL", "Investigate source ETL for missing values"]
                }
                insights.append(insight)

        # If no column has nulls, add a single positive info insight
        if not insights:
            insights.append({
                "id": str(uuid.uuid4()),
                "type": "missing_values",
                "severity": "info",
                "metric": "null_count",
                "value": {"null_count": 0},
                "columns": [],
                "description": f"No missing values detected across {len(row_values)} columns.",
                "evidence": {"sql": sql_used, "sample_rows": sample_rows[:3] if sample_rows else []},
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "actionable": False,
                "suggested_actions": ["Consider running distinct_count or distribution profiling"]
            })

        # Build payload following our standardized schema
        payload = {
            "summary": f"Detected missing values in {len(cols_with_nulls)} of {len(row_values)} columns." if row_values else "No columns profiled.",
            "metadata": {
                "table": table,
                "rows_scanned": rows_scanned,
                "columns_profiled": len(row_values),
                "execution_time_ms": (time.time() - start_ts) * 1000.0,
                "sql": sql_used,
                "data_source": provenance.get("engine")
            },
            "sample": {
                "sample_type": "first_n",
                "sample_size": len(sample_rows),
                "total_available": rows_scanned,
                "rows": sample_rows
            },
            "visualization": {
                "chart_type": "bar",
                "chart_data": chart_data
            },
            "insights": insights,
            "llm_commentary": None,
            "provenance": provenance,
            "diagnostics": diagnostics
        }

        # Optionally: ask LLM to produce a short commentary (non-deterministic, optional)
        # Only do this if create_langchain_sql_agent is available and configured -> re-use the LLM client (not the SQL agent)
        # To avoid introducing hallucinations into the core facts, we keep llm_commentary optional and secondary.
        if create_langchain_sql_agent:
            try:
                # Try to use agent's LLM for commentary if present (best effort)
                agent = create_langchain_sql_agent()
                commentary_prompt = (
                    f"Given the following null counts per column for table {table}:\n"
                    f"{chart_data}\n"
                    "Provide a concise single-sentence suggestion for the next profiling step."
                )
                commentary = agent.run(commentary_prompt)
                # sanitize commentary: keep short, remove SQL blocks if any
                commentary = self._strip_sql_blocks(commentary).strip()
                payload["llm_commentary"] = commentary
                provenance["llm_used_for"].append("commentary")
            except Exception as e:
                # silently ignore commentary failures but record diagnostics
                diagnostics["warnings"].append(f"LLM commentary failed: {str(e)}")

        # finalize state
        state["message"] = payload["summary"]
        state["payload"] = payload
        state["next_prompt"] = payload.get("llm_commentary") or "Would you like to run distinct counts or distribution next?"
        return state

    # ----------------------------
    # Helper utilities
    # ----------------------------
    def _extract_table_from_text(self, text: str) -> Optional[str]:
        """
        Very basic heuristic to find a table name in user text.
        e.g., "show null count for employees table" -> "employees"
        This should be replaced by a more robust parser / intent extractor or LLM in production.
        """
        text = (text or "").strip().lower()
        tokens = re.split(r'\\s+|,|\\(|\\)', text)
        # look for token followed by 'table' or preceded by 'from'
        for i, tok in enumerate(tokens):
            if tok == "table" and i > 0:
                return tokens[i - 1]
            if tok == "from" and i + 1 < len(tokens):
                return tokens[i + 1]
        # fallback: last token if looks like a name
        if tokens:
            last = tokens[-1]
            if last and len(last) > 0 and last.isidentifier():
                return last
        return None

    def _extract_sql_from_text(self, text: str) -> Optional[str]:
        """
        Heuristic: find the first SQL-looking segment in the text.
        Looks for triple-backtick blocks, or first 'select ...' occurrence.
        """
        if not text:
            return None
        # strip markdown codeblock if present
        m = re.search(r"```(?:sql)?\\n([\\s\\S]*?)```", text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        # otherwise, search for the first SELECT ... ; pattern
        m2 = re.search(r"(select[\\s\\S]*?;)", text, re.IGNORECASE)
        if m2:
            return m2.group(1).strip()
        # or return the whole text if it looks like SQL (starts with SELECT)
        if text.strip().lower().startswith("select"):
            return text.strip()
        return None

    def _strip_sql_blocks(self, text: str) -> str:
        """
        Extract the SQL code from a markdown code block, if present. 
        For example, with input like:
        ```sql
        SELECT 
            COUNT("id") AS "id_null_count", 
            COUNT("name") AS "name_null_count", 
            COUNT("age") AS "age_null_count", 
            COUNT("department") AS "department_null_count", 
            COUNT("salary") AS "salary_null_count", 
            COUNT("hire_date") AS "hire_date_null_count", 
            COUNT("is_active") AS "is_active_null_count"
        FROM employee
        WHERE "id" IS NULL OR "name" IS NULL OR "age" IS NULL OR "department" IS NULL OR "salary" IS NULL OR "hire_date" IS NULL OR "is_active" IS NULL;
        ```
        this method will return only the SQL, without code fences.
        """
        if not text:
            return ""
        # Prefer to extract SQL code from code block, otherwise fallback to guessing
        m = re.search(r"```(?:sql)?\n([\s\S]*?)```", text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        # Fallback: extract first SELECT ... ; pattern
        """
        Remove SQL code fences from text to leave a natural-language commentary.
        """
        if not text:
            return ""
        text = re.sub(r"```(?:sql)?[\\s\\S]*?```", "", text, flags=re.IGNORECASE)
        # remove leading/trailing whitespace and "SQL:" labels
        text = re.sub(r"^sql:?", "", text, flags=re.IGNORECASE).strip()
        return text


#### old code start from here ####


    def run(self, state):
        intent = state.get("intent", "profile")
        handler = self.ACTIONS.get(intent)
        if not handler:
            state["message"] = f"Unknown intent: {intent}"
            return state
        result = handler(state)
        state.update(result)
        return state

    # --------------- ACTION HANDLERS -----------------

    def perform_full_profile(self, state):
        table = self._extract_table(state)
        df = execute_sql(f"SELECT * FROM {table} LIMIT 1000")
        state["message"] = f"Profiled {table}: {len(df)} rows, {len(df.columns)} columns."
        state["payload"] = {"metadata": {"rows": len(df), "columns": len(df.columns)}}
        return state

    def get_table_schema(self, state):
        table = self._extract_table(state)
        df = execute_sql(f"PRAGMA table_info({table})")
        state["message"] = f"Schema for {table} retrieved."
        state["payload"] = {"chart_type": "table", "chart_data": df.to_dict(orient='records')}
        return state

    def get_sample_rows(self, state):
        table = self._extract_table(state)
        df = execute_sql(f"SELECT * FROM {table} LIMIT 5")
        state["message"] = f"Sample rows from {table}."
        state["payload"] = {"chart_type": "table", "chart_data": df.to_dict(orient='records')}
        return state

    def compute_null_counts(self, state):
        table = self._extract_table(state)
        df = execute_sql(f"SELECT * FROM {table} LIMIT 1000")
        nulls = df.isnull().sum().reset_index()
        nulls.columns = ["column", "null_count"]
        chart_data = nulls.to_dict(orient="records")
        state["message"] = f"Computed null counts for {len(chart_data)} columns."
        state["payload"] = {"chart_type": "bar", "chart_data": chart_data}
        return state

    def compute_distinct_counts(self, state):
        table = self._extract_table(state)
        df = execute_sql(f"SELECT * FROM {table} LIMIT 1000")
        distincts = [{"column": c, "distinct_count": df[c].nunique()} for c in df.columns]
        state["message"] = "Computed distinct counts."
        state["payload"] = {"chart_type": "bar", "chart_data": distincts}
        return state

    def generate_distribution_chart(self, state):
        table = self._extract_table(state)
        df = execute_sql(f"SELECT * FROM {table} LIMIT 1000")
        col = df.select_dtypes(include="number").columns[0] if not df.empty else None
        if col:
            chart_data = df[col].value_counts(bins=10, sort=False).reset_index()
            chart_data.columns = ["bucket", "count"]
            data = chart_data.to_dict(orient="records")
            msg = f"Distribution chart for {col}."
        else:
            data, msg = [], "No numeric columns found."
        state["message"] = msg
        state["payload"] = {"chart_type": "histogram", "chart_data": data}
        return state

    def find_duplicates(self, state):
        table = self._extract_table(state)
        df = execute_sql(f"SELECT * FROM {table} LIMIT 1000")
        duplicates = df[df.duplicated()]
        state["message"] = f"Found {len(duplicates)} duplicate rows."
        state["payload"] = {"chart_type": "bar", "chart_data": [{"duplicates": len(duplicates)}]}
        return state

    def detect_outliers(self, state):
        table = self._extract_table(state)
        df = execute_sql(f"SELECT * FROM {table} LIMIT 1000")
        col = df.select_dtypes(include="number").columns[0] if not df.empty else None
        if not col:
            state["message"] = "No numeric column found for outlier detection."
            return state
        q1, q3 = df[col].quantile([0.25, 0.75])
        iqr = q3 - q1
        outliers = df[(df[col] < q1 - 1.5 * iqr) | (df[col] > q3 + 1.5 * iqr)]
        state["message"] = f"Detected {len(outliers)} outliers in {col}."
        state["payload"] = {"chart_type": "bar", "chart_data": [{"column": col, "outliers": len(outliers)}]}
        return state

    def compare_schema(self, state):
        src, tgt = state.get("context", {}).get("source"), state.get("context", {}).get("target")
        state["message"] = f"Compared schema for {src} vs {tgt} (mocked)."
        state["payload"] = {"metadata": {"source": src, "target": tgt, "differences": 0}}
        return state

    # --------------- UTIL -----------------
    def _extract_table(self, state):
        text = state.get("userText", "").lower()
        for t in text.split():
            if not any(x in t for x in ["show", "null", "count", "profile", "table", "schema", "for"]):
                return t
        return "employees"
