# agents/profiling/null_count_node.py
from agents.profiling.base_node import BaseProfilingNode
from utils.helpers import quote_ident
from utils.sql_executor import execute_sql
import re
import time
import uuid
from datetime import datetime

class NullCountNode(BaseProfilingNode):
    def run_profiling(self, state: dict) -> dict:
        start_ts = time.time()
        table = self.get_table(state)
        user_text = state.get("userText", "")
        intent = "nulls"

        provenance = {"_start_ts": start_ts, "engine": None, "llm_used_for": []}
        diagnostics = {"warnings": [], "errors": []}
        sql_used = None

        system_prompt = f"""
                        You are a SQL generator focused on returning NULL-count metrics.
        Instruction: Given a user's short question (provided at invocation), produce a single, safe SELECT statement
        that returns NULL counts per column for the specified table. Use the canonical pattern for null counts.
        If specific columns are mentioned, restrict to those.
        Intent: "nulls"
        Table: `{table}`
        If the user provided filters (e.g., WHERE conditions), apply them.
        If specific columns are mentioned, restrict to those.
        If unclear, include all columns.
        Return only SQL — no commentary, no markdown.
                """

        # Try LLM SQL generation
        gen_sql, gen_prov = self.generate_sql(intent, table, user_text, system_prompt)
        if gen_prov:
            provenance.update(gen_prov)
        if gen_sql:
            sql_used = gen_sql

        # Fallback deterministic SQL generation using introspection if LLM not provided SQL
        if not sql_used:
            try:
                # naive reflection: attempt to use SQL to gather column names via PRAGMA or information_schema
                # We will attempt a generic PRAGMA approach for sqlite and fallback to a minimal approach.
                # Use execute_sql to get column list safely if possible
                try:
                    cols_df = execute_sql(f"PRAGMA table_info({quote_ident(table)})")
                    columns = [r.get("name") for r in cols_df.to_dict(orient="records")]
                except Exception:
                    # fallback: try querying zero rows (SELECT * LIMIT 0) and inspect columns
                    df0 = execute_sql(f"SELECT * FROM {quote_ident(table)} LIMIT 0")
                    columns = list(df0.columns) if not df0 is None else []
                if not columns:
                    raise RuntimeError(f"Could not introspect columns for table {table}")

                null_agg_cols = []
                for col in columns:
                    null_agg_cols.append(f"SUM(CASE WHEN {quote_ident(col)} IS NULL THEN 1 ELSE 0 END) AS {quote_ident(col + '_nulls')}")
                sql_used = f"SELECT {', '.join(null_agg_cols)} FROM {quote_ident(table)};"
                provenance["llm_used_for"].append("fallback_sql_generation")
            except Exception as e:
                diagnostics["errors"].append(str(e))
                state["message"] = f"Failed to generate fallback null-count SQL: {e}"
                state["payload"] = {
                    "summary": state["message"],
                    "metadata": {"table": table},
                    "diagnostics": diagnostics,
                    "provenance": provenance
                }
                return state

        # Execute SQL
        try:
            df_counts = execute_sql(sql_used)
        except Exception as e:
            diagnostics["errors"].append(f"Execution error: {str(e)}")
            state["message"] = f"Error executing SQL: {e}"
            state["payload"] = {
                "summary": state["message"],
                "metadata": {"table": table, "sql": sql_used},
                "diagnostics": diagnostics,
                "provenance": provenance
            }
            return state

        # Normalize result to chart_data and insights
        chart_data = []
        insights = []
        row_values = {}
        if df_counts is not None and not df_counts.empty:
            first_row = df_counts.iloc[0]
            for colname in df_counts.columns:
                pretty = colname
                # remove trailing _nulls or quotes
                pretty = pretty.replace('"', '').replace("'", "")
                if pretty.endswith("_nulls"):
                    col = pretty[:-7]
                else:
                    col = pretty
                try:
                    cnt = int(first_row[colname]) if first_row[colname] is not None else 0
                except Exception:
                    try:
                        cnt = int(df_counts.iloc[0].loc[colname])
                    except Exception:
                        cnt = 0
                row_values[col] = cnt
                chart_data.append({"column": col, "null_count": cnt})
        else:
            diagnostics["warnings"].append("Null-count query returned no rows.")

        # rows_scanned (optional)
        rows_scanned = None
        try:
            cntdf = execute_sql(f"SELECT COUNT(*) AS __cnt FROM {quote_ident(table)};")
            if cntdf is not None and not cntdf.empty:
                rows_scanned = int(cntdf.iloc[0]["__cnt"])
        except Exception:
            diagnostics["warnings"].append("COUNT(*) failed or not available; rows_scanned unknown")

        # Build insights: one insight per column with nulls > 0, else a positive info insight
        cols_with_nulls = [c for c,v in row_values.items() if v and v > 0]
        for col, cnt in row_values.items():
            if cnt > 0:
                pct = None
                if rows_scanned:
                    try:
                        pct = (cnt / rows_scanned) * 100 if rows_scanned and rows_scanned > 0 else None
                    except Exception:
                        pct = None
                desc = f"Column '{col}' has {cnt} null value{'s' if cnt!=1 else ''}"
                if pct is not None:
                    desc += f" ({pct:.2f}% of {rows_scanned} rows)"
                insight = {
                    "id": str(uuid.uuid4()),
                    "type": "missing_values",
                    "severity": "warning" if (pct is not None and pct > 0) else "info",
                    "metric": "null_count",
                    "value": {"null_count": cnt, "null_pct": pct},
                    "columns": [col],
                    "description": desc,
                    "evidence": {"sample_rows": [], "sql": sql_used},
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "actionable": True,
                    "suggested_actions": [f"Filter rows WHERE {col} IS NULL", "Investigate source ETL for missing values"]
                }
                insights.append(insight)

        if not insights:
            insights.append({
                "id": str(uuid.uuid4()),
                "type": "missing_values",
                "severity": "info",
                "metric": "null_count",
                "value": {"null_count": 0},
                "columns": [],
                "description": f"No missing values detected across {len(row_values)} columns.",
                "evidence": {"sql": sql_used},
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "actionable": False,
                "suggested_actions": ["Consider running distinct_count or distribution profiling"]
            })

        commentary = None
        # Optional: LLM commentary (best-effort) using create_langchain_sql_agent if available
        # try:
        #     from services.langchain_sql_agent import create_langchain_sql_agent
        #     agent = create_langchain_sql_agent()
        #     commentary_prompt = f"Given the null counts: {chart_data}\nProvide a concise next-step suggestion."
        #     commentary = None
        #     try:
        #         commentary = agent.run(commentary_prompt)
        #     except Exception:
        #         try:
        #             commentary = agent.invoke({"question": commentary_prompt})
        #         except Exception:
        #             commentary = None
        #     if commentary:
        #         provenance.setdefault("llm_used_for", []).append("commentary")
        # except Exception:
        #     commentary = None

        # finalize payload
        payload = self.build_core_payload(
            summary = f"Detected missing values in {len(cols_with_nulls)} of {len(row_values)} columns." if row_values else "No columns profiled.",
            table = table,
            sql = sql_used,
            chart_type = "bar",
            chart_data = chart_data,
            insights = insights,
            diagnostics = diagnostics,
            provenance = provenance,
            sample_rows = [],  # sample fetching can be added similarly
            rows_scanned = rows_scanned
        )

        payload["llm_commentary"] = commentary or None

        state["message"] = payload["summary"]
        state["payload"] = payload
        state["next_prompt"] = payload.get("llm_commentary") or "Would you like to run distinct counts or distribution next?"
        return state
