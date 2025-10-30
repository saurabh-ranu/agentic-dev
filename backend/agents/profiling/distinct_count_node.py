# agents/profiling/distinct_count_node.py
from agents.profiling.base_node import BaseProfilingNode
from utils.helpers import quote_ident
import re
import time
import uuid
from datetime import datetime

class DistinctCountNode(BaseProfilingNode):
    def run_profiling(self, state: dict) -> dict:
        start_ts = time.time()
        table = self.get_table(state)
        user_text = state.get("userText", "")
        intent = "distincts"

        provenance = {"_start_ts": start_ts, "engine": None, "llm_used_for": []}
        diagnostics = {"warnings": [], "errors": []}
        sql_used = None

        system_prompt = (
            "You are a SQL generator that produces DISTINCT-value summary queries.\n"
            "Instruction: Given a user's short question (provided at invocation), produce a single, safe SELECT statement\n"
            "that returns per-column distinct-counts or top-N distinct values as the user requests. Use COUNT(DISTINCT col)\n"
            "for distinct counts or an appropriate aggregated query for top-N. Do NOT output any extra explanatory text.\n"
            "If the user provided filters (e.g., WHERE conditions), apply them.\n"
            "If specific columns are mentioned, restrict to those.\n"
            "If unclear, include all columns.\n"
            "Return only SQL — no commentary, no markdown."
        )

        # Attempt LLM SQL generation
        gen_sql, gen_prov = self.generate_sql(intent, table, user_text, system_prompt)
        if gen_prov:
            provenance.update(gen_prov)
        if gen_sql:
            sql_used = gen_sql

        # Fallback SQL generation: COUNT(DISTINCT ...)
        if not sql_used:
            try:
                # Attempt to introspect columns similarly to null_count_node
                try:
                    cols_df = __import__('agents.utils.sql_executor', fromlist=['dummy'])  # placeholder to avoid circular import warnings
                except Exception:
                    pass
                # Simple fallback: attempt PRAGMA or SELECT LIMIT 0 to get columns
                from utils.sql_executor import execute_sql as _exec
                try:
                    cols_df = _exec(f"PRAGMA table_info({quote_ident(table)})")
                    columns = [r.get("name") for r in cols_df.to_dict(orient="records")]
                except Exception:
                    df0 = _exec(f"SELECT * FROM {quote_ident(table)} LIMIT 0")
                    columns = list(df0.columns) if df0 is not None and not df0.empty else []
                if not columns:
                    raise RuntimeError(f"Could not introspect columns for table {table}")
                distinct_cols = [f"COUNT(DISTINCT {quote_ident(c)}) AS {quote_ident(c + '_distinct')}" for c in columns]
                sql_used = f"SELECT {', '.join(distinct_cols)} FROM {quote_ident(table)};"
                provenance["llm_used_for"].append("fallback_sql_generation")
            except Exception as e:
                diagnostics["errors"].append(str(e))
                state["message"] = f"Failed to build distinct-count query: {e}"
                state["payload"] = {"summary": state["message"], "metadata": {"table": table}, "diagnostics": diagnostics, "provenance": provenance}
                return state

        # Execute
        try:
            from utils.sql_executor import execute_sql as _exec
            df_counts = _exec(sql_used)
        except Exception as e:
            diagnostics["errors"].append(f"Execution error: {str(e)}")
            state["message"] = f"Error executing SQL: {e}"
            state["payload"] = {"summary": state["message"], "metadata": {"table": table, "sql": sql_used}, "diagnostics": diagnostics, "provenance": provenance}
            return state

        chart_data = []
        insights = []
        row_values = {}
        if df_counts is not None and not df_counts.empty:
            first_row = df_counts.iloc[0]
            for colname in df_counts.columns:
                pretty = colname.replace('"', '').replace("'", "")
                col = pretty
                if col.endswith("_distinct"):
                    col = col[:-9]
                try:
                    cnt = int(first_row[colname]) if first_row[colname] is not None else 0
                except Exception:
                    try:
                        cnt = int(df_counts.iloc[0].loc[colname])
                    except Exception:
                        cnt = 0
                row_values[col] = cnt
                chart_data.append({"column": col, "distinct_count": cnt})
        else:
            diagnostics["warnings"].append("Distinct-count query returned no rows.")

        # rows_scanned attempt
        rows_scanned = None
        try:
            _exec = __import__('agents.utils.sql_executor', fromlist=['dummy'])
            from utils.sql_executor import execute_sql as _exec2
            cntdf = _exec2(f"SELECT COUNT(*) AS __cnt FROM {quote_ident(table)};")
            if cntdf is not None and not cntdf.empty:
                rows_scanned = int(cntdf.iloc[0]["__cnt"])
        except Exception:
            diagnostics["warnings"].append("COUNT(*) failed; rows_scanned unknown")

        # Build insights: e.g., unique columns or constant columns
        for col, cnt in row_values.items():
            desc = f"Column '{col}' has {cnt} distinct value{'s' if cnt!=1 else ''}."
            insight = {
                "id": str(uuid.uuid4()),
                "type": "distinct_values",
                "severity": "info",
                "metric": "distinct_count",
                "value": {"distinct_count": cnt},
                "columns": [col],
                "description": desc,
                "evidence": {"sql": sql_used},
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "actionable": False,
                "suggested_actions": ["Verify domain values", "Consider indexing high-cardinality columns"]
            }
            insights.append(insight)

        payload = self.build_core_payload(
            summary = f"Computed distinct counts for {len(row_values)} columns." if row_values else "No columns profiled.",
            table = table,
            sql = sql_used,
            chart_type = "bar",
            chart_data = chart_data,
            insights = insights,
            diagnostics = diagnostics,
            provenance = provenance,
            sample_rows = [],
            rows_scanned = rows_scanned
        )

        state["message"] = payload["summary"]
        state["payload"] = payload
        state["next_prompt"] = "Would you like distributions or null counts next?"
        return state
