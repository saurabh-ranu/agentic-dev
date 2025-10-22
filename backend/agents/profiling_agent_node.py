from services.langchain_sql_agent import create_langchain_sql_agent
from services.sql_executor import execute_sql
import pandas as pd

class ProfilingAgentNode:
    def __init__(self):
        self.ACTIONS = {
            "profile": self.perform_full_profile,
            "schema": self.get_table_schema,
            "sample": self.get_sample_rows,
            "nulls": self.compute_null_counts,
            "distinct": self.compute_distinct_counts,
            "distribution": self.generate_distribution_chart,
            "duplicates": self.find_duplicates,
            "outliers": self.detect_outliers,
            "compare": self.compare_schema,
        }

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
