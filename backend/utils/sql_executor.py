# agents/utils/sql_executor.py
import os
import pandas as pd

# Prefer your existing execute_sql helper if present
try:
    # If you already have services.sql_executor.execute_sql, use it
    from services.sql_executor import execute_sql as _exec_helper
except Exception:
    _exec_helper = None

def execute_sql(query: str, db_url: str | None = None) -> pd.DataFrame:
    """
    Execute SQL and return a pandas DataFrame.
    If services.sql_executor.execute_sql exists, prefer it. Otherwise, use pandas + sqlalchemy.
    """
    if _exec_helper:
        # assume helper returns a DataFrame
        return _exec_helper(query)

    # fallback: use sqlalchemy + pandas
    from sqlalchemy import create_engine
    db_url = db_url or os.getenv("DATABASE_URL", "sqlite:///./demo.db")
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df
