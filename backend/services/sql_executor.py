from sqlalchemy import create_engine, text
import pandas as pd, os

def execute_sql(query: str):
    db_url = os.getenv("DATABASE_URL", "sqlite:///./demo.db")
    if not db_url.startswith("sqlite:///"):
        raise ValueError(f"Expected a SQLite database URL, got '{db_url}'")
    db_path = db_url.replace("sqlite:///", "")
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"SQLite database file not found at: {db_path}")
    engine = create_engine(db_url)
    if not query.strip().lower().startswith("select"):
        raise ValueError("Unsafe query detected")
    with engine.connect() as conn:
        result = conn.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df
