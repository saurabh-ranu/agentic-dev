# agents/utils/sql_generator.py
from typing import Tuple
from utils.helpers import strip_sql_blocks
import traceback

# Prefer your create_langchain_sql_query helper if present
try:
    from services.langchain_sql_agent import create_langchain_sql_query
except Exception:
    create_langchain_sql_query = None

def generate_sql_for_intent(intent: str, table: str, user_text: str | None, system_prompt: str) -> Tuple[str | None, dict]:
    """
    Try to generate SQL using the SQL query chain (LLM). Returns (sql_text_or_None, provenance).
    Provenance contains metadata about LLM usage and warnings.
    """
    provenance = {"llm_used_for": [], "warnings": [], "source": None}
    sql_text = None

    prompt_user_text = (user_text or "").strip()
    if not system_prompt:
        prompt = f"""
        You are a SQL generation assistant.
        Intent: "{intent}"
        Table: `{table}`
        User message: \"\"\"{prompt_user_text}\"\"\"

        Goal:
        Generate a single READ-ONLY SQL query that implements the intent above.
        Return only the SQL (no commentary, no markdown). Use the table name exactly as provided where possible.
        """
    try:
        if create_langchain_sql_query:
            agent = create_langchain_sql_query(system_prompt)
            # the chain may accept different call signatures; try common ones
            raw = None
            try:
                raw = agent.invoke({"question": prompt_user_text})
            except Exception:
                try:
                    # some chain forms accept a direct string
                    raw = agent.run(prompt)
                except Exception:
                    raw = None
            if raw:
                sql_text = strip_sql_blocks(raw)
                if sql_text:
                    provenance["llm_used_for"].append("sql_generation")
                    provenance["source"] = "llm"
                else:
                    provenance["warnings"].append("LLM returned no parsable SQL.")
        else:
            provenance["warnings"].append("No SQL query chain configured (create_langchain_sql_query missing).")
    except Exception as e:
        provenance["warnings"].append(f"LLM SQL generation exception: {str(e)}")
        provenance["warnings"].append(traceback.format_exc())

    return sql_text, provenance
