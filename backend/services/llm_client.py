from langchain_openai import ChatOpenAI
import os, re, json
from typing import Optional, Dict

from dotenv import load_dotenv

load_dotenv()

def get_openai_llm():
    return ChatOpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0,
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    )


# ----------- Intent Detection -------------
def detect_intent_llm(user_text: str) -> Dict[str, Optional[str]]:
    """
    Uses LLM to classify user query intent among supported profiling types.
    Returns {"intent": "nulls"/"distincts"/..., "explanation": "..."}.
    """
    if not user_text.strip():
        return {"intent": None, "explanation": "empty input"}

    prompt = f"""
You are an intent classifier for data profiling tasks.
Allowed intents:
["nulls", "distincts", "distribution", "duplicates", "outliers", "schema", "full_profile"]

Respond ONLY in JSON:
{{
  "intent": "<one of allowed intents or null>",
  "explanation": "<why you classified it this way>"
}}

User input: "{user_text}"
""".strip()

    try:
        llm = get_openai_llm()
        raw = llm.invoke(prompt).content
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            obj = json.loads(m.group(0))
            return obj
        return heuristic_intent(user_text)
    except Exception:
        return heuristic_intent(user_text)


def heuristic_intent(text: str) -> Dict[str, Optional[str]]:
    t = text.lower()
    if "null" in t:
        return {"intent": "nulls", "explanation": "Detected null-count request"}
    if "distinct" in t or "unique" in t:
        return {"intent": "distincts", "explanation": "Detected distinct-count request"}
    if "distribution" in t:
        return {"intent": "distribution", "explanation": "Detected distribution request"}
    return {"intent": None, "explanation": "Could not classify intent"}


# ----------- Table Extraction -------------
def extract_table_llm(user_text: str) -> Optional[str]:
    """
    Extracts table name either by regex or LLM fallback.
    """
    # Simple regex heuristic
    match = re.search(r"for\s+([a-zA-Z_][a-zA-Z0-9_]*)", user_text.lower())
    if match:
        return match.group(1)


    prompt = f"""
Extract the table name from this sentence. 
If a table name is present, return ONLY the table name (no extra text). 
If not present, return a blank string.
Example: "show nulls for employees table" -> employees
Example: "show nulls" -> 

User: "{user_text}"
"""
    try:
        llm = get_openai_llm()
        response = llm.invoke(prompt).content.strip()
        response = re.sub(r"[^a-zA-Z0-9_]", "", response)
        return response or None
    except Exception:
        return None