# agents/utils/helpers.py
import re

def quote_ident(identifier: str) -> str:
    """Simple identifier quoting (double quotes)."""
    if not identifier:
        return identifier
    if '"' in identifier or "'" in identifier:
        return identifier
    return f'"{identifier}"'

def extract_table_from_text(text: str) -> str | None:
    """Small heuristic to extract table name from userText."""
    if not text:
        return None
    text = text.strip()
    tokens = re.split(r'\s+|,|\(|\)', text)
    tokens = [t for t in tokens if t]
    for i, tok in enumerate(tokens):
        if tok.lower() == "table" and i > 0:
            return tokens[i - 1]
        if tok.lower() == "from" and i + 1 < len(tokens):
            return tokens[i + 1]
    # fallback: last token if looks like an identifier
    last = tokens[-1]
    if last.isidentifier():
        return last
    return None

def strip_sql_blocks(text: str) -> str:
    """
    Extract SQL code from outputs that may include markdown fences or commentary.
    Returns empty string if nothing SQL-like found.
    """
    if not text:
        return ""
    # prefer fenced code-block
    m = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # find first SELECT ... ; block
    m2 = re.search(r"(select[\s\S]*?;)", text, re.IGNORECASE)
    if m2:
        return m2.group(1).strip()
    # if starts with select, return whole
    if text.strip().lower().startswith("select"):
        return text.strip()
    return ""
