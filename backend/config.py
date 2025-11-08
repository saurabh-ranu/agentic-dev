import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "your_openai_api_key_here")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./demo.db")

# Application Configuration
DEBUG = os.getenv("DEBUG", "True").lower() == "true"

# backend/config.py
"""
Prompt templates and small constants.
Use Python .format() placeholders: {dialect}, {table}, {schema}, {user_text}
"""

SYSTEM_PROMPT = (
    "You are a professional data profiling assistant.\n"
    "Generate only a single VALID SELECT SQL query, nothing else.\n"
    "Do not include explanations, markdown, or comments.\n"
    "Use only the columns provided in the schema. Do not guess columns.\n"
    "If filters are present in the user request, include them in a WHERE clause.\n"
)

NULLS_PROMPT = (
    "Database dialect: {dialect}\n"
    "Table: {table}\n"
    "Schema:\n{schema}\n\n"
    "User request: {user_text}\n\n"
    "Task: Generate a single SQL SELECT statement that returns a single row with "
    "the count of NULL values per column in {table}. Use the pattern:\n"
    "SUM(CASE WHEN <col> IS NULL THEN 1 ELSE 0 END) AS <col>_nulls\n"
    "Apply any filters requested by the user in the WHERE clause.\n\n"
    "Output only SQL."
)

DISTINCTS_PROMPT = (
    "Database dialect: {dialect}\n"
    "Table: {table}\n"
    "Schema:\n{schema}\n\n"
    "User request: {user_text}\n\n"
    "Task: Generate a single SQL SELECT statement that returns a single row with "
    "the distinct count per column in {table}. Use the pattern:\n"
    "COUNT(DISTINCT <col>) AS <col>_distinct\n"
    "Apply any filters requested by the user in the WHERE clause.\n\n"
    "Output only SQL."
)

