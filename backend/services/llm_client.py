from langchain_openai import ChatOpenAI
import os

def get_openai_llm():
    return ChatOpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0,
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    )
