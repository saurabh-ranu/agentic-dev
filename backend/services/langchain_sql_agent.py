from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from services.llm_client import get_openai_llm
from sqlalchemy import create_engine
import os

def create_langchain_sql_agent():
    """
    Create a LangChain SQL Agent that connects to the database using SQLAlchemy,
    and can generate and execute SQL queries using the Azure OpenAI LLM.
    """
    llm = get_openai_llm()
    db_url = os.getenv("DATABASE_URL", "sqlite:///./demo.db")
    engine = create_engine(db_url)
    db = SQLDatabase(engine)

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    agent_executor = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        verbose=True
    )
    return agent_executor

