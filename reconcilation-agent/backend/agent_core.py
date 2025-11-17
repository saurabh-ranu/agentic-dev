# agent_core.py

import operator
import json
#import sqlite3
import uuid
from typing import TypedDict, List, Annotated, Dict, Any, Optional
from langgraph.graph import StateGraph, END
#from langgraph.checkpoint.sqlite import SqliteSaver
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, ToolMessage
from langchain.tools import tool
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
from llm_client import get_openai_llm
from langgraph.checkpoint.memory import MemorySaver
import os

# --- Setup ---
load_dotenv()
#DB_FILE = "agent_checkpoints.sqlite"

# --- 1. State Definition ---
class AgentState(TypedDict):
    """Represents the state of the agent in the LangGraph workflow."""
    messages: Annotated[List[BaseMessage], operator.add]
    tool_calls: List[dict] # Internal tracking

# --- 2. Tool Definitions ---
# Placeholder for API calls
def api_call(request_data: Dict[str, Any]) -> Dict[str, Any]:
    print(f"\n[API CALL EXECUTED] Request:\n{json.dumps(request_data, indent=2)}")
    if request_data["action"] == "schema_key_compare":
        return {
            "status": "success", "comparison_type": "Schema/Key",
            "match_count": 9850, "mismatch_count": 150,
            "details": "Comparison complete using columns and keys."
        }
    return {"status": "success", "comparison_type": "Custom Query", "details": "Custom query comparison executed successfully."}

@tool
def compareTool(source_table: str, target_table: str, source_columns: List[str], target_columns: List[str], source_key: str, target_key: str) -> str:
    """Compares data between two tables using specified schemas and keys."""
    request = {"action": "schema_key_compare", "source_table": source_table, "target_table": target_table, "source_columns": source_columns, "target_columns": target_columns, "source_key": source_key, "target_key": target_key}
    return json.dumps(api_call(request))

@tool
def compareToolUsingCustomQuery(source: str, target: str, source_table: str, target_table: str, source_query: str, target_query: str) -> str:
    """Compares data using custom SQL queries for both source and target systems."""
    request = {"action": "custom_query_compare", "source_system": source, "target_system": target, "source_table": source_table, "target_table": target_table, "source_query": source_query, "target_query": target_query}
    return json.dumps(api_call(request))

TOOLS = [compareTool, compareToolUsingCustomQuery]
TOOL_MAP = {tool.name: tool for tool in TOOLS}

# --- 3. LLM and Prompt (For Conversational Memory) ---
SYSTEM_PROMPT = """
You are an expert Data Comparison Agent. Use the full conversation history to gather all required parameters.
* **CRITICAL RULE:** If you are missing any required arguments for a tool call, **DO NOT** make up values. Instead, you must **ask the user conversationally** for the specific missing inputs.
* If a tool call result (a JSON string) is returned, present the result in a clear, conversational summary.
"""

llm = get_openai_llm()
llm = ChatOpenAI(model="gpt-4o", temperature=0).bind_tools(TOOLS)

# --- 4. Graph Nodes ---
def call_agent(state: AgentState) -> Dict[str, List[BaseMessage]]:
    messages = state["messages"]
    response = llm.invoke(messages)
    return {"messages": [response]}

def call_tool(state: AgentState) -> Dict[str, List[BaseMessage]]:
    last_message = state["messages"][-1]
    tool_results = []
    for tool_call in last_message.tool_calls:
        output = TOOL_MAP[tool_call["name"]].invoke(tool_call["args"])
        tool_results.append(ToolMessage(content=output, tool_call_id=tool_call["id"]))
    return {"messages": tool_results}

def decide_next_step(state: AgentState) -> str:
    last_message = state["messages"][-1]
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        print("-> Transition: Tool Call Detected -> Going to call_tool")
        return "call_tool"
    print("-> Transition: Final Answer/Prompt Detected -> Ending")
    return END


memory = MemorySaver()
# --- 5. Graph Compilation with Checkpointer ---
#memory = SqliteSaver.from_conn_string(f"sqlite:///{DB_FILE}")

#DB_FILE = os.path.join("agent_checkpoints.sqlite.db")
#db_path = os.path.join(os.getcwd(), "checkpoint.db")
# Ensure the directory exists (SQLite creates the file, not the directory)
#os.makedirs(os.path.dirname(db_path), exist_ok=True)

# Use the recommended method to create the checkpointer instance
# try:
#     memory = SqliteSaver.from_conn_string("sqlite:///./checkpoint.db")
# except Exception as e:
#     print(f"Error creating SqliteSaver: {e}")
#     # Fallback or exit if persistence fails
#     raise

workflow = StateGraph(AgentState)
workflow.add_node("agent", call_agent)
workflow.add_node("call_tool", call_tool)
workflow.set_entry_point("agent")
workflow.add_conditional_edges(
    "agent", decide_next_step, {"call_tool": "call_tool", END: END},
)
workflow.add_edge("call_tool", "agent")

import tempfile
temp_dir = tempfile.gettempdir()
db_path = os.path.join(temp_dir, "checkpoint.db")
db_dir = r"D:\sqllitedemo" 
#os.makedirs(db_dir, exist_ok=True) 
db_path = os.path.join(db_dir, "checkpoint.db")

# Compile the agent with the memory layer
agent_app = workflow.compile(checkpointer=memory)