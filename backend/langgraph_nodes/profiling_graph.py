from langgraph.graph import StateGraph, END
from typing import TypedDict, Optional, Dict, List, Any
from agents.parse_intent_node import ParseIntentNode
from agents.profiling_agent_node import ProfilingAgentNode
from agents.visualization_node import VisualizationNode

class ProfilingState(TypedDict):
    session_id: str
    agent: str
    mode: str
    userText: str
    context: Optional[Dict[str, Any]]
    intent: Optional[str]
    message: Optional[str]
    payload: Optional[Dict[str, Any]]
    chart_type: Optional[str]
    chart_data: Optional[List[Dict[str, Any]]]
    metadata: Optional[Dict[str, Any]]
    conversation: Optional[List[Dict[str, Any]]]

def build_profiling_graph():
    """
    Code-defined LangGraph workflow for Profiling Agent
    Supports sequence:
        ParseIntent -> ProfilingAgent -> Visualization -> END
    """
    workflow = StateGraph(ProfilingState)

    parse = ParseIntentNode()
    agent = ProfilingAgentNode()
    viz = VisualizationNode()

    # Register nodes
    workflow.add_node("ParseIntent", parse.run)
    workflow.add_node("ProfilingAgent", agent.run)
    workflow.add_node("Visualization", viz.run)

    # Define edges
    workflow.set_entry_point("ParseIntent")
    workflow.add_edge("ParseIntent", "ProfilingAgent")
    workflow.add_edge("ProfilingAgent", "Visualization")
    workflow.add_edge("Visualization", END)

    # Compile the executable graph
    return workflow.compile()
