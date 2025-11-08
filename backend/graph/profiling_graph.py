# backend/graph/profiling_graph.py

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from models import AgentState

# Import nodes
from agents.nodes.intent_detection_node import IntentDetectionNode
from agents.nodes.null_count_node import NullCountNode
from agents.nodes.distinct_count_node import DistinctCountNode
from agents.nodes.clarification_node import ClarificationNode


# --------------------------------------------------
# ROUTER FUNCTION: Decide which profiling node to call
# --------------------------------------------------
from typing import Optional

def route_by_intent(state: AgentState) -> Optional[str]:
    """
    Routes to the correct profiling node.
    - If awaiting user input → pause (avoid recursion)
    - If intent is None or unknown → ask once, then stop (no infinite clarify loop)
    """
    # 1️⃣ Still waiting for user input
    if state.awaiting_input and not state.resumed:
        return None  # pause, await next user message

    if state.awaiting_input and state.resumed:
        return "clarify"  # pause, await next user message

    # 2️⃣ No detected intent (LLM failed or not provided)
    intent = (state.intent or "").lower().strip()
    if not intent:
        # Send helpful message and stop
        state.message = (
            "I couldn’t identify what you want to do. "
            "Please specify — for example, ‘show nulls’ or ‘check distincts’."
        )
        state.awaiting_input = True
        state.missing_params = ["intent"]
        return None  # stop; don’t loop back

    # 3️⃣ Route to correct profiling node
    if intent == "nulls":
        return "null_count"
    elif intent == "distincts":
        return "distinct_count"

    # 4️⃣ Unknown intent fallback — single clarify, no loop
    state.message = f"I’m not sure how to handle '{intent}'. Try 'nulls' or 'distincts'."
    state.awaiting_input = True
    state.missing_params = ["intent"]
    return None  # stop; don't loop endlessly



# --------------------------------------------------
# BUILD PROFILING LANGGRAPH
# --------------------------------------------------
def build_profiling_graph() -> StateGraph:
    """
    Constructs a full conversational LangGraph for the profiling agent.
    Supports intent detection, clarification, and execution for:
    - Null counts
    - Distinct counts
    """
    graph = StateGraph(AgentState)

    # Initialize nodes
    intent_detector = IntentDetectionNode()
    clarifier = ClarificationNode()
    null_node = NullCountNode()
    distinct_node = DistinctCountNode()

    # Register nodes
    graph.add_node("intent_detect", intent_detector.run)
    graph.add_node("clarify", clarifier.run)
    graph.add_node("null_count", null_node.run)
    graph.add_node("distinct_count", distinct_node.run)

    # Start → Intent Detection
    graph.set_entry_point("intent_detect")

    # Route after intent detection
    graph.add_conditional_edges(
        "intent_detect",
        route_by_intent,
        {
            "null_count": "null_count",
            "distinct_count": "distinct_count",
            "clarify": "clarify",
            None: END,
        },
    )

    # After null or distinct profiling → END (or future feedback loop)
    graph.add_edge("null_count", END)
    graph.add_edge("distinct_count", END)

    # Clarification node can send back to router after receiving missing input
    graph.add_conditional_edges(
        "clarify",
        route_by_intent,
        {
            "null_count": "null_count",
            "distinct_count": "distinct_count",
            None: END,
        },
    )

    # Attach memory for session persistence
    memory = MemorySaver()

    # Compile and return
    return graph.compile(checkpointer=memory)
