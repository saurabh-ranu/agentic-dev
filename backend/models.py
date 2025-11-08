# backend/models.py
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ----------------------------
# Core Agent Conversation State
# ----------------------------
class AgentState(BaseModel):
    """
    Represents the full conversation context shared across LangGraph nodes.
    This is the 'state' that MemorySaver persists for each session/thread.
    """
    session_id: Optional[str] = None
    userText: Optional[str] = None
    userText_history: List[str] = Field(default_factory=list)
    resumed: Optional[bool] = False

    # Conversational info
    intent: Optional[str] = None
    awaiting_input: bool = False
    missing_params: List[str] = Field(default_factory=list)

    # Contextual info
    context: Dict[str, Any] = Field(default_factory=dict)

    # Computed response
    message: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    next_prompt: Optional[str] = None

    # Provenance / memory info
    provenance: Dict[str, Any] = Field(default_factory=dict)

    def add_user_text(self, text: str):
        """Append new user text to history."""
        if text:
            self.userText_history.append(text.strip())
            self.userText = text.strip()

    def combined_user_text(self, last_n: int = 5) -> str:
        """Combine recent user messages into one coherent context string."""
        return " ".join(self.userText_history[-last_n:])

# ----------------------------
# Response Models for FastAPI
# ----------------------------
class Insight(BaseModel):
    id: str
    type: str
    severity: str
    metric: str
    description: str
    columns: List[str]
    value: Dict[str, Any]
    evidence: Optional[Dict[str, Any]] = None
    actionable: Optional[bool] = None
    suggested_actions: Optional[List[str]] = None


class Visualization(BaseModel):
    chart_type: Optional[str]
    chart_data: Optional[List[Dict[str, Any]]]


class ProfilingPayload(BaseModel):
    summary: Optional[str]
    metadata: Optional[Dict[str, Any]]
    sample: Optional[Dict[str, Any]] = None
    visualization: Optional[Visualization] = None
    insights: Optional[List[Insight]] = None
    llm_commentary: Optional[str] = None
    provenance: Optional[Dict[str, Any]] = None
    diagnostics: Optional[Dict[str, Any]] = None


class ProfilingAgentResponse(BaseModel):
    """
    Final FastAPI response model, validated on output.
    """
    session_id: str
    message: str
    payload: Optional[ProfilingPayload]
    next_prompt: Optional[str] = None


# ----------------------------
# Helper: Default empty response
# ----------------------------
def empty_response(session_id: str, message: str) -> ProfilingAgentResponse:
    """Generate a minimal safe ProfilingAgentResponse when nothing else is available."""
    return ProfilingAgentResponse(
        session_id=session_id,
        message=message,
        payload=ProfilingPayload(summary=message, metadata={}, diagnostics={}),
        next_prompt=None,
    )
