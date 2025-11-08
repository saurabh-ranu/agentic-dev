# backend/agents/nodes/base.py
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field

class AgentState(BaseModel):
    session_id: str
    userText: str
    intent: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)
    payload: Optional[Dict[str, Any]] = None
    message: Optional[str] = None
    next_prompt: Optional[str] = None
    awaiting_input: bool = False
    missing_params: Optional[List[str]] = None

class BaseNode:
    def validate_required(self, state: AgentState, required_params: List[str]) -> Optional[str]:
        """Validate presence of required context parameters."""
        for param in required_params:
            if not state.context.get(param):
                return param
        return None
