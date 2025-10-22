from pydantic import BaseModel
from typing import Optional, Dict, List, Any

class AgentRequest(BaseModel):
    session_id: str
    agent: str
    mode: str = "nl"
    userText: str
    context: Optional[Dict[str, Any]] = None

class AgentResponse(BaseModel):
    session_id: str
    message: str
    payload: Optional[Dict[str, Any]] = None
    next_prompt: Optional[str] = None
    chart_type: Optional[str] = None
    chart_data: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None
    conversation: Optional[List[Dict[str, Any]]] = None
