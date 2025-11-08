# backend/main.py
import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from models import AgentState, ProfilingAgentResponse, ProfilingPayload, empty_response
from graph.profiling_graph import build_profiling_graph

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="DataSure Profiling Agent (Stable Main)", version="2.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Build the compiled graph once
profiling_graph = build_profiling_graph()

# Simple in-memory session store (state dicts). Replace with Redis/DB for production.
SESSIONS: Dict[str, Dict[str, Any]] = {}


def normalize_invoke_result(raw_result: Any) -> Dict[str, Any]:
    """
    Normalize whatever LangGraph returns into a plain dict of state values.
    """
    if raw_result is None:
        return {}
    if isinstance(raw_result, dict):
        return dict(raw_result)
    # Many LangGraph returns have `model_dump()` or similar
    if hasattr(raw_result, "model_dump"):
        return raw_result.model_dump()
    if hasattr(raw_result, "__dict__"):
        # fallback - but ensure we get the internal `.values` if present
        data = dict(getattr(raw_result, "values", raw_result.__dict__))
        return data
    raise RuntimeError("Unable to normalize graph result into dict.")


@app.post("/run_agent", response_model=ProfilingAgentResponse)
async def run_agent(request: Request):
    """
    Conversational endpoint for the profiling agent.

    Expected JSON:
      {
        "session_id": "sess-123",
        "userText": "show nulls",
        "context": { ... }  # optional, user-supplied context updates
      }
    """
    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    session_id: str = body.get("session_id") or "session-default"
    user_text: str = (body.get("userText") or "").strip()
    context_update: Dict[str, Any] = body.get("context") or {}

    if not user_text:
        return empty_response(session_id, "Please enter a message to start.")

    # Prepare config for graph.invoke (thread id useful for internal logs / checkpointer)
    config = {"configurable": {"thread_id": session_id}}

    # Build or resume state dict (plain dict structure expected by LangGraph)
    prior = SESSIONS.get(session_id)
    if prior:
        # Resume: merge prior state with new userText and context updates
        state_dict = dict(prior)  # shallow copy
        # Append to history
        history = state_dict.get("userText_history") or []
        history = list(history)  # ensure mutable copy
        history.append(user_text)
        state_dict["userText_history"] = history
        state_dict["userText"] = user_text
        state_dict["resumed"] = True
        ctx = state_dict.get("context") or {}
        ctx.update(context_update or {})
        if not state_dict["awaiting_input"]:
            state_dict["intent"] = None
            ctx = {}
        # Merge context updates (if provided)

        state_dict["context"] = ctx
    else:
        # Fresh conversation
        state_dict = {
            "session_id": session_id,
            "userText": user_text,
            "userText_history": [user_text],
            "intent": None,
            "awaiting_input": False,
            "missing_params": [],
            "context": context_update or {},
            "message": None,
            "payload": None,
            "next_prompt": None,
            "provenance": {},
        }

    # Invoke the graph. Always pass `config` first and a plain dict as second.
    try:
        raw_result = profiling_graph.invoke(state_dict, config=config)
    except Exception as e:
        # Surface a useful error for debugging (no internal stack leak)
        logger.exception("LangGraph invoke failed")
        raise HTTPException(status_code=500, detail=f"Graph execution error: {e}")

    # Normalize to dict
    try:
        result_data: Dict[str, Any] = normalize_invoke_result(raw_result)
    except Exception as e:
        logger.exception("Normalization failed")
        raise HTTPException(status_code=500, detail=f"Graph result normalization error: {e}")

    # Persist the latest state for the session (so next call we can resume)
    # Keep only serializable parts in session store
    SESSIONS[session_id] = result_data

    # Prepare safe output fields for response model
    safe_session_id = result_data.get("session_id", session_id)
    safe_message = result_data.get("message") or "No message returned."
    raw_payload = result_data.get("payload") or {}

    # Ensure payload can be validated by ProfilingPayload
    if isinstance(raw_payload, dict):
        # Build ProfilingPayload via pydantic to validate shape (but allow flexible dicts)
        try:
            validated_payload = ProfilingPayload(**raw_payload)
        except Exception:
            # If validation fails, fall back to a minimal payload that contains summary & metadata
            validated_payload = ProfilingPayload(
                summary=safe_message,
                metadata={"session_id": safe_session_id},
                diagnostics={"warnings": ["Payload did not match precise schema; returned as minimal payload."]}
            )
    else:
        # Raw payload is not a dict; replace with minimal fallback
        validated_payload = ProfilingPayload(
            summary=safe_message,
            metadata={"session_id": safe_session_id},
            diagnostics={"warnings": ["Payload not a dict; replaced with minimal payload."]}
        )

    safe_next_prompt = result_data.get("next_prompt")

    response = ProfilingAgentResponse(
        session_id=safe_session_id,
        message=safe_message,
        payload=validated_payload,
        next_prompt=safe_next_prompt,
    )

    return response


@app.get("/")
async def root():
    return {"status": "ok", "message": "DataSure Profiling Agent is running"}
