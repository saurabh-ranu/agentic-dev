# backend/main.py
import json
import re
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from agents.profiling.router_node import ProfilingRouterNode

# Pydantic response model you created earlier
from models import ProfilingAgentResponse

# Profiling agent node (implements handlers like "nulls", "distincts", etc.)
from agents.profiling_agent_node import ProfilingAgentNode

app = FastAPI(title="DataSure Agentic Backend", version="1.0.0")

# very permissive CORS for local dev; tighten for prod
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

profiling_agent = ProfilingRouterNode()


# -------------------------
# Intent detection helpers
# -------------------------
def parse_intent_result(result_text: str) -> Dict[str, Optional[str]]:
    """
    Try to extract JSON like {"intent":"label","explanation":"..."} from LLM text.
    Returns dict with keys: intent, explanation, raw (original string)
    """
    if not result_text:
        return {"intent": None, "explanation": None, "raw": ""}

    # If LLM returned JSON, extract it
    try:
        m = re.search(r"\{[\s\S]*\}", result_text)
        if m:
            obj = json.loads(m.group(0))
            return {
                "intent": obj.get("intent"),
                "explanation": obj.get("explanation"),
                "raw": result_text,
            }
    except Exception:
        # fall through to heuristic parsing
        pass

    # Heuristic: look for a known label word in plain text
    normalized = result_text.lower()
    for label in ["nulls", "distincts", "distribution", "duplicates", "outliers", "schema", "full_profile", "reconciliation"]:
        if label in normalized:
            return {"intent": label, "explanation": result_text.strip(), "raw": result_text}

    return {"intent": None, "explanation": result_text.strip(), "raw": result_text}


async def detect_intent_llm(user_text: str) -> Dict[str, Optional[str]]:
    """
    Call an LLM to classify the user's intent. Uses services.llm_client.get_azure_llm()
    if available; otherwise attempts to use langchain.chat_models.ChatOpenAI.
    Returns: {"intent": str|None, "explanation": str|None, "raw": str}
    """
    if not user_text or not user_text.strip():
        return {"intent": None, "explanation": "empty query", "raw": ""}

    prompt = f"""
You are an intent classifier for dataset profiling. Allowed labels:
["nulls","distincts","distribution","duplicates","outliers","schema","full_profile","reconciliation"]

Return JSON only in this exact format:
{{"intent":"<label>","explanation":"<one-line explanation>"}}

Examples:
User: "show null count for employees table"
-> {{"intent":"nulls","explanation":"asks for missing value counts per column"}}

User: "how many unique departments are there?"
-> {{"intent":"distincts","explanation":"asks for distinct counts per column"}}

Now classify this user query (return JSON only):
USER: "{user_text}"
""".strip()

    # Try to use a local helper first (if you have one)
    try:
        # Optional helper: services/llm_client.get_azure_llm(temperature=0)
        from services.llm_client import get_openai_llm

        llm = get_openai_llm()
        # assume the helper exposes a `predict` or `invoke` method; try common names
        if hasattr(llm, "predict"):
            raw = llm.predict(prompt)
        elif hasattr(llm, "invoke"):
            raw = llm.invoke(prompt)
        elif hasattr(llm, "generate"):  # langchain v0.x pattern
            # some wrappers return a Generation object; try to coerce to string
            gen = llm.generate([prompt])
            raw = str(gen)
        else:
            raw = None
        parsed = parse_intent_result(raw or "")
        parsed["raw"] = raw or ""
        return parsed
    except Exception:
        # Fallback to LangChain ChatOpenAI if installed
        try:
            from langchain.chat_models import ChatOpenAI
            chat = ChatOpenAI(temperature=0)
            # ChatOpenAI has a .predict method that accepts a plain string
            if hasattr(chat, "predict"):
                raw = chat.predict(prompt)
            else:
                # last resort - attempt to call as a callable
                raw = chat(prompt)
            parsed = parse_intent_result(raw or "")
            parsed["raw"] = raw or ""
            return parsed
        except Exception:
            # LLM not available - return no-detection
            return {"intent": None, "explanation": "LLM not configured", "raw": ""}


# -------------------------
# Run agent endpoint
# -------------------------
@app.post("/run_agent", response_model=ProfilingAgentResponse)
async def run_agent(request: Request):
    """
    Generic endpoint to handle profiling agent requests.
    Request JSON should include:
      - session_id (optional)
      - agent: "profiling"
      - mode: "nl" or "sql"
      - userText: natural language or SQL
      - context: dict (optional), may contain table, source, target, condition
      - intent: (optional) one of supported intents (e.g., "nulls", "distincts")

    Behavior:
      - If intent present -> use it
      - Else -> detect via LLM classifier (temperature=0) and use top predicted intent
    """
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}")

    # Basic validation / normalization
    session_id = payload.get("session_id", f"session-{int( (0) )}")  # generate or keep simple
    agent = payload.get("agent", "profiling")
    user_text = payload.get("userText", "")
    mode = payload.get("mode", "nl")
    context = payload.get("context", {}) or {}
    explicit_intent = payload.get("intent")

    # Only support profiling for now
    if agent != "profiling":
        raise HTTPException(status_code=400, detail="Unsupported agent. Only 'profiling' is supported by this endpoint.")

    detected_intent_info = None
    intent_to_use = explicit_intent

    if not explicit_intent:
        # Call LLM classifier (simple, synchronous)
        detected_intent_info = await detect_intent_llm(user_text)
        intent_to_use = detected_intent_info.get("intent")

    # If still no intent, respond with a friendly error guiding user to choose intent
    if not intent_to_use:
        # Return a 200-ish assistant-like message wrapped in ProfilingAgentResponse schema.
        # Build a minimal payload so UI can display a helpful message and intention to choose an intent.
        fake_payload = {
            "summary": "Could not determine intent automatically. Please select an intent and try again.",
            "metadata": {"table": context.get("table") or None},
            "diagnostics": {"warnings": ["Intent not detected automatically."]},
        }
        response_obj = ProfilingAgentResponse(
            session_id=session_id,
            message="I couldn't detect what exactly you want me to do. Please choose an intent (e.g., nulls, distincts).",
            payload=fake_payload,
            next_prompt=None
        )
        return response_obj

    # Prepare state for ProfilingAgentNode
    state: Dict[str, Any] = {
        "session_id": session_id,
        "agent": agent,
        "mode": mode,
        "userText": user_text,
        "context": context,
        "intent": intent_to_use,
    }

    # Run the profiling node (synchronous call)
    # ProfilingAgentNode.run returns a state dict containing message and payload
    try:
        result_state = profiling_agent.run(state)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Profiling node error: {e}")

    # Attach detected intent info (if classifier was used) into result_state for transparency
    if detected_intent_info:
        # Attach in top-level and also inside payload.provenance.detected_intent if you prefer
        result_state["detected_intent"] = {
            "intent": detected_intent_info.get("intent"),
            "explanation": detected_intent_info.get("explanation"),
            "raw": detected_intent_info.get("raw"),
            "source": "llm_classifier"
        }
        # Also add to payload.provenance.detected_intent (create substructure if missing)
        payload_obj = result_state.get("payload", {})
        prov = payload_obj.get("provenance") or {}
        prov["detected_intent"] = result_state["detected_intent"]
        payload_obj["provenance"] = prov
        result_state["payload"] = payload_obj

    # Build the final Pydantic response (this will validate the payload shapes)
    try:
        response = ProfilingAgentResponse(
            session_id=result_state.get("session_id", session_id),
            message=result_state.get("message", "") or "Completed",
            payload=result_state.get("payload", {}),
            next_prompt=result_state.get("next_prompt")
        )
    except Exception as e:
        # If Pydantic validation fails, return a 500 with debug information
        raise HTTPException(status_code=500, detail=f"Response validation error: {e}")

    return response
