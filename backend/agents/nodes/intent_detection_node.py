# backend/agents/intent_detection_node.py

import json
import re
from typing import Dict, Optional
from models import AgentState
from services.llm_client import get_openai_llm


class IntentDetectionNode:
    """
    Node responsible for detecting the user's intent (e.g., "nulls", "distincts").
    Uses LLM classification when not explicitly provided or stored in memory.
    """

    SUPPORTED_INTENTS = [
        "nulls",
        "distincts",
        "distribution",
        "duplicates",
        "outliers",
        "schema",
        "full_profile"
    ]

    def _parse_result(self, raw: str) -> Dict[str, Optional[str]]:
        """Extract JSON object with intent and explanation from model output."""
        if not raw:
            return {"intent": None, "explanation": None}

        try:
            # extract JSON block from response
            match = re.search(r"\{[\s\S]*\}", raw)
            if match:
                obj = json.loads(match.group(0))
                return {
                    "intent": obj.get("intent"),
                    "explanation": obj.get("explanation"),
                }
        except Exception as e:
            pass

        # fallback: keyword-based detection
        normalized = raw.lower()
        for label in self.SUPPORTED_INTENTS:
            if label in normalized:
                return {"intent": label, "explanation": f"Detected keyword '{label}'"}
        return {"intent": None, "explanation": raw.strip()}

    def run(self, state: AgentState) -> AgentState:
        """
        Detect user intent if not present in state.
        """
        if state.intent:
            # intent already known (maybe stored in memory)
            return state

        user_text = state.userText or ""
        if not user_text.strip():
            state.message = "Please tell me what profiling you want to do."
            state.intent = None
            return state

        # Construct the classifier prompt
        prompt = f"""
You are an intent classifier for a data profiling assistant.
Allowed intents: {self.SUPPORTED_INTENTS}

Return JSON only in this format:
{{"intent":"<label>","explanation":"<short reason>"}}

Example:
User: "show nulls for employees"
-> {{"intent":"nulls","explanation":"wants missing value stats per column"}}

User: "how many unique departments?"
-> {{"intent":"distincts","explanation":"wants distinct value counts"}}

Now classify this user request:
USER: "{user_text}"
""".strip()

        try:
            llm = get_openai_llm()
            if hasattr(llm, "predict"):
                raw = llm.predict(prompt)    
            elif hasattr(llm, "invoke"):
                raw = llm.invoke(prompt)
            elif hasattr(llm, "generate"):
                raw = llm.generate([prompt])
            else:
                raw = None
            parsed = self._parse_result(raw)
            state.intent = parsed.get("intent")
            state.provenance["intent_detection"] = parsed
            if not state.intent:
                state.message = (
                    "I couldn’t determine the profiling type. "
                    "You can ask me to show nulls, distincts, distributions, etc."
                )
            return state
        except Exception as e:
            state.intent = "None"
            state.message = f"Intent detection failed: {e}"
            return state


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
