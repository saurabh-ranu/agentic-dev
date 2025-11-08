# backend/agents/clarification_node.py
from typing import Optional
from models import AgentState


class ClarificationNode:
    """
    Clarifies missing parameters conversationally.

    Correct conversational flow:
      • If awaiting_input=True → assume user just replied.
      • Fill the missing parameter(s) into context.
      • If more params still missing → ask next.
      • If all params filled → clear awaiting_input and return control to router.
    """

    def run(self, state: AgentState) -> AgentState:
        ctx = state.context or {}
        user_input = (state.userText or "").strip()

        # Case 1️⃣ - User just provided a missing parameter
        if state.awaiting_input and state.missing_params:
            param_to_fill = state.missing_params[0]

            if user_input:
                # Fill this parameter from user input
                ctx[param_to_fill] = user_input
                state.context = ctx
                state.missing_params.pop(0)

                # If all params filled → continue to profiling node
                if not state.missing_params:
                    state.awaiting_input = False
                    state.resumed = False
                    state.message = (
                        f"Got it. Using {param_to_fill} = '{user_input}'. Running analysis..."
                    )
                    return state
                else:
                    # Ask for next missing param
                    next_param = state.missing_params[0]
                    state.awaiting_input = True
                    state.resumed = False
                    state.message = (
                        f"Thanks, noted {param_to_fill} = '{user_input}'. "
                        f"Please provide the {next_param} next."
                    )
                    return state

            # If user input empty → re-ask same param politely
            state.message = f"Please provide the {param_to_fill} to continue."
            state.awaiting_input = True
            return state

        # Case 2️⃣ - Fresh detection (no missing params recorded yet)
        missing_params = self._detect_missing(state)
        if missing_params:
            state.awaiting_input = True
            state.missing_params = missing_params
            ask_for = missing_params[0]
            state.message = f"Please provide the {ask_for} to continue."
            return state

        # Case 3️⃣ - Nothing to clarify
        state.awaiting_input = False
        state.missing_params = []
        return state

    # ---------------------------------------------------------------------
    def _detect_missing(self, state: AgentState) -> Optional[list]:
        """
        Simple intent-specific missing parameter detection.
        Each profiling node can reuse this to check before execution.
        """
        ctx = state.context or {}
        missing = []

        if state.intent in ["nulls", "distincts"]:
            if not ctx.get("table"):
                missing.append("table")

        # Extend here later if other intents need more parameters.
        return missing or None
