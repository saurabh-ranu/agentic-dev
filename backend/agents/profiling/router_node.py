# agents/profiling/router_node.py
from agents.profiling.null_count_node import NullCountNode
from agents.profiling.distinct_count_node import DistinctCountNode

class ProfilingRouterNode:
    def __init__(self):
        self._nodes = {
            "nulls": NullCountNode(),
            "distincts": DistinctCountNode(),
            # add more mappings as you implement other nodes: distribution, duplicates, outliers...
        }

    def run(self, state: dict) -> dict:
        intent = state.get("intent")
        if not intent:
            # fallback: use LLM classifier or default to nulls (up to your design)
            intent = "nulls"

        node = self._nodes.get(intent)
        if not node:
            state["message"] = f"Intent '{intent}' not implemented."
            state["payload"] = {"summary": state["message"], "metadata": {}, "diagnostics": {"errors": [f"Unknown intent {intent}"]}}
            return state

        return node.run_profiling(state)
