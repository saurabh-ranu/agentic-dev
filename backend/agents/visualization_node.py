class VisualizationNode:
    def run(self, state):
        payload = state.get("payload", {})
        msg = state.get("message", "Visualization generated.")
        state["message"] = msg
        state["chart_type"] = payload.get("chart_type")
        state["chart_data"] = payload.get("chart_data")
        return state
