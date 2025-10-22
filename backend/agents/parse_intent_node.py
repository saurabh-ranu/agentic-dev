class ParseIntentNode:
    def run(self, state):
        text = state.get("userText", "").lower()
        intent = "profile"
        if "schema" in text:
            intent = "schema"
        elif "null" in text:
            intent = "nulls"
        elif "distinct" in text:
            intent = "distinct"
        elif "distribution" in text or "histogram" in text:
            intent = "distribution"
        elif "duplicate" in text:
            intent = "duplicates"
        elif "outlier" in text:
            intent = "outliers"
        elif "compare" in text:
            intent = "compare"
        elif "sample" in text:
            intent = "sample"
        state["intent"] = intent
        return state
