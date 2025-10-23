# backend/main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from models import ProfilingAgentResponse
from agents.profiling_agent_node import ProfilingAgentNode

app = FastAPI(title="DataSure Agentic Backend", version="1.0.0")

# Allow CORS from frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

profiling_agent = ProfilingAgentNode()


@app.post("/run_agent", response_model=ProfilingAgentResponse)
async def run_agent(request: dict):
    """
    Generic endpoint to handle profiling agent requests.
    The request should include keys like:
    {
      "session_id": "abc123",
      "agent": "profiling",
      "mode": "nl",
      "userText": "show null count for employees table",
      "context": { "source": "sqlite-demo", "table": "employees" }
    }
    """
    try:
        if not request.get("agent"):
            raise HTTPException(status_code=400, detail="Missing agent type in request.")

        # Currently only profiling agent supported
        if request["agent"] != "profiling":
            raise HTTPException(status_code=400, detail="Unsupported agent type.")

        # Enrich request with session_id if not provided
        if "session_id" not in request:
            request["session_id"] = "default-session"

        # Prepare LangGraph-like state (simplified)
        state = {
            "session_id": request["session_id"],
            "intent": "nulls",  # For now we run null_count profiling
            "agent": request["agent"],
            "mode": request.get("mode", "nl"),
            "userText": request.get("userText", ""),
            "context": request.get("context", {}),
        }

        # Execute profiling node (null count)
        result_state = profiling_agent.run(state)

        # Construct ProfilingAgentResponse
        response = ProfilingAgentResponse(
            session_id=request["session_id"],
            message=result_state.get("message"),
            payload=result_state.get("payload"),
            next_prompt=result_state.get("next_prompt"),
        )

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {str(e)}")
