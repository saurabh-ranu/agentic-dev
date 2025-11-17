# main.py (FastAPI)

import uvicorn
import uuid
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from langchain_core.runnables import RunnableConfig
from langchain_core.messages import HumanMessage
from agent_core import agent_app # Import the compiled graph

# --- Setup ---
app = FastAPI(title="LangGraph Conversational Agent API")

# Add CORS middleware to allow the React app to communicate
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for simplicity (use specific domains in production)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API Schemas ---
class ConversationRequest(BaseModel):
    """UI sends the new message and the session ID."""
    user_input: str
    thread_id: Optional[str] = None

class ConversationResponse(BaseModel):
    """API returns the final response and the thread ID for the next turn."""
    response_message: str
    thread_id: str

# --- Main Endpoint ---
@app.post("/api/chat", response_model=ConversationResponse)
async def run_agent_chat(request: ConversationRequest):
    # 1. Get or Generate Thread ID
    thread_id = request.thread_id if request.thread_id else str(uuid.uuid4())
    
    # 2. Configure LangGraph for Persistence
    # This thread_id tells the checkpointer which conversation to load/save
    config: RunnableConfig = {"configurable": {"thread_id": thread_id}}
    
    # 3. Define the Input (only the NEW message is needed)
    new_input = {"messages": [HumanMessage(content=request.user_input)]}
    
    try:
        # 4. Invoke the Graph: Loads history, runs, and saves the new history.
        output_state = agent_app.invoke(new_input, config=config)
        
        # 5. Extract the Final Response
        final_lc_message = output_state["messages"][-1]
        
        return ConversationResponse(
            response_message=final_lc_message.content,
            thread_id=thread_id,
        )

    except Exception as e:
        print(f"Error during agent invocation (thread {thread_id}): {e}")
        # Clean up the SQLite file if it exists and is corrupted
        # (For simple SQLite, robust cleanup for production uses Postgres/Redis)
        raise HTTPException(status_code=500, detail=f"Internal Agent Error: {e}")

if __name__ == "__main__":
    # Ensure OPENAI_API_KEY is set in your .env or environment
    if not os.getenv("OPENAI_API_KEY"):
         print("WARNING: OPENAI_API_KEY not found. Please set it in your environment or .env file.")
         
    # Run server
    uvicorn.run(app, host="0.0.0.0", port=8001)