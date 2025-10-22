from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from models import AgentRequest, AgentResponse
from langgraph_nodes.profiling_graph import build_profiling_graph
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI(title="DataSure Agentic Backend")

# Initialize the profiling graph once at startup
profiling_graph = build_profiling_graph()

# Add CORS middleware to accept everything
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

@app.get("/")
async def root():
    return {"message": "DataSure Agentic Backend is running!"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/run_agent", response_model=AgentResponse)
async def run_agent(req: AgentRequest):
    state = req.dict()
    result_state = profiling_graph.invoke(state)  # Use the pre-built graph
    return AgentResponse(**result_state)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
