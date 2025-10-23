# backend/models.py
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Any, Optional
from datetime import datetime


class Insight(BaseModel):
    id: Optional[str] = None
    type: str
    severity: Optional[str] = Field(default="info", pattern="^(info|warning|critical)$")
    metric: Optional[str] = None
    value: Optional[Any] = None
    columns: Optional[List[str]] = None
    description: str
    evidence: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None
    actionable: Optional[bool] = False
    suggested_actions: Optional[List[str]] = None


class Visualization(BaseModel):
    chart_type: str = Field(..., pattern="^(bar|histogram|table|pie|line|boxplot|scatter)$")
    chart_data: Optional[List[Dict[str, Any]]] = None


class SampleData(BaseModel):
    sample_type: Optional[str] = Field(default="first_n")
    sample_size: Optional[int] = None
    total_available: Optional[int] = None
    rows: List[Dict[str, Any]]


class Metadata(BaseModel):
    table: Optional[str]
    rows_scanned: Optional[int] = None
    columns_profiled: Optional[int] = None
    execution_time_ms: Optional[float] = None
    sql: Optional[str] = None
    data_source: Optional[str] = None


class Provenance(BaseModel):
    engine: Optional[str] = None
    executor: Optional[str] = None
    llm_used_for: Optional[List[str]] = None


class Diagnostics(BaseModel):
    warnings: Optional[List[str]] = None
    truncated: Optional[bool] = False
    errors: Optional[List[str]] = None


class ProfilingPayload(BaseModel):
    summary: str
    metadata: Metadata
    sample: Optional[SampleData] = None
    visualization: Optional[Visualization] = None
    insights: Optional[List[Insight]] = None
    llm_commentary: Optional[str] = None
    provenance: Optional[Provenance] = None
    diagnostics: Optional[Diagnostics] = None


class ProfilingAgentResponse(BaseModel):
    session_id: str
    message: str
    payload: ProfilingPayload
    next_prompt: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
