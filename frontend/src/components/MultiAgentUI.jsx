import React, { useEffect, useRef, useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";
import { Play, Activity, Zap, CheckCircle, MessageSquare, Code } from "lucide-react";

const API_URL = "http://localhost:8000/run_agent";
const STORAGE_KEY = "datasure_conversational_state_v3";

export default function MultiAgentUI() {
  // Basic session & UI state
  const [sessionId, setSessionId] = useState(() => {
    try {
      return crypto.randomUUID();
    } catch {
      return `sess-${Date.now()}`;
    }
  });
  const [selectedAgent, setSelectedAgent] = useState("profiling");
  const [intent, setIntent] = useState(""); // empty string means "auto-detect"
  const [mode, setMode] = useState("nl");
  const [source, setSource] = useState("");
  const [target, setTarget] = useState("");
  const [condition, setCondition] = useState("");
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState([
    { role: "assistant", text: "Hi — I'm your Profiling agent. Ask something like: 'show null count for employees'." },
  ]);
  const [runStatus, setRunStatus] = useState("idle"); // idle|running|success|error
  const [result, setResult] = useState(null); // payload object (ProfilingPayload)
  const [sqlPreviewOpen, setSqlPreviewOpen] = useState(false);
  const [sampleOpen, setSampleOpen] = useState(false);
  const [insightsOpen, setInsightsOpen] = useState(true);

  const messagesEndRef = useRef(null);

  // Load/persist
  useEffect(() => {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      try {
        const s = JSON.parse(saved);
        setSessionId(s.sessionId || sessionId);
        setSelectedAgent(s.selectedAgent || selectedAgent);
        setMessages(s.messages || messages);
        setResult(s.result || null);
        setSource(s.source || "");
        setTarget(s.target || "");
        setCondition(s.condition || "");
      } catch {}
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const toSave = { sessionId, selectedAgent, messages, result, source, target, condition };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(toSave));
  }, [sessionId, selectedAgent, messages, result, source, target, condition]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Helpers
  function addMessage(role, text) {
    setMessages((m) => [...m, { role, text }]);
  }

  function clearConversation() {
    setMessages([{ role: "assistant", text: `Switched to ${selectedAgent} agent. How can I help?` }]);
    setResult(null);
    setRunStatus("idle");
    setSqlPreviewOpen(false);
    setSampleOpen(false);
  }

  // Send request to backend
  async function handleSend() {
    const trimmed = (input || "").trim();
    if (!trimmed) return;
    addMessage("user", trimmed);
    setInput("");
    setRunStatus("running");

    const payload = {
      session_id: sessionId,
      agent: selectedAgent,
      mode,
      userText: trimmed,
      context: { source, target, condition, table: extractTableFromText(trimmed) },
      ...(intent ? { intent } : {})
    };

    try {
      const resp = await fetch(API_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`HTTP ${resp.status}: ${text}`);
      }

      const data = await resp.json();

      // Primary assistant message
      if (data.message) addMessage("assistant", data.message);

      // If LLM commentary exists inside payload, show as assistant line too
      if (data.payload?.llm_commentary) addMessage("assistant", data.payload.llm_commentary);

      // next_prompt (also shown as faint guidance)
      if (data.next_prompt) {
        // don't auto-add next_prompt as message; show as suggestion and also add subtle assistant bubble
        addMessage("assistant", data.next_prompt);
      }

      // Store payload for rendering
      setResult(data.payload || null);
      setRunStatus("success");
    } catch (err) {
      console.error("Run agent error:", err);
      addMessage("assistant", `Error: ${err.message || String(err)}`);
      setRunStatus("error");
    }
  }

  // Utility: tiny heuristic to pull possible table name from user text
  function extractTableFromText(text = "") {
    const t = text.toLowerCase();
    // common patterns: "for <table>", "from <table>", "table <table>"
    const m = t.match(/(?:for|from|table)\s+([a-zA-Z0-9_]+)/);
    if (m && m[1]) return m[1];
    return undefined;
  }

  // Render helpers
  function renderVisualization(payload) {
    if (!payload || !payload.visualization) return <div className="text-gray-400">No visualization available.</div>;

    const chart = payload.visualization;
    const type = (chart.chart_type || "").toLowerCase();
    const data = chart.chart_data || [];

    if (!Array.isArray(data) || data.length === 0) {
      return <div className="text-gray-500 italic">No chart data available.</div>;
    }

    if (type === "bar" || type === "histogram") {
      // infer keys
      const xKey = Object.keys(data[0])[0] || "column";
      const yKey = Object.keys(data[0])[1] || Object.keys(data[0]).find(k => k !== xKey);
      return (
        <div className="w-full h-56">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey={xKey} />
              <YAxis />
              <Tooltip />
              <Bar dataKey={yKey} name={yKey} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      );
    }

    if (type === "table") {
      const cols = Object.keys(data[0] || {});
      return (
        <div className="overflow-auto max-h-64 border rounded">
          <table className="min-w-full text-sm border-collapse">
            <thead className="bg-gray-100">
              <tr>
                {cols.map((c) => (
                  <th key={c} className="px-2 py-1 border">{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((row, i) => (
                <tr key={i} className="odd:bg-white even:bg-gray-50">
                  {cols.map((c) => (
                    <td key={c} className="px-2 py-1 border">{String(row[c])}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
    }

    return <div className="text-gray-500 italic">Unsupported chart type: {chart.chart_type}</div>;
  }

  function renderInsights(payload) {
    if (!payload?.insights || !Array.isArray(payload.insights) || payload.insights.length === 0) {
      return <div className="text-gray-400">No insights available.</div>;
    }
    return (
      <div className="space-y-3">
        {payload.insights.map((ins, idx) => {
          const sev = (ins.severity || "info").toLowerCase();
          const badge =
            sev === "critical" ? "bg-red-600" :
            sev === "warning" ? "bg-yellow-500" : "bg-green-600";
          return (
            <div key={ins.id || idx} className="p-2 border rounded">
              <div className="flex items-start justify-between">
                <div className="flex items-center gap-3">
                  <div className={`${badge} text-white px-2 py-0.5 rounded text-xs`}>{sev}</div>
                  <div className="text-sm">
                    <div className="font-medium">{ins.description}</div>
                    {ins.columns && ins.columns.length > 0 && (
                      <div className="text-xs text-gray-500 mt-1">Columns: {ins.columns.join(", ")}</div>
                    )}
                  </div>
                </div>
                <div className="text-right">
                  {ins.actionable && ins.suggested_actions && ins.suggested_actions.length > 0 && (
                    <div className="text-xs">
                      <button
                        onClick={() => {
                          // quick action: fill input with suggested action text or SQL hint
                          setInput(ins.suggested_actions[0]);
                        }}
                        className="text-blue-600 text-xs underline"
                      >
                        Use suggestion
                      </button>
                    </div>
                  )}
                </div>
              </div>

              {ins.suggested_actions && ins.suggested_actions.length > 0 && (
                <ul className="mt-2 ml-5 list-disc text-xs text-gray-600">
                  {ins.suggested_actions.map((a, j) => <li key={j}>{a}</li>)}
                </ul>
              )}
            </div>
          );
        })}
      </div>
    );
  }

  function renderSample(payload) {
    const rows = payload?.sample?.rows || [];
    if (!rows || rows.length === 0) {
      return <div className="text-gray-400">No sample rows available.</div>;
    }
    const cols = Object.keys(rows[0]);
    return (
      <div className="overflow-auto max-h-64 border rounded">
        <table className="min-w-full text-sm border-collapse">
          <thead className="bg-gray-100">
            <tr>
              {cols.map(c => <th key={c} className="px-2 py-1 border">{c}</th>)}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="odd:bg-white even:bg-gray-50">
                {cols.map(c => <td key={c} className="px-2 py-1 border">{r[c] === null ? <span className="text-gray-400">NULL</span> : String(r[c])}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  // Small UI: render metadata / SQL / provenance
  function renderMetadata(payload) {
    if (!payload?.metadata) return null;
    const md = payload.metadata;
    return (
      <div className="text-xs text-gray-600 space-y-1">
        <div><strong>Table:</strong> {md.table || "-" } &nbsp; | &nbsp; <strong>Rows:</strong> {md.rows_scanned ?? "-" } &nbsp; | &nbsp; <strong>Cols:</strong> {md.columns_profiled ?? "-"}</div>
        {md.execution_time_ms != null && <div>Execution: {md.execution_time_ms.toFixed(0)} ms</div>}
        {md.sql && (
          <div className="mt-2">
            <button onClick={() => setSqlPreviewOpen(s => !s)} className="text-sm px-2 py-1 border rounded">
              {sqlPreviewOpen ? "Hide SQL" : "Show SQL"}
            </button>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="h-screen flex bg-gray-50">
      {/* Sidebar */}
      <aside className="w-80 bg-white border-r p-4 flex flex-col">
        <h1 className="text-xl font-semibold mb-4">DataSure — Agents</h1>

        <div className="space-y-2">
          <button
            onClick={() => { setSelectedAgent("profiling"); clearConversation(); }}
            className={`w-full text-left p-3 rounded-lg flex items-center gap-3 ${selectedAgent === "profiling" ? "bg-blue-50 border border-blue-200" : "hover:bg-gray-50"}`}
          >
            <Activity size={18} />
            <div>
              <div className="font-medium">Profiling Agent</div>
              <div className="text-xs text-gray-500">Profile tables, stats, and insights</div>
            </div>
          </button>

          <button
            onClick={() => { setSelectedAgent("reconciliation"); clearConversation(); }}
            className={`w-full text-left p-3 rounded-lg flex items-center gap-3 ${selectedAgent === "reconciliation" ? "bg-blue-50 border border-blue-200" : "hover:bg-gray-50"}`}
          >
            <Zap size={18} />
            <div>
              <div className="font-medium">Reconciliation Agent</div>
              <div className="text-xs text-gray-500">Compare schemas & query diffs</div>
            </div>
          </button>
        </div>

        

        <div className="mt-auto text-xs text-gray-400">
          <div>Session: <span className="font-medium text-gray-700">{sessionId.slice(0, 8)}</span></div>
          <div className="mt-2">Mode: <span className="font-medium">{mode}</span></div>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 p-6 flex flex-col">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-2xl font-semibold capitalize">{selectedAgent} — Conversational</h2>
            <p className="text-sm text-gray-500 mt-1">Chat naturally or switch to SQL mode to provide an explicit query.</p>
          </div>

          <div className="flex items-center gap-3">
            <div className="text-sm text-gray-600">Status:</div>
            <div className="flex items-center gap-2">
              {runStatus === "idle" && <span className="text-gray-500">Idle</span>}
              {runStatus === "running" && <div className="flex items-center gap-2 text-blue-600"><Play className="animate-pulse" size={16} /> <span>Running</span></div>}
              {runStatus === "success" && <div className="flex items-center text-green-600 gap-2"><CheckCircle size={16} /> <span>Success</span></div>}
              {runStatus === "error" && <div className="flex items-center text-red-600 gap-2">✖ <span>Error</span></div>}
            </div>
          </div>
        </div>

        {/* Top controls */}
        <div className="bg-white p-4 rounded shadow-sm mb-4">
          <div className="grid grid-cols-4 gap-4">
            <div>
              <label className="text-xs text-gray-600">Source System</label>
              <input value={source} onChange={(e) => setSource(e.target.value)} placeholder="e.g., sqlite-demo" className="mt-1 block w-full border p-2 rounded" />
            </div>
            <div>
              <label className="text-xs text-gray-600">Target System</label>
              <input value={target} onChange={(e) => setTarget(e.target.value)} placeholder="e.g., snowflake-dev" className="mt-1 block w-full border p-2 rounded" />
            </div>
            <div>
              <label className="text-xs text-gray-600">Condition (optional)</label>
              <input value={condition} onChange={(e) => setCondition(e.target.value)} placeholder="e.g., age > 80" className="mt-1 block w-full border p-2 rounded" />
            </div>
            <div>
              <label className="text-xs text-gray-600">Mode</label>
              <div className="mt-1 flex gap-2">
                <button onClick={() => setMode("nl")} className={`px-3 py-2 rounded border ${mode === "nl" ? "bg-blue-50 border-blue-200" : ""}`}><MessageSquare size={16}/> NL</button>
                <button onClick={() => setMode("sql")} className={`px-3 py-2 rounded border ${mode === "sql" ? "bg-blue-50 border-blue-200" : ""}`}><Code size={16}/> SQL</button>
              </div>
            </div>
                {/* 🆕 Intent Selector */}
    <div>
      <label className="text-xs text-gray-600">Intent</label>
      <select
        value={intent}
        onChange={(e) => setIntent(e.target.value)}
        className="mt-1 block w-full border p-2 rounded"
      >
        <option value="">Auto-detect</option>
        <option value="nulls">Null Counts</option>
        <option value="distincts">Distinct Counts</option>
        <option value="distribution">Distributions</option>
        <option value="duplicates">Duplicates</option>
        <option value="outliers">Outliers</option>
        <option value="schema">Schema</option>
        <option value="full_profile">Full Profile</option>
        <option value="reconciliation">Reconciliation</option>
      </select>
    </div>
          </div>
        </div>

        {/* Chat + right panel */}
        <div className="flex-1 grid grid-cols-3 gap-4">
          {/* Chat area */}
          <section className="col-span-2 bg-white p-4 rounded shadow-sm flex flex-col">
            <div className="flex-1 overflow-auto mb-4 p-2 bg-gray-50 rounded">
              {messages.map((m, i) => (
                <div key={i} className={`mb-3 flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
                  <div className={`${m.role === "user" ? "bg-blue-600 text-white" : "bg-white border"} p-3 rounded-lg max-w-[75%]`}>
                    {m.text}
                  </div>
                </div>
              ))}
              <div ref={messagesEndRef} />
              {runStatus === "running" && (
  <div className="flex justify-start mb-3">
    <div className="bg-white border p-3 rounded-lg max-w-3/4 flex items-center gap-2 text-gray-500">
      <svg
        className="animate-spin h-4 w-4 text-blue-500"
        xmlns="http://www.w3.org/2000/svg"
        fill="none"
        viewBox="0 0 24 24"
      >
        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
        <path
          className="opacity-75"
          fill="currentColor"
          d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"
        ></path>
      </svg>
      <span>Agent thinking...</span>
    </div>
  </div>
)}
            </div>

            <div className="border-t pt-3">
              {/* SQL preview area */}
              {sqlPreviewOpen && result?.metadata?.sql && (
                <div className="mb-3 text-sm bg-gray-100 p-3 rounded whitespace-pre-wrap">
                  <strong>Generated SQL:</strong>
                  <pre className="text-xs mt-2">{result.metadata.sql}</pre>
                </div>
              )}

              <div className="flex gap-2">
                <textarea
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  className="flex-1 border p-2 rounded"
                  rows={2}
                  placeholder={mode === "nl" ? "Ask: e.g., show null count for employees table" : "Paste SQL here (e.g., SELECT ...)"} />
                <div className="flex flex-col gap-2">
                  <button onClick={handleSend} className="bg-blue-600 text-white px-4 py-2 rounded inline-flex items-center gap-2"><Play size={16}/>Send</button>
                  <button onClick={() => { setInput(""); setSqlPreviewOpen(false); }} className="px-3 py-2 border rounded">Clear</button>
                </div>
              </div>

              <div className="text-xs text-gray-500 mt-2">Tip: click a suggestion below to use it as input.</div>
            </div>
          </section>

          {/* Results & Insights panel */}
          <aside className="col-span-1 bg-white p-4 rounded shadow-sm flex flex-col">
            <div className="flex items-center justify-between mb-2">
              <h3 className="font-medium">Result & Insights</h3>
              <div className="text-xs text-gray-400">Profiling</div>
            </div>

            <div className="flex-1 overflow-auto text-sm space-y-3">
              {/* Metadata */}
              {renderMetadata(result)}

              {/* Visualization */}
              <div className="mt-2">
                {renderVisualization(result)}
              </div>

              {/* Insights collapsible */}
              <div className="mt-3">
                <div className="flex items-center justify-between">
                  <h4 className="text-sm font-medium">Insights</h4>
                  <button onClick={() => setInsightsOpen(o => !o)} className="text-xs px-2 py-1 border rounded">{insightsOpen ? "Hide" : "Show"}</button>
                </div>
                <div className="mt-2">
                  {insightsOpen ? renderInsights(result) : <div className="text-xs text-gray-400">Insights hidden</div>}
                </div>
              </div>

              {/* Sample rows */}
              <div className="mt-3">
                <div className="flex items-center justify-between">
                  <h4 className="text-sm font-medium">Sample Data</h4>
                  <button onClick={() => setSampleOpen(s => !s)} className="text-xs px-2 py-1 border rounded">{sampleOpen ? "Hide" : "Show"}</button>
                </div>
                <div className="mt-2">
                  {sampleOpen ? renderSample(result) : <div className="text-xs text-gray-400">Sample hidden</div>}
                </div>
              </div>

              {/* Provenance & Diagnostics */}
              {result?.provenance && (
                <div className="mt-3 text-xs text-gray-500">
                  <div><strong>Engine:</strong> {result.provenance.engine}</div>
                  <div><strong>Executor:</strong> {result.provenance.executor}</div>
                  {result.provenance.llm_used_for && <div><strong>LLM:</strong> {result.provenance.llm_used_for.join(", ")}</div>}
                </div>
              )}

              {result?.diagnostics && (result.diagnostics.warnings?.length > 0 || result.diagnostics.errors?.length > 0) && (
                <div className="mt-3 text-xs">
                  {result.diagnostics.warnings?.length > 0 && <div className="text-yellow-700 bg-yellow-50 p-2 rounded">Warnings: {result.diagnostics.warnings.join("; ")}</div>}
                  {result.diagnostics.errors?.length > 0 && <div className="text-red-700 bg-red-50 p-2 rounded mt-2">Errors: {result.diagnostics.errors.join("; ")}</div>}
                </div>
              )}

            </div>

            {/* Footer: quick actions */}
            <div className="mt-4">
              <div className="flex gap-2">
                <button onClick={() => {
                  // quick follow-up: run distinct counts
                  setInput("show distinct count for employees table");
                }} className="text-sm px-3 py-2 border rounded">Run Distinct Count</button>
                <button onClick={() => {
                  if (result?.metadata?.sql) {
                    navigator.clipboard?.writeText(result.metadata.sql);
                  }
                }} className="text-sm px-3 py-2 border rounded">Copy Generated SQL</button>
                <button onClick={clearConversation} className="text-sm px-3 py-2 border rounded">Clear</button>
              </div>
            </div>
          </aside>
        </div>
      </main>
    </div>
  );
}
