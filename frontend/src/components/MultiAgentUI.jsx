import React, { useState, useEffect, useRef } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer } from "recharts";
import { Play, Activity, Zap, CheckCircle, MessageSquare, Code } from "lucide-react";

// Conversational Multi-Agent UI - single-file React component (bugfixed)
// - Fixes syntax errors caused by stray/unformatted comments and unterminated strings
// - Session persistence (localStorage)
// - Quick actions for profiling/reconciliation results
// - Mock orchestrator (replace with real endpoints)

const STORAGE_KEY = "datasure_conversational_state_v1";

export default function MultiAgentUI() {
  const [selectedAgent, setSelectedAgent] = useState("profiling");
  const [source, setSource] = useState("");
  const [target, setTarget] = useState("");
  const [condition, setCondition] = useState("");
  const [mode, setMode] = useState("nl"); // 'nl' or 'sql'

  // Chat state
  const [messages, setMessages] = useState([
    { role: "assistant", text: "Hi — I'm your Profiling agent. Tell me what you want to profile (or switch agent)." },
  ]);
  const [input, setInput] = useState("");
  const [runStatus, setRunStatus] = useState("idle");
  const [logs, setLogs] = useState([]);
  const [timeline, setTimeline] = useState([]);
  const [result, setResult] = useState(null);
  const [showSQLPreview, setShowSQLPreview] = useState(false);

  const messagesEndRef = useRef(null);

  // load from localStorage on mount
  useEffect(() => {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      try {
        const state = JSON.parse(saved);
        setSelectedAgent(state.selectedAgent || "profiling");
        setSource(state.source || "");
        setTarget(state.target || "");
        setCondition(state.condition || "");
        setMode(state.mode || "nl");
        setMessages(state.messages || messages);
        setResult(state.result || null);
        setTimeline(state.timeline || timelineSeed());
        setLogs(state.logs || []);
      } catch (e) {
        console.warn("Failed to load state", e);
      }
    } else {
      setTimeline(timelineSeed());
    }
  }, []);

  // persist to localStorage on important state changes
  useEffect(() => {
    const toSave = { selectedAgent, source, target, condition, mode, messages, result, timeline, logs };
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(toSave)); } catch(e) { console.warn('persist failed', e); }
  }, [selectedAgent, source, target, condition, mode, messages, result, timeline, logs]);

  useEffect(() => scrollToBottom(), [messages]);

  function timelineSeed() {
    return [
      { name: "T+0", value: 0 },
      { name: "T+1", value: 12 },
      { name: "T+2", value: 24 },
      { name: "T+3", value: 18 },
      { name: "T+4", value: 30 },
    ];
  }

  function scrollToBottom() {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }

  function appendLog(text) {
    setLogs((l) => [...l, `${new Date().toLocaleTimeString()}: ${text}`]);
  }

  function addMessage(role, text) {
    setMessages((m) => [...m, { role, text }]);
  }

  async function handleSend() {
    if (!input && mode === "nl") return;
    if (mode === "sql" && !input) return;

    const userText = input;
    addMessage("user", userText);
    setInput("");
    setRunStatus("running");
    appendLog(`User sent message (${selectedAgent})`);

    // Build payload to send to orchestrator
    const payload = {
      agent: selectedAgent,
      source,
      target,
      mode,
      userText,
      condition,
    };

    // Indicate assistant is typing
    addMessage("assistant", "...");

    try {
      const reply = await fakeConversationalRun(payload, (stage) => appendLog(stage));
      // replace the last assistant "..." with real content
      setMessages((m) => {
        const copy = [...m];
        const lastIdx = copy.map((x) => x.role).lastIndexOf("assistant");
        if (lastIdx >= 0 && copy[lastIdx].text === "...") copy[lastIdx] = { role: "assistant", text: reply.message };
        else copy.push({ role: "assistant", text: reply.message });
        return copy;
      });

      setResult(reply.payload || null);
      setRunStatus("success");
      appendLog("Agent finished conversational response");
    } catch (err) {
      setRunStatus("error");
      appendLog(`Error: ${err.message || err}`);
      setMessages((m) => [...m, { role: "assistant", text: `Error: ${err.message || err}` }]);
    }
  }

  function handleAgentSwitch(agentName) {
    setSelectedAgent(agentName);
    setMessages([{ role: "assistant", text: `Switched to ${agentName} agent. How can I help?` }]);
    setResult(null);
    setLogs([]);
    setRunStatus("idle");
  }

  // Quick actions based on result payload
  function handleShowColumnStats() {
    if (!result?.metadata) return;
    const cols = result.metadata.column_stats.map(c => `${c.column}: nulls=${c.nulls}, distinct=${c.distinct ?? 'N/A'}`);
    // Use explicit newlines to avoid unterminated string problems
    addMessage('assistant', `Column stats:
${cols.join('')}`);
  }

  function handleShowSampleMismatches() {
    if (!result?.sample_mismatches) return;
    const lines = result.sample_mismatches.map(m => `id=${m.id} | source=${m.source_val} | target=${m.target_val}`);
    addMessage('assistant', `Sample mismatches:
${lines.join('')}`);
  }

  function handleGenerateReconciliationSQL() {
    if (!result?.generated_query) return;
    addMessage('assistant', `Generated reconciliation SQL:
${result.generated_query}`);
    setShowSQLPreview(true);
  }

  function handleClearConversation() {
    setMessages([{ role: "assistant", text: `Switched to ${selectedAgent} agent. How can I help?` }]);
    setResult(null);
    setLogs([]);
    setRunStatus('idle');
    setShowSQLPreview(false);
  }

  return (
    <div className="h-screen flex bg-gray-50">
      {/* Sidebar */}
      <aside className="w-80 bg-white border-r p-4 flex flex-col">
        <h1 className="text-xl font-semibold mb-4">DataSure — Conversational Agents</h1>

        <div className="mb-4">
          <h3 className="text-sm text-gray-500">Agents</h3>
          <div className="mt-2 space-y-2">
            <button
              onClick={() => handleAgentSwitch("profiling")}
              className={`w-full text-left p-3 rounded-lg flex items-center gap-3 ${
                selectedAgent === "profiling" ? "bg-blue-50 border border-blue-200" : "hover:bg-gray-50"
              }`}
            >
              <Activity size={18} />
              <div>
                <div className="font-medium">Profiling Agent</div>
                <div className="text-xs text-gray-500">Ask questions for metadata, stats, suggestions</div>
              </div>
            </button>

            <button
              onClick={() => handleAgentSwitch("reconciliation")}
              className={`w-full text-left p-3 rounded-lg flex items-center gap-3 ${
                selectedAgent === "reconciliation" ? "bg-blue-50 border border-blue-200" : "hover:bg-gray-50"
              }`}
            >
              <Zap size={18} />
              <div>
                <div className="font-medium">Reconciliation Agent</div>
                <div className="text-xs text-gray-500">Chat to generate/compare queries & view diffs</div>
              </div>
            </button>
          </div>
        </div>

        <div className="mt-auto text-xs text-gray-400">
          <div>Session: <span className="font-medium text-gray-700">dev-session-01</span></div>
          <div className="mt-2">Version: <span className="font-medium">0.3.0</span></div>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 p-6 flex flex-col">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-2xl font-semibold">{selectedAgent === "profiling" ? "Profiling" : "Reconciliation"} — Conversational</h2>
            <p className="text-sm text-gray-500 mt-1">Chat naturally or switch to SQL mode to provide an explicit query.</p>
          </div>

          <div className="flex items-center gap-3">
            <div className="text-sm text-gray-600">Status:</div>
            <div className="flex items-center gap-2">
              {runStatus === "idle" && <span className="text-gray-500">Idle</span>}
              {runStatus === "running" && <div className="flex items-center gap-2"><Play className="animate-pulse" size={16} /> <span>Running</span></div>}
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
              <input value={source} onChange={(e) => setSource(e.target.value)} placeholder="e.g., mssql-prod" className="mt-1 block w-full border p-2 rounded" />
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
          </div>
        </div>

        {/* Main content: Chat + right panel */}
        <div className="flex-1 grid grid-cols-3 gap-4">
          {/* Chat area */}
          <section className="col-span-2 bg-white p-4 rounded shadow-sm flex flex-col">
            <div className="flex-1 overflow-auto mb-4 p-2 bg-gray-50 rounded">
              {messages.map((m, i) => (
                <div key={i} className={`mb-3 flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
                  <div className={`${m.role === "user" ? "bg-blue-600 text-white" : "bg-white border"} p-3 rounded-lg max-w-3/4`}>{m.text}</div>
                </div>
              ))}
              <div ref={messagesEndRef} />
            </div>

            <div className="border-t pt-3">
              {mode === "sql" && showSQLPreview && result?.generated_query && (
                <div className="mb-2 text-sm bg-gray-100 p-2 rounded"><strong>Generated SQL:</strong><pre className="whitespace-pre-wrap">{result.generated_query}</pre></div>
              )}

              <div className="flex gap-2">
                <textarea value={input} onChange={(e) => setInput(e.target.value)} placeholder={mode === "nl" ? "Ask: e.g., profile employee table for nulls, distinct counts" : "Paste or write SQL (e.g. SELECT * FROM employees WHERE age > 80)"} className="flex-1 border p-2 rounded" rows={2} />
                <div className="flex flex-col gap-2">
                  <button onClick={handleSend} className="bg-blue-600 text-white px-4 py-2 rounded inline-flex items-center gap-2"><Play size={16}/>Send</button>
                  <button onClick={() => { setInput(''); setShowSQLPreview(false); }} className="px-3 py-2 border rounded">Clear</button>
                </div>
              </div>

              <div className="text-xs text-gray-500 mt-2">Tip: you can type natural language or switch to SQL mode to submit explicit queries. The agent will respond conversationally.</div>
            </div>
          </section>

          {/* Right panel: Results & Logs */}
          <aside className="col-span-1 bg-white p-4 rounded shadow-sm flex flex-col">
            <h3 className="font-medium mb-3">Result & Insights</h3>

            <div className="flex-1 overflow-auto text-sm">
              {result ? (
                <div>
                  <pre className="bg-gray-50 p-2 rounded text-xs overflow-auto">{JSON.stringify(result, null, 2)}</pre>

                  {/* Quick actions based on payload */}
                  <div className="mt-3 flex flex-col gap-2">
                    {result.metadata && <button onClick={handleShowColumnStats} className="text-sm px-3 py-2 border rounded">Show Column Stats</button>}
                    {result.sample_mismatches && <button onClick={handleShowSampleMismatches} className="text-sm px-3 py-2 border rounded">Show Sample Mismatches</button>}
                    {result.generated_query && <button onClick={handleGenerateReconciliationSQL} className="text-sm px-3 py-2 border rounded">Show/Preview Generated SQL</button>}
                    <button onClick={handleClearConversation} className="text-sm px-3 py-2 border rounded">Clear Conversation</button>
                  </div>
                </div>
              ) : (
                <div className="text-gray-400">No results yet. Ask something in the chat and run the agent.</div>
              )}
            </div>

            <div className="mt-4">
              <h4 className="text-sm font-medium mb-2">Execution Logs</h4>
              <div className="h-36 overflow-auto bg-gray-50 p-2 rounded text-xs">
                {logs.length === 0 ? <div className="text-gray-400">No logs yet.</div> : logs.map((l,i) => <div key={i} className="mb-2">{l}</div>)}
              </div>
            </div>

            <div className="mt-4 h-28">
              <ResponsiveContainer width="100%" height={120}>
                <LineChart data={timeline}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="name" />
                  <YAxis />
                  <Tooltip />
                  <Line type="monotone" dataKey="value" strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </div>

          </aside>
        </div>
      </main>
    </div>
  );
}


// -----------------------------
// Mock conversational orchestrator (replace with real API calls)
// - Returns a conversational message and a payload that can include generated SQL, diffs, metadata etc.
// -----------------------------
async function fakeConversationalRun(payload, onUpdate) {
  onUpdate && onUpdate(`Orchestrator received payload for agent=${payload.agent}`);
  await delay(400);

  if (payload.agent === "profiling") {
    onUpdate && onUpdate("Profiling: analyzing schema & sampling data...");
    await delay(700);

    // If user provided SQL in SQL mode, prefer it; otherwise generate sample query
    const generated = payload.mode === "sql" && payload.userText ? payload.userText : `SELECT * FROM employees WHERE ${payload.condition || '1=1'} LIMIT 1000`;

    await delay(500);
    onUpdate && onUpdate("Computed column distributions, null counts, cardinality suggestions");
    await delay(300);

    return {
      message: `I profiled the dataset. I sampled 1000 rows and found 12 columns. Top suggestions: add NOT NULL on employee_id, consider indexing \"age\". Would you like the full column stats or SQL to run?`,
      payload: {
        generated_query: generated,
        metadata: {
          tables: [{ name: "employees", rows: 124234, columns: 12 }],
          column_stats: [
            { column: "employee_id", nulls: 0, distinct: 124234 },
            { column: "age", nulls: 12, mean: 42.1 },
          ],
          suggestions: ["Add NOT NULL on employee_id", "Index candidate: age"],
        },
      },
    };
  }

  if (payload.agent === "reconciliation") {
    onUpdate && onUpdate("Reconciliation: generating source + target queries and diffing sample results...");
    await delay(800);

    const generatedQuery = payload.mode === "sql" && payload.userText ? payload.userText : `SELECT * FROM employees WHERE ${payload.condition || '1=1'}`;

    await delay(500);
    onUpdate && onUpdate("Executed sample queries on source and target (mock)");
    await delay(400);

    return {
      message: `I executed the sample queries. Source returned 1000 rows, Target returned 998 rows. I found 2 mismatches. Shall I show sample mismatches or generate reconciliation SQL?`,
      payload: {
        generated_query: generatedQuery,
        diff_summary: { total_source: 1000, total_target: 998, mismatches: 2 },
        sample_mismatches: [
          { id: 123, source_val: 'A', target_val: 'B' },
          { id: 555, source_val: 'X', target_val: 'Y' },
        ],
      },
    };
  }

  throw new Error('Unknown agent');
}

function delay(ms) { return new Promise((res) => setTimeout(res, ms)); }
