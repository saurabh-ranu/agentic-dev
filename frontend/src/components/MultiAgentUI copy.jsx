import React, { useState, useEffect, useRef } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer,
} from "recharts";
import { Play, Activity, Zap, CheckCircle, MessageSquare, Code } from "lucide-react";

const API_URL = "http://localhost:8000/run_agent";
const STORAGE_KEY = "datasure_conversational_state_v2";

export default function MultiAgentUI() {
  const [selectedAgent, setSelectedAgent] = useState("profiling");
  const [source, setSource] = useState("");
  const [target, setTarget] = useState("");
  const [condition, setCondition] = useState("");
  const [mode, setMode] = useState("nl");
  const [messages, setMessages] = useState([
    { role: "assistant", text: "Hi — I’m your Profiling agent. How can I help?" },
  ]);
  const [input, setInput] = useState("");
  const [runStatus, setRunStatus] = useState("idle");
  const [result, setResult] = useState(null);
  const [sessionId, setSessionId] = useState(() => crypto.randomUUID());
  const messagesEndRef = useRef(null);

  // Load saved chat from localStorage
  useEffect(() => {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      const state = JSON.parse(saved);
      setMessages(state.messages || []);
      setSelectedAgent(state.selectedAgent || "profiling");
      setSessionId(state.sessionId || crypto.randomUUID());
      setResult(state.result || null);
      setSource(state.source || "");
      setTarget(state.target || "");
    }
  }, []);

  // Persist session
  useEffect(() => {
    const toSave = { messages, selectedAgent, result, source, target, sessionId };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(toSave));
  }, [messages, selectedAgent, result, source, target, sessionId]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  function addMessage(role, text) {
    setMessages((prev) => [...prev, { role, text }]);
  }

  async function handleSend() {
    if (!input.trim()) return;
    const userText = input;
    setInput("");
    addMessage("user", userText);
    setRunStatus("running");

    try {
      const response = await fetch(API_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          session_id: sessionId,
          agent: selectedAgent,
          mode,
          userText,
          context: { source, target, condition },
        }),
      });

      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();

      addMessage("assistant", data.message || "Got it!");
      if (data.next_prompt) addMessage("assistant", data.next_prompt);
      if (data.payload) setResult(data.payload);

      setRunStatus("success");
    } catch (err) {
      console.error("Error:", err);
      setRunStatus("error");
      addMessage("assistant", `Error: ${err.message}`);
    }
  }

  function handleAgentSwitch(agent) {
    setSelectedAgent(agent);
    addMessage("assistant", `Switched to ${agent} agent. What would you like to do?`);
  }

  // --- Rendering helpers ---
  function renderVisualization() {
    if (!result?.chart_type) return <div className="text-gray-400">No visualization yet.</div>;
    const type = result.chart_type.toLowerCase();
    const data = result.chart_data || [];

    if (type === "bar" || type === "histogram") {
      const xKey = Object.keys(data[0] || {})[0];
      const yKey = Object.keys(data[0] || {})[1];
      return (
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={xKey} />
            <YAxis />
            <Tooltip />
            <Bar dataKey={yKey} fill="#2563eb" />
          </BarChart>
        </ResponsiveContainer>
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
                <tr key={i}>
                  {cols.map((c) => (
                    <td key={c} className="px-2 py-1 border">{row[c]?.toString()}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
    }

    return <div className="text-gray-500 italic">Unsupported visualization type.</div>;
  }

  return (
    <div className="h-screen flex bg-gray-50">
      {/* Sidebar */}
      <aside className="w-80 bg-white border-r p-4 flex flex-col">
        <h1 className="text-xl font-semibold mb-4">DataSure — Agents</h1>
        <div className="space-y-2">
          <button
            onClick={() => handleAgentSwitch("profiling")}
            className={`w-full text-left p-3 rounded-lg flex items-center gap-3 ${
              selectedAgent === "profiling" ? "bg-blue-50 border border-blue-200" : "hover:bg-gray-50"
            }`}
          >
            <Activity size={18} />
            <div>
              <div className="font-medium">Profiling Agent</div>
              <div className="text-xs text-gray-500">Profile tables, stats, and insights</div>
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
              <div className="text-xs text-gray-500">Compare schemas & query diffs</div>
            </div>
          </button>
        </div>
        <div className="mt-auto text-xs text-gray-400">
          Session: <span className="font-medium">{sessionId.slice(0, 8)}</span>
        </div>
      </aside>

      {/* Main Area */}
      <main className="flex-1 p-6 flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-2xl font-semibold capitalize">
            {selectedAgent} — Conversational
          </h2>
          <div className="flex items-center gap-2">
            {runStatus === "running" && (
              <span className="text-blue-600 flex items-center gap-1"><Play className="animate-pulse" size={14}/>Running</span>
            )}
            {runStatus === "success" && (
              <span className="text-green-600 flex items-center gap-1"><CheckCircle size={14}/>Success</span>
            )}
            {runStatus === "error" && (
              <span className="text-red-600 flex items-center gap-1">✖ Error</span>
            )}
          </div>
        </div>

        {/* Top Inputs */}
        <div className="bg-white p-4 rounded shadow-sm mb-4 grid grid-cols-4 gap-3">
          <input
            className="border p-2 rounded"
            placeholder="Source (e.g., mssql-prod)"
            value={source}
            onChange={(e) => setSource(e.target.value)}
          />
          <input
            className="border p-2 rounded"
            placeholder="Target (optional)"
            value={target}
            onChange={(e) => setTarget(e.target.value)}
          />
          <input
            className="border p-2 rounded"
            placeholder="Condition (e.g., age > 30)"
            value={condition}
            onChange={(e) => setCondition(e.target.value)}
          />
          <div className="flex gap-2">
            <button onClick={() => setMode("nl")} className={`px-3 py-2 rounded border ${mode==="nl"?"bg-blue-50 border-blue-300":""}`}>
              <MessageSquare size={14}/> NL
            </button>
            <button onClick={() => setMode("sql")} className={`px-3 py-2 rounded border ${mode==="sql"?"bg-blue-50 border-blue-300":""}`}>
              <Code size={14}/> SQL
            </button>
          </div>
        </div>

        {/* Chat + Results */}
        <div className="flex-1 grid grid-cols-3 gap-4">
          {/* Chat */}
          <section className="col-span-2 bg-white p-4 rounded shadow-sm flex flex-col">
            <div className="flex-1 overflow-auto mb-3 bg-gray-50 p-2 rounded">
              {messages.map((m, i) => (
                <div key={i} className={`mb-2 flex ${m.role==="user"?"justify-end":"justify-start"}`}>
                  <div className={`${m.role==="user"?"bg-blue-600 text-white":"bg-white border"} p-3 rounded-lg max-w-[75%]`}>
                    {m.text}
                  </div>
                </div>
              ))}
              <div ref={messagesEndRef}/>
            </div>
            <div className="border-t pt-3 flex gap-2">
              <textarea
                className="flex-1 border p-2 rounded"
                rows={2}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder={mode==="nl"?"Ask me something (e.g., profile employee table)":"Write SQL manually"}
              />
              <button
                onClick={handleSend}
                className="bg-blue-600 text-white px-4 py-2 rounded flex items-center gap-2"
              >
                <Play size={14}/> Send
              </button>
            </div>
          </section>

          {/* Results */}
          <aside className="col-span-1 bg-white p-4 rounded shadow-sm flex flex-col">
            <h3 className="font-medium mb-2">Result & Insights</h3>
            {result ? (
              <>
                {result.metadata && (
                  <div className="text-sm mb-2 text-gray-600">
                    Rows: {result.metadata.rows || "-"} | Columns: {result.metadata.columns || "-"}
                  </div>
                )}
                {renderVisualization()}
              </>
            ) : (
              <div className="text-gray-400 text-sm">No results yet. Ask a question to start profiling.</div>
            )}
          </aside>
        </div>
      </main>
    </div>
  );
}
