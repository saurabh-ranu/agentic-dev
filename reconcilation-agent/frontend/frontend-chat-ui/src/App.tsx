// ChatComponent.jsx (Minimal Functional Component)

import React, { useState } from 'react';
import axios from 'axios';
import './Chat.css'; // Assume simple CSS for styling

const API_URL = 'http://localhost:8001/api/chat'; 

// Simple Message type for the UI
interface Message {
  type: 'human' | 'ai';
  content: string;
}

function ChatComponent() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  // CRITICAL: This state holds the session ID (memory key)
  const [threadId, setThreadId] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || loading) return;

    setLoading(true);
    const userMessage: Message = { type: 'human', content: input };
    
    // 1. Optimistically update UI
    setMessages(prev => [...prev, userMessage]);
    const currentInput = input;
    setInput('');

    try {
      // 2. API Call: Send new message + existing threadId
      const response = await axios.post(API_URL, {
        user_input: currentInput,
        thread_id: threadId,
      });

      const { response_message, thread_id: newThreadId } = response.data;

      // 3. Update thread ID (if it's the first turn)
      setThreadId(newThreadId);

      // 4. Update messages with agent's response
      const agentMessage: Message = { type: 'ai', content: response_message };
      
      // The current UI state needs to be rebuilt to replace the optimistic input
      // This is a simple fix for optimistic update in a stateless design:
      setMessages(prev => {
        // Find the index of the message we just sent and ensure we don't duplicate
        const historyWithoutOptimisticInput = prev.slice(0, prev.length - 1);
        return [...historyWithoutOptimisticInput, userMessage, agentMessage];
      });
      
    } catch (error) {
      console.error("Agent API Error:", error);
      setMessages(prev => [...prev, { type: 'ai', content: "Sorry, an error occurred. Check the API server." }]);
    } finally {
      setLoading(false);
    }
  };
  
  // Simple CSS for the chat display (you'd need a Chat.css file for this)
  const chatStyle = {
    container: { maxWidth: '600px', margin: '20px auto', border: '1px solid #ccc', borderRadius: '8px', padding: '10px' },
    messages: { height: '300px', overflowY: 'scroll', padding: '10px', borderBottom: '1px solid #eee' },
    inputForm: { display: 'flex', marginTop: '10px' },
    input: { flexGrow: 1, padding: '10px', border: '1px solid #ccc', borderRadius: '4px' },
    button: { padding: '10px', marginLeft: '10px', border: 'none', borderRadius: '4px', cursor: 'pointer', background: '#007bff', color: 'white' },
    message: { padding: '8px', margin: '5px 0', borderRadius: '5px' },
    human: { textAlign: 'right', background: '#e0f7fa' },
    ai: { textAlign: 'left', background: '#f1f1f1' },
  };

  return (
    <div style={chatStyle.container}>
      <h3>Data Comparison Agent</h3>
      <p style={{fontSize: '10px', color: '#666'}}>Thread ID: {threadId || 'New Session'}</p>
      <div style={chatStyle.messages}>
        {messages.map((msg, index) => (
          <div key={index} style={{...chatStyle.message, ...(msg.type === 'human' ? chatStyle.human : chatStyle.ai)}}>
            <strong>{msg.type === 'human' ? 'You' : 'Agent'}:</strong> {msg.content}
          </div>
        ))}
        {loading && <div style={{padding: '8px', fontStyle: 'italic'}}>Agent is processing...</div>}
      </div>
      <form onSubmit={handleSubmit} style={chatStyle.inputForm}>
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="e.g., compare orders and sales"
          disabled={loading}
          style={chatStyle.input}
        />
        <button type="submit" disabled={loading} style={chatStyle.button}>Send</button>
      </form>
    </div>
  );
}

export default ChatComponent;