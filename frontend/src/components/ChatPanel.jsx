const SHOW_DEBUG_DETAILS = String(import.meta.env.VITE_SHOW_DEBUG_DETAILS || "").toLowerCase() === "true";

function formatEvidenceRow(row) {
  return Object.entries(row)
    .filter(([, value]) => value !== null && value !== "")
    .slice(0, 4)
    .map(([key, value]) => `${key}: ${value}`)
    .join(" | ");
}

export default function ChatPanel({
  backendStatus,
  messages,
  input,
  loading,
  error,
  onInputChange,
  onSubmit
}) {
  return (
    <aside className="chat-panel">
      <div className="chat-header">
        <h2>Chat with Graph</h2>
        <p>Order to Cash</p>
      </div>

      <div className="message-list">
        {messages.map((message) => (
          <div
            key={message.id}
            className={`message-row ${message.role === "user" ? "message-row-user" : "message-row-assistant"}`}
          >
            {message.role === "assistant" ? (
              <div className="message-avatar">D</div>
            ) : null}
            <div className="message-content">
              <div className="message-name">{message.role === "assistant" ? "Dodge AI" : "You"}</div>
              {message.role === "assistant" ? (
                <div className="message-role">Graph Agent</div>
              ) : null}
              <div className={`message-bubble ${message.role === "user" ? "message-bubble-user" : ""}`}>
                {message.text}
              </div>
              {SHOW_DEBUG_DETAILS && message.role === "assistant" && message.rows?.length ? (
                <div className="message-evidence">
                  {message.rows.slice(0, 2).map((row, index) => (
                    <div key={`${message.id}-row-${index}`} className="evidence-row">
                      {formatEvidenceRow(row)}
                    </div>
                  ))}
                </div>
              ) : null}
              {SHOW_DEBUG_DETAILS && message.role === "assistant" && message.sql ? (
                <details className="sql-details">
                  <summary>SQL used</summary>
                  <pre>{message.sql}</pre>
                </details>
              ) : null}
            </div>
          </div>
        ))}
      </div>

      <div className="chat-input-area">
        <div className={`chat-status chat-status-${backendStatus?.tone || "neutral"}`}>
          <span className="status-dot" />
          {backendStatus?.text || "Checking backend connection"}
        </div>
        {error ? <div className="chat-error">{error}</div> : null}
        <form className="chat-form" onSubmit={onSubmit}>
          <textarea
            value={input}
            onChange={(event) => onInputChange(event.target.value)}
            placeholder="Try: Trace billing document 90504298"
            rows={4}
          />
          <button type="submit" disabled={loading}>
            {loading ? "Analyzing..." : "Send"}
          </button>
        </form>
      </div>
    </aside>
  );
}
