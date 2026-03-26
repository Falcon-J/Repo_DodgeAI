import { useEffect, useRef, useState } from "react";
import Header from "./components/Header";
import GraphSection from "./components/GraphSection";
import ChatPanel from "./components/ChatPanel";
import { describeApiTarget, fetchJson } from "./lib/api";

const introMessage = {
  id: "intro",
  role: "assistant",
  text: "I work best with context-rich O2C questions: inspect top billed products, trace a billing document, or spot broken sales-order handoffs.",
  intent: "welcome",
  rows: []
};

function mapGraphResponse(response) {
  const rawLinks = response.edges || response.links || [];
  return {
    nodes: (response.nodes || []).map((node) => ({ ...node })),
    links: rawLinks.map((link) => ({
      source: normalizeNodeId(link.source),
      target: normalizeNodeId(link.target),
      type: link.type,
    }))
  };
}

function normalizeNodeId(value) {
  if (value && typeof value === "object") {
    return value.id || "";
  }
  return String(value || "");
}

function linkKey(link) {
  return `${normalizeNodeId(link.source)}|${link.type}|${normalizeNodeId(link.target)}`;
}

function mergeGraphData(current, incoming) {
  const nextNodes = new Map(current.nodes.map((node) => [node.id, node]));
  const nextLinks = new Map(
    current.links.map((link) => [linkKey(link), {
      source: normalizeNodeId(link.source),
      target: normalizeNodeId(link.target),
      type: link.type,
    }])
  );

  for (const node of incoming.nodes || []) {
    nextNodes.set(node.id, node);
  }

  for (const link of incoming.links || []) {
    const normalizedLink = {
      source: normalizeNodeId(link.source),
      target: normalizeNodeId(link.target),
      type: link.type,
    };
    nextLinks.set(linkKey(normalizedLink), normalizedLink);
  }

  return {
    nodes: Array.from(nextNodes.values()),
    links: Array.from(nextLinks.values())
  };
}

export default function App() {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [overviewGraph, setOverviewGraph] = useState({ nodes: [], links: [] });
  const [messages, setMessages] = useState([introMessage]);
  const [input, setInput] = useState("");
  const [selectedNode, setSelectedNode] = useState(null);
  const [nodeDetails, setNodeDetails] = useState(null);
  const [highlightedNodes, setHighlightedNodes] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [showGranularOverlay, setShowGranularOverlay] = useState(true);
  const [isGraphMinimized, setIsGraphMinimized] = useState(false);
  const [fitRequestId, setFitRequestId] = useState(0);
  const nodeRequestRef = useRef(0);
  const [backendStatus, setBackendStatus] = useState({
    tone: "neutral",
    text: `Checking backend at ${describeApiTarget()}`
  });

  useEffect(() => {
    async function loadGraph() {
      try {
        await fetchJson("/health");
        setBackendStatus({
          tone: "success",
          text: `Connected to backend at ${describeApiTarget()}`
        });

        const response = await fetchJson("/graph");
        const mapped = mapGraphResponse(response);
        setOverviewGraph(mapped);
        setGraphData(mapped);
      } catch (loadError) {
        setBackendStatus({
          tone: "error",
          text: `Backend unavailable at ${describeApiTarget()}`
        });
        setError(loadError.message);
      }
    }

    loadGraph();
  }, []);

  async function loadNodeAndNeighborhood(nodeId, { merge = true } = {}) {
    const requestId = ++nodeRequestRef.current;
    const [details, neighborhood] = await Promise.all([
      fetchJson(`/node/${encodeURIComponent(nodeId)}`),
      fetchJson(`/graph/neighborhood?node_id=${encodeURIComponent(nodeId)}&depth=1`)
    ]);
    if (requestId !== nodeRequestRef.current) {
      return;
    }
    setSelectedNode(nodeId);
    setNodeDetails(details);
    if (merge) {
      setGraphData((current) => mergeGraphData(current, mapGraphResponse(neighborhood)));
    } else {
      setGraphData(mapGraphResponse(neighborhood));
    }
  }

  async function loadTrace(nodeId) {
    const requestId = ++nodeRequestRef.current;
    const [details, trace] = await Promise.all([
      fetchJson(`/node/${encodeURIComponent(nodeId)}`),
      fetchJson(`/trace?node_id=${encodeURIComponent(nodeId)}`)
    ]);
    if (requestId !== nodeRequestRef.current) {
      return;
    }
    setSelectedNode(nodeId);
    setNodeDetails(details);
    setGraphData(mapGraphResponse(trace));
    setFitRequestId((current) => current + 1);
  }

  async function handleNodeClick(node) {
    try {
      setError("");
      await loadNodeAndNeighborhood(node.id);
    } catch (nodeError) {
      setError(nodeError.message);
    }
  }

  async function handleChatSubmit(event) {
    event.preventDefault();
    const question = input.trim();
    if (!question || loading) {
      return;
    }

    setError("");
    setLoading(true);
    setInput("");
    setMessages((current) => [
      ...current,
      { id: `${Date.now()}-user`, role: "user", text: question }
    ]);

    try {
      const response = await fetchJson("/chat", {
        method: "POST",
        body: JSON.stringify({ question })
      });

      setMessages((current) => [
        ...current,
        {
          id: `${Date.now()}-assistant`,
          role: "assistant",
          text: response.answer,
          intent: response.intent,
          rows: response.rows || [],
          sql: response.sql || ""
        }
      ]);

      const nextHighlightedNodes = response.nodes_to_highlight || [];
      setHighlightedNodes(nextHighlightedNodes);

      try {
        if (response.trace_node_id) {
          await loadTrace(response.trace_node_id);
          setIsGraphMinimized(false);
        } else if (nextHighlightedNodes.length) {
          const focusNode = pickFocusNode(nextHighlightedNodes);
          await loadNodeAndNeighborhood(focusNode, { merge: false });
          setIsGraphMinimized(false);
        } else {
          setSelectedNode(null);
          setNodeDetails(null);
        }
      } catch (graphFocusError) {
        setError(graphFocusError.message || "Response returned, but graph focus failed.");
      }
    } catch (chatError) {
      setError(chatError.message);
    } finally {
      setLoading(false);
    }
  }

  function pickFocusNode(nodeIds) {
    const priority = [
      "billing_document:",
      "billing_item:",
      "sales_order:",
      "sales_order_item:",
      "delivery:",
      "delivery_item:",
      "payment:",
      "journal_entry:",
      "customer:",
      "product:",
      "plant:",
      "address:"
    ];
    for (const prefix of priority) {
      const match = nodeIds.find((nodeId) => nodeId.startsWith(prefix));
      if (match) {
        return match;
      }
    }
    return nodeIds[0];
  }

  function handleToggleMinimizeGraph() {
    setIsGraphMinimized((current) => !current);
  }

  function handleResetGraphView() {
    setGraphData(overviewGraph);
    setSelectedNode(null);
    setNodeDetails(null);
    setFitRequestId((current) => current + 1);
  }

  function handleToggleGranularOverlay() {
    setShowGranularOverlay((current) => !current);
  }

  return (
    <div className="app">
      <Header />
      <main className={`main-layout ${isGraphMinimized ? "main-layout-graph-minimized" : ""}`}>
        <GraphSection
          graphData={graphData}
          fitRequestId={fitRequestId}
          isGraphMinimized={isGraphMinimized}
          selectedNode={selectedNode}
          nodeDetails={nodeDetails}
          highlightedNodes={highlightedNodes}
          showGranularOverlay={showGranularOverlay}
          onNodeClick={handleNodeClick}
          onToggleMinimizeGraph={handleToggleMinimizeGraph}
          onResetGraphView={handleResetGraphView}
          onToggleGranularOverlay={handleToggleGranularOverlay}
        />
        <ChatPanel
          backendStatus={backendStatus}
          messages={messages}
          input={input}
          loading={loading}
          error={error}
          onInputChange={setInput}
          onSubmit={handleChatSubmit}
        />
      </main>
    </div>
  );
}
