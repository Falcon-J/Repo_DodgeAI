import { useEffect, useRef, useState } from "react";
import ForceGraph2D from "react-force-graph-2d";
import NodeDetailsCard from "./NodeDetailsCard";

const NODE_COLORS = {
  sales_order: "#1d4ed8",
  sales_order_item: "#3b82f6",
  delivery: "#0f766e",
  delivery_item: "#14b8a6",
  billing_document: "#ea580c",
  billing_item: "#fb923c",
  journal_entry: "#7c3aed",
  payment: "#9333ea",
  customer: "#be123c",
  product: "#059669",
  address: "#64748b",
  plant: "#0891b2",
};

export default function GraphSection({
  graphData,
  fitRequestId,
  isGraphMinimized,
  selectedNode,
  nodeDetails,
  highlightedNodes,
  showGranularOverlay,
  onNodeClick,
  onToggleMinimizeGraph,
  onResetGraphView,
  onToggleGranularOverlay,
}) {
  const containerRef = useRef(null);
  const graphRef = useRef(null);
  const [size, setSize] = useState({ width: 0, height: 0 });
  const [hoveredNodeId, setHoveredNodeId] = useState(null);

  useEffect(() => {
    function updateSize() {
      if (!containerRef.current) {
        return;
      }

      setSize({
        width: containerRef.current.clientWidth,
        height: containerRef.current.clientHeight,
      });
    }

    updateSize();
    window.addEventListener("resize", updateSize);
    return () => window.removeEventListener("resize", updateSize);
  }, []);

  useEffect(() => {
    if (!graphRef.current || !graphData.nodes.length || isGraphMinimized) {
      return;
    }

    const timer = window.setTimeout(() => {
      graphRef.current.zoomToFit(500, 80);
    }, 180);

    return () => window.clearTimeout(timer);
  }, [fitRequestId, isGraphMinimized]);

  function getNodeColor(node) {
    if (highlightedNodes.includes(node.id)) {
      return "#dc2626";
    }
    return NODE_COLORS[node.type] || "#475569";
  }

  return (
    <section className={`graph-section ${isGraphMinimized ? "graph-section-minimized" : ""}`}>
      <div className="graph-toolbar">
        <button type="button" onClick={onToggleMinimizeGraph}>
          {isGraphMinimized ? "Maximize" : "Minimize"}
        </button>
        <button
          type="button"
          onClick={onResetGraphView}
        >
          Reset View
        </button>
        <button
          type="button"
          className="dark-button"
          onClick={onToggleGranularOverlay}
          disabled={isGraphMinimized}
        >
          {showGranularOverlay ? "Hide Granular Overlay" : "Show Granular Overlay"}
        </button>
      </div>

      {isGraphMinimized ? (
        <div className="graph-minimized-note">
          Graph is minimized. Click <strong>Maximize</strong> to continue exploration.
        </div>
      ) : (
        <div className="graph-canvas" ref={containerRef}>
        <ForceGraph2D
          ref={graphRef}
          graphData={graphData}
          width={size.width}
          height={size.height}
          nodeLabel={(node) => `${node.label || node.id}\n${node.type}`}
          linkColor={(link) =>
            highlightedNodes.includes(link.source.id || link.source) &&
            highlightedNodes.includes(link.target.id || link.target)
              ? "#fb7185"
              : showGranularOverlay
                ? "#d8e2f7"
                : "rgba(216, 226, 247, 0.10)"
          }
          linkWidth={(link) =>
            highlightedNodes.includes(link.source.id || link.source) &&
            highlightedNodes.includes(link.target.id || link.target)
              ? 2.2
              : showGranularOverlay
                ? 1
                : 0.15
          }
          nodeCanvasObject={(node, ctx, globalScale) => {
            const isHighlighted = highlightedNodes.includes(node.id);
            const isSelected = selectedNode === node.id;
            const isHovered = hoveredNodeId === node.id;
            const radius = isSelected ? 8 : isHighlighted ? 6.4 : 5;
            const fontSize = 11 / globalScale;

            ctx.beginPath();
            ctx.arc(node.x, node.y, radius, 0, 2 * Math.PI, false);
            ctx.fillStyle = getNodeColor(node);
            ctx.fill();

            if (isSelected || isHighlighted) {
              ctx.beginPath();
              ctx.arc(node.x, node.y, radius + 3, 0, 2 * Math.PI, false);
              ctx.strokeStyle = isSelected ? "#0f172a" : "#fca5a5";
              ctx.lineWidth = 1.2;
              ctx.stroke();
            }

            if (isHovered) {
              ctx.font = `${fontSize}px Georgia`;
              ctx.fillStyle = "#0f172a";
              ctx.fillText(
                node.label || node.id,
                node.x + radius + 4,
                node.y - radius,
              );
            }
          }}
          // onNodeHover={(node) => setHoveredNodeId(node?.id || null)}
          onNodeClick={onNodeClick}
        />
      </div>
      )}

      {!isGraphMinimized ? <NodeDetailsCard nodeDetails={nodeDetails} /> : null}
    </section>
  );
}
