"use client";
import { useMemo } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type Node,
  type Edge,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { LineageEdge } from "@/lib/api";

const TRANSFORM_COLOURS: Record<string, string> = {
  passthrough: "#4ade80",
  aggregation: "#fbbf24",
  expression:  "#a78bfa",
  join_key:    "#60a5fa",
  window:      "#818cf8",
  cast:        "#94a3b8",
  filter:      "#f87171",
};

type Props = {
  nodes: { id: string }[];
  edges: LineageEdge[];
  targetColId: string;
};

/** Split a column id like "catalog.schema.table.col" into [table, col] */
function splitColId(id: string): [string, string] {
  const dot = id.lastIndexOf(".");
  if (dot === -1) return [id, ""];
  return [id.slice(0, dot), id.slice(dot + 1)];
}

/**
 * Assign nodes to layers using topological depth (longest-path from roots).
 * This creates a clear left-to-right flow: sources → intermediates → targets.
 */
function layeredLayout(
  nodeIds: string[],
  edges: LineageEdge[],
  targetColId: string,
): Map<string, { x: number; y: number }> {
  // Build adjacency for depth calculation
  const successors = new Map<string, string[]>();
  const predecessors = new Map<string, string[]>();
  const nodeSet = new Set(nodeIds);

  for (const e of edges) {
    if (!nodeSet.has(e.source_col) || !nodeSet.has(e.target_col)) continue;
    if (!successors.has(e.source_col)) successors.set(e.source_col, []);
    successors.get(e.source_col)!.push(e.target_col);
    if (!predecessors.has(e.target_col)) predecessors.set(e.target_col, []);
    predecessors.get(e.target_col)!.push(e.source_col);
  }

  // Find roots (no predecessors) and compute longest-path depth via BFS
  const depth = new Map<string, number>();
  const roots = nodeIds.filter((n) => !predecessors.has(n) || predecessors.get(n)!.length === 0);

  // Initialize all nodes
  for (const n of nodeIds) depth.set(n, 0);

  // BFS from roots to assign maximum depth
  const queue = [...roots];
  while (queue.length > 0) {
    const current = queue.shift()!;
    const d = depth.get(current)!;
    for (const succ of successors.get(current) ?? []) {
      if (d + 1 > (depth.get(succ) ?? 0)) {
        depth.set(succ, d + 1);
        queue.push(succ);
      }
    }
  }

  // Group by depth layer
  const layers = new Map<number, string[]>();
  for (const [node, d] of depth) {
    if (!layers.has(d)) layers.set(d, []);
    layers.get(d)!.push(node);
  }

  // Position nodes: x = layer (left-to-right), y = index within layer
  const COL_WIDTH = 280;
  const ROW_HEIGHT = 80;
  const positions = new Map<string, { x: number; y: number }>();

  const sortedLayers = [...layers.keys()].sort((a, b) => a - b);
  for (const layer of sortedLayers) {
    const nodesInLayer = layers.get(layer)!;
    // Sort nodes within layer for consistent ordering
    nodesInLayer.sort();
    const layerHeight = nodesInLayer.length * ROW_HEIGHT;
    const startY = -layerHeight / 2;
    nodesInLayer.forEach((n, i) => {
      positions.set(n, { x: layer * COL_WIDTH, y: startY + i * ROW_HEIGHT });
    });
  }

  return positions;
}

export function LineageGraph({ nodes, edges, targetColId }: Props) {
  const positions = useMemo(
    () => layeredLayout(nodes.map((n) => n.id), edges, targetColId),
    [nodes, edges, targetColId],
  );

  const rfNodes: Node[] = useMemo(() =>
    nodes.map((n) => {
      const [table, col] = splitColId(n.id);
      const pos = positions.get(n.id) ?? { x: 0, y: 0 };
      const isTarget = n.id === targetColId;

      // Determine if this is a source node (no incoming edges) or sink (no outgoing)
      const hasIncoming = edges.some((e) => e.target_col === n.id);
      const hasOutgoing = edges.some((e) => e.source_col === n.id);
      const isSource = !hasIncoming && hasOutgoing;
      const isSink = hasIncoming && !hasOutgoing;

      let bg = "#1a2233";
      let border = "1px solid #3d4f6b";
      let color = "#a0b4c8";
      if (isTarget) {
        bg = "#1e3a5f"; border = "2px solid #7ec8e3"; color = "#7ec8e3";
      } else if (isSource) {
        bg = "#1a2a1a"; border = "1px solid #4ade80"; color = "#86efac";
      } else if (isSink) {
        bg = "#2a1a2a"; border = "1px solid #c084fc"; color = "#d8b4fe";
      }

      return {
        id: n.id,
        position: pos,
        data: { label: `${table}\n${col}` },
        style: {
          background: bg,
          color,
          border,
          borderRadius: 6,
          fontSize: 11,
          padding: "6px 10px",
          whiteSpace: "pre" as const,
          minWidth: 120,
          textAlign: "center" as const,
        },
      };
    }), [nodes, edges, targetColId, positions]);

  const rfEdges: Edge[] = useMemo(() =>
    edges.map((e, i) => ({
      id: `e-${i}`,
      source: e.source_col,
      target: e.target_col,
      label: e.transform_type,
      animated: e.transform_type === "aggregation" || e.transform_type === "window",
      style: { stroke: TRANSFORM_COLOURS[e.transform_type] ?? "#888", strokeWidth: 1.5 },
      labelStyle: { fontSize: 9, fill: "#888" },
      labelBgStyle: { fill: "#0a0f1a", fillOpacity: 0.8 },
    })), [edges]);

  return (
    <div style={{ height: 500, background: "#0a0f1a", borderRadius: 8 }}>
      <ReactFlow
        nodes={rfNodes}
        edges={rfEdges}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.3}
        maxZoom={2}
      >
        <Background color="#1a2233" />
        <Controls />
        <MiniMap
          nodeColor={(n) => {
            if (n.id === targetColId) return "#7ec8e3";
            const hasIn = edges.some((e) => e.target_col === n.id);
            const hasOut = edges.some((e) => e.source_col === n.id);
            if (!hasIn && hasOut) return "#4ade80";
            if (hasIn && !hasOut) return "#c084fc";
            return "#3d4f6b";
          }}
        />
      </ReactFlow>
      {/* Legend */}
      <div className="flex gap-4 px-3 py-1.5 text-xs" style={{ color: "#6b7a8d" }}>
        <span><span style={{ color: "#4ade80" }}>●</span> Source</span>
        <span><span style={{ color: "#7ec8e3" }}>●</span> Selected</span>
        <span><span style={{ color: "#c084fc" }}>●</span> Target</span>
        <span className="ml-auto flex gap-3">
          {Object.entries(TRANSFORM_COLOURS).map(([type, color]) => (
            <span key={type}><span style={{ color }}>—</span> {type}</span>
          ))}
        </span>
      </div>
    </div>
  );
}
