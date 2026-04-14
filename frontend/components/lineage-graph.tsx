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

export function LineageGraph({ nodes, edges, targetColId }: Props) {
  const rfNodes: Node[] = useMemo(() =>
    nodes.map((n, i) => {
      const [table, col] = n.id.split(".");
      return {
        id: n.id,
        position: { x: (i % 4) * 220, y: Math.floor(i / 4) * 100 },
        data: { label: `${table}\n${col}` },
        style: {
          background: n.id === targetColId ? "#1e3a5f" : "#1a2233",
          color: n.id === targetColId ? "#7ec8e3" : "#a0b4c8",
          border: n.id === targetColId ? "2px solid #7ec8e3" : "1px solid #3d4f6b",
          borderRadius: 6,
          fontSize: 11,
          padding: "6px 10px",
          whiteSpace: "pre" as const,
        },
      };
    }), [nodes, targetColId]);

  const rfEdges: Edge[] = useMemo(() =>
    edges.map((e, i) => ({
      id: `e-${i}`,
      source: e.source_col,
      target: e.target_col,
      label: e.transform_type,
      style: { stroke: TRANSFORM_COLOURS[e.transform_type] ?? "#888" },
      labelStyle: { fontSize: 9, fill: "#888" },
    })), [edges]);

  return (
    <div style={{ height: 400, background: "#0a0f1a", borderRadius: 8 }}>
      <ReactFlow nodes={rfNodes} edges={rfEdges} fitView>
        <Background color="#1a2233" />
        <Controls />
        <MiniMap nodeColor={() => "#1a2233"} />
      </ReactFlow>
    </div>
  );
}
