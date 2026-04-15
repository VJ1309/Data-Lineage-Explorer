"use client";
import { useMemo, useState } from "react";
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

function splitColId(id: string): [string, string] {
  const dot = id.lastIndexOf(".");
  if (dot === -1) return [id, ""];
  return [id.slice(0, dot), id.slice(dot + 1)];
}

function layeredLayout(
  nodeIds: string[],
  edgePairs: { source: string; target: string }[],
): Map<string, { x: number; y: number }> {
  const successors = new Map<string, string[]>();
  const predecessors = new Map<string, string[]>();
  const nodeSet = new Set(nodeIds);

  for (const e of edgePairs) {
    if (!nodeSet.has(e.source) || !nodeSet.has(e.target)) continue;
    if (!successors.has(e.source)) successors.set(e.source, []);
    successors.get(e.source)!.push(e.target);
    if (!predecessors.has(e.target)) predecessors.set(e.target, []);
    predecessors.get(e.target)!.push(e.source);
  }

  const depth = new Map<string, number>();
  const roots = nodeIds.filter((n) => !predecessors.has(n) || predecessors.get(n)!.length === 0);
  for (const n of nodeIds) depth.set(n, 0);
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

  const layers = new Map<number, string[]>();
  for (const [node, d] of depth) {
    if (!layers.has(d)) layers.set(d, []);
    layers.get(d)!.push(node);
  }

  const COL_WIDTH = 280;
  const ROW_HEIGHT = 80;
  const positions = new Map<string, { x: number; y: number }>();
  const sortedLayers = [...layers.keys()].sort((a, b) => a - b);
  for (const layer of sortedLayers) {
    const nodesInLayer = layers.get(layer)!;
    nodesInLayer.sort();
    const layerHeight = nodesInLayer.length * ROW_HEIGHT;
    const startY = -layerHeight / 2;
    nodesInLayer.forEach((n, i) => {
      positions.set(n, { x: layer * COL_WIDTH, y: startY + i * ROW_HEIGHT });
    });
  }
  return positions;
}

function toTableLevel(
  colNodeIds: string[],
  colEdges: LineageEdge[],
): { nodes: { id: string }[]; edges: { source: string; target: string; count: number; types: Set<string> }[] } {
  const tables = new Set<string>();
  for (const id of colNodeIds) {
    const [table] = splitColId(id);
    tables.add(table);
  }

  const edgeMap = new Map<string, { count: number; types: Set<string> }>();
  for (const e of colEdges) {
    const [srcTable] = splitColId(e.source_col);
    const [tgtTable] = splitColId(e.target_col);
    if (srcTable === tgtTable) continue;
    const key = `${srcTable}||${tgtTable}`;
    if (!edgeMap.has(key)) edgeMap.set(key, { count: 0, types: new Set() });
    const entry = edgeMap.get(key)!;
    entry.count++;
    entry.types.add(e.transform_type);
  }

  return {
    nodes: [...tables].map((id) => ({ id })),
    edges: [...edgeMap.entries()].map(([key, val]) => {
      const [source, target] = key.split("||");
      return { source, target, ...val };
    }),
  };
}

export function LineageGraph({ nodes, edges, targetColId }: Props) {
  const [collapsed, setCollapsed] = useState(false);
  const [targetTable] = splitColId(targetColId);

  const colPositions = useMemo(
    () => layeredLayout(nodes.map((n) => n.id), edges.map((e) => ({ source: e.source_col, target: e.target_col }))),
    [nodes, edges],
  );

  const tableLevel = useMemo(() => toTableLevel(nodes.map((n) => n.id), edges), [nodes, edges]);
  const tablePositions = useMemo(
    () => layeredLayout(tableLevel.nodes.map((n) => n.id), tableLevel.edges),
    [tableLevel],
  );

  const rfNodes: Node[] = useMemo(() => {
    if (!collapsed) {
      return nodes.map((n) => {
        const [table, col] = splitColId(n.id);
        const pos = colPositions.get(n.id) ?? { x: 0, y: 0 };
        const isTarget = n.id === targetColId;
        const hasIncoming = edges.some((e) => e.target_col === n.id);
        const hasOutgoing = edges.some((e) => e.source_col === n.id);
        const isSource = !hasIncoming && hasOutgoing;
        const isSink = hasIncoming && !hasOutgoing;

        let bg = "#1a2233", border = "1px solid #3d4f6b", color = "#a0b4c8";
        if (isTarget) { bg = "#1e3a5f"; border = "2px solid #7ec8e3"; color = "#7ec8e3"; }
        else if (isSource) { bg = "#1a2a1a"; border = "1px solid #4ade80"; color = "#86efac"; }
        else if (isSink) { bg = "#2a1a2a"; border = "1px solid #c084fc"; color = "#d8b4fe"; }

        return {
          id: n.id,
          position: pos,
          data: { label: `${table}\n${col}` },
          style: { background: bg, color, border, borderRadius: 6, fontSize: 11, padding: "6px 10px", whiteSpace: "pre" as const, minWidth: 120, textAlign: "center" as const },
        };
      });
    }

    return tableLevel.nodes.map((n) => {
      const pos = tablePositions.get(n.id) ?? { x: 0, y: 0 };
      const isTarget = n.id === targetTable;
      const hasIncoming = tableLevel.edges.some((e) => e.target === n.id);
      const hasOutgoing = tableLevel.edges.some((e) => e.source === n.id);
      const isSource = !hasIncoming && hasOutgoing;
      const isSink = hasIncoming && !hasOutgoing;

      let bg = "#1a2233", border = "1px solid #3d4f6b", color = "#a0b4c8";
      if (isTarget) { bg = "#1e3a5f"; border = "2px solid #7ec8e3"; color = "#7ec8e3"; }
      else if (isSource) { bg = "#1a2a1a"; border = "1px solid #4ade80"; color = "#86efac"; }
      else if (isSink) { bg = "#2a1a2a"; border = "1px solid #c084fc"; color = "#d8b4fe"; }

      const colCount = nodes.filter((cn) => splitColId(cn.id)[0] === n.id).length;

      return {
        id: n.id,
        position: pos,
        data: { label: `${n.id}\n${colCount} column${colCount !== 1 ? "s" : ""}` },
        style: { background: bg, color, border, borderRadius: 6, fontSize: 11, padding: "8px 14px", whiteSpace: "pre" as const, minWidth: 140, textAlign: "center" as const },
      };
    });
  }, [collapsed, nodes, edges, targetColId, targetTable, colPositions, tableLevel, tablePositions]);

  const rfEdges: Edge[] = useMemo(() => {
    if (!collapsed) {
      return edges.map((e, i) => ({
        id: `e-${i}`,
        source: e.source_col,
        target: e.target_col,
        label: e.transform_type,
        animated: e.transform_type === "aggregation" || e.transform_type === "window",
        style: { stroke: TRANSFORM_COLOURS[e.transform_type] ?? "#888", strokeWidth: 1.5 },
        labelStyle: { fontSize: 9, fill: "#888" },
        labelBgStyle: { fill: "#0a0f1a", fillOpacity: 0.8 },
      }));
    }

    return tableLevel.edges.map((e, i) => {
      const dominantType = [...e.types][0] ?? "passthrough";
      const label = e.count === 1 ? dominantType : `${e.count} edges`;
      return {
        id: `te-${i}`,
        source: e.source,
        target: e.target,
        label,
        style: { stroke: TRANSFORM_COLOURS[dominantType] ?? "#888", strokeWidth: 2 },
        labelStyle: { fontSize: 9, fill: "#888" },
        labelBgStyle: { fill: "#0a0f1a", fillOpacity: 0.8 },
      };
    });
  }, [collapsed, edges, tableLevel]);

  return (
    <div>
      <div className="flex justify-end mb-2">
        <button
          onClick={() => setCollapsed((v) => !v)}
          className={`text-xs px-3 py-1 rounded border transition-colors ${
            collapsed
              ? "bg-accent text-accent-foreground border-accent"
              : "text-muted-foreground border-border hover:text-foreground"
          }`}
        >
          {collapsed ? "⊞ Expand columns" : "⊟ Group by table"}
        </button>
      </div>
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
              if (collapsed) {
                if (n.id === targetTable) return "#7ec8e3";
                const hasIn = tableLevel.edges.some((e) => e.target === n.id);
                const hasOut = tableLevel.edges.some((e) => e.source === n.id);
                if (!hasIn && hasOut) return "#4ade80";
                if (hasIn && !hasOut) return "#c084fc";
                return "#3d4f6b";
              }
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
    </div>
  );
}
