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
  const [showFilters, setShowFilters] = useState(false);
  const [showJoinKeys, setShowJoinKeys] = useState(false);
  const [targetTable] = splitColId(targetColId);

  // Filter & join_key edges emit to pseudo-columns (target.__filter__, target.__joinkey__).
  // Hide those edges AND their pseudo-column nodes unless the user opts in.
  const visibleEdges = useMemo(
    () => edges.filter((e) => {
      if (!showFilters && e.transform_type === "filter") return false;
      if (!showJoinKeys && e.transform_type === "join_key") return false;
      return true;
    }),
    [edges, showFilters, showJoinKeys],
  );

  const visibleNodes = useMemo(() => {
    const hidden = new Set<string>();
    if (!showFilters) {
      for (const n of nodes) if (n.id.endsWith(".__filter__")) hidden.add(n.id);
    }
    if (!showJoinKeys) {
      for (const n of nodes) if (n.id.endsWith(".__joinkey__")) hidden.add(n.id);
    }
    return nodes.filter((n) => !hidden.has(n.id));
  }, [nodes, showFilters, showJoinKeys]);

  const colPositions = useMemo(
    () => layeredLayout(visibleNodes.map((n) => n.id), visibleEdges.map((e) => ({ source: e.source_col, target: e.target_col }))),
    [visibleNodes, visibleEdges],
  );

  const tableLevel = useMemo(() => toTableLevel(visibleNodes.map((n) => n.id), visibleEdges), [visibleNodes, visibleEdges]);
  const tablePositions = useMemo(
    () => layeredLayout(tableLevel.nodes.map((n) => n.id), tableLevel.edges),
    [tableLevel],
  );

  const rfNodes: Node[] = useMemo(() => {
    if (!collapsed) {
      return visibleNodes.map((n) => {
        const [table, col] = splitColId(n.id);
        const pos = colPositions.get(n.id) ?? { x: 0, y: 0 };
        const isTarget = n.id === targetColId;
        const isPseudo = n.id.endsWith(".__filter__") || n.id.endsWith(".__joinkey__");
        const hasIncoming = visibleEdges.some((e) => e.target_col === n.id);
        const hasOutgoing = visibleEdges.some((e) => e.source_col === n.id);
        const isSource = !hasIncoming && hasOutgoing;
        const isSink = hasIncoming && !hasOutgoing;

        let bg = "#1a2233", border = "1px solid #3d4f6b", color = "#a0b4c8";
        if (isTarget) { bg = "#1e3a5f"; border = "2px solid #7ec8e3"; color = "#7ec8e3"; }
        else if (isPseudo) { bg = "#2a2418"; border = "1px dashed #f59e0b"; color = "#fbbf24"; }
        else if (isSource) { bg = "#1a2a1a"; border = "1px solid #4ade80"; color = "#86efac"; }
        else if (isSink) { bg = "#2a1a2a"; border = "1px solid #c084fc"; color = "#d8b4fe"; }

        const displayCol = col === "__filter__" ? "⚑ filter" : col === "__joinkey__" ? "⚷ join_key" : col;

        return {
          id: n.id,
          position: pos,
          data: {
            label: (
              <div style={{ textAlign: "center" }}>
                <div style={{ fontSize: 10, opacity: 0.65, marginBottom: 2, wordBreak: "break-all", lineHeight: 1.3 }}>{table}</div>
                <div style={{ fontWeight: 600, wordBreak: "break-word" }}>{displayCol}</div>
              </div>
            ),
          },
          style: { background: bg, color, border, borderRadius: 6, fontSize: 11, padding: "6px 10px", width: 180, textAlign: "center" as const },
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

      const colCount = visibleNodes.filter((cn) => splitColId(cn.id)[0] === n.id).length;

      return {
        id: n.id,
        position: pos,
        data: {
          label: (
            <div style={{ textAlign: "center" }}>
              <div style={{ fontWeight: 600, wordBreak: "break-all", lineHeight: 1.3 }}>{n.id}</div>
              <div style={{ fontSize: 10, opacity: 0.65, marginTop: 2 }}>{colCount} column{colCount !== 1 ? "s" : ""}</div>
            </div>
          ),
        },
        style: { background: bg, color, border, borderRadius: 6, fontSize: 11, padding: "8px 12px", width: 200, textAlign: "center" as const },
      };
    });
  }, [collapsed, visibleNodes, visibleEdges, targetColId, targetTable, colPositions, tableLevel, tablePositions]);

  const rfEdges: Edge[] = useMemo(() => {
    if (!collapsed) {
      return visibleEdges.map((e, i) => {
        const isUnqualified = e.qualified === false;
        const isApprox = e.confidence === "approximate";
        const dim = isUnqualified || isApprox;
        const stroke = TRANSFORM_COLOURS[e.transform_type] ?? "#888";
        const label = `${e.transform_type}${isUnqualified ? " ~" : ""}`;
        return {
          id: `e-${i}`,
          source: e.source_col,
          target: e.target_col,
          label,
          animated: e.transform_type === "aggregation" || e.transform_type === "window",
          style: {
            stroke,
            strokeWidth: 1.5,
            strokeDasharray: dim ? "4 3" : undefined,
            opacity: dim ? 0.55 : 1,
          },
          labelStyle: { fontSize: 9, fill: dim ? "#6b7280" : "#888" },
          labelBgStyle: { fill: "#0a0f1a", fillOpacity: 0.8 },
        };
      });
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
  }, [collapsed, visibleEdges, tableLevel]);

  const filterEdgeCount = edges.filter((e) => e.transform_type === "filter").length;
  const joinKeyEdgeCount = edges.filter((e) => e.transform_type === "join_key").length;

  const pillClass = (on: boolean) =>
    `text-xs px-3 py-1 rounded border transition-colors ${
      on
        ? "bg-accent text-accent-foreground border-accent"
        : "text-muted-foreground border-border hover:text-foreground"
    }`;

  return (
    <div>
      <div className="flex justify-end mb-2 gap-2">
        {filterEdgeCount > 0 && (
          <button
            onClick={() => setShowFilters((v) => !v)}
            className={pillClass(showFilters)}
            title="Show edges from WHERE predicates"
          >
            ⚑ Filters ({filterEdgeCount})
          </button>
        )}
        {joinKeyEdgeCount > 0 && (
          <button
            onClick={() => setShowJoinKeys((v) => !v)}
            className={pillClass(showJoinKeys)}
            title="Show edges from JOIN ON predicates"
          >
            ⚷ Join keys ({joinKeyEdgeCount})
          </button>
        )}
        <button
          onClick={() => setCollapsed((v) => !v)}
          className={pillClass(collapsed)}
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
              const hasIn = visibleEdges.some((e) => e.target_col === n.id);
              const hasOut = visibleEdges.some((e) => e.source_col === n.id);
              if (!hasIn && hasOut) return "#4ade80";
              if (hasIn && !hasOut) return "#c084fc";
              return "#3d4f6b";
            }}
            style={{ background: "#0d1520", border: "1px solid #1e2d42" }}
            maskColor="rgba(10, 15, 26, 0.65)"
          />
        </ReactFlow>
        {/* Legend */}
        <div className="flex gap-4 px-3 py-1.5 text-xs flex-wrap" style={{ color: "#6b7a8d" }}>
          <span><span style={{ color: "#4ade80" }}>●</span> Source</span>
          <span><span style={{ color: "#7ec8e3" }}>●</span> Selected</span>
          <span><span style={{ color: "#c084fc" }}>●</span> Target</span>
          <span title="Unqualified / approximate lineage"><span style={{ color: "#6b7280", letterSpacing: 2 }}>╌</span> Unqualified</span>
          <span className="ml-auto flex gap-3 flex-wrap">
            {Object.entries(TRANSFORM_COLOURS).map(([type, color]) => (
              <span key={type}><span style={{ color }}>—</span> {type}</span>
            ))}
          </span>
        </div>
      </div>
    </div>
  );
}
