"use client";
import { useState } from "react";
import type { LineageEdge } from "@/lib/api";
import { TransformBadge } from "./transform-badge";

/** Split "catalog.schema.table.col" into [table, col] */
function splitColId(id: string): [string, string] {
  const dot = id.lastIndexOf(".");
  if (dot === -1) return [id, ""];
  return [id.slice(0, dot), id.slice(dot + 1)];
}

type TreeNode = {
  colId: string;
  edge: LineageEdge | null;
  children: TreeNode[];
};

function buildUpstreamTree(colId: string, edges: LineageEdge[], visited: Set<string> = new Set()): TreeNode {
  if (visited.has(colId)) return { colId, edge: null, children: [] };
  visited.add(colId);
  const incoming = edges.filter((e) => e.target_col === colId);
  return {
    colId,
    edge: null,
    children: incoming.map((e) => ({
      colId: e.source_col,
      edge: e,
      children: buildUpstreamTree(e.source_col, edges, visited).children,
    })),
  };
}

function buildDownstreamTree(colId: string, edges: LineageEdge[], visited: Set<string> = new Set()): TreeNode {
  if (visited.has(colId)) return { colId, edge: null, children: [] };
  visited.add(colId);
  const outgoing = edges.filter((e) => e.source_col === colId);
  return {
    colId,
    edge: null,
    children: outgoing.map((e) => ({
      colId: e.target_col,
      edge: e,
      children: buildDownstreamTree(e.target_col, edges, visited).children,
    })),
  };
}

function TreeNodeRow({
  node,
  depth,
  direction,
}: {
  node: TreeNode;
  depth: number;
  direction: "upstream" | "downstream";
}) {
  const [open, setOpen] = useState(true);
  const hasChildren = node.children.length > 0;
  const [table, col] = splitColId(node.colId);

  // Visual: leaf nodes in upstream are sources, leaf nodes in downstream are final targets
  const isLeaf = !hasChildren;
  const isSource = direction === "upstream" && isLeaf && depth > 0;
  const isTarget = direction === "downstream" && isLeaf && depth > 0;

  return (
    <div style={{ marginLeft: depth * 20 }}>
      <div
        className="flex items-center gap-2 py-1.5 text-sm cursor-pointer hover:bg-muted/40 rounded px-2"
        onClick={() => setOpen((v) => !v)}
      >
        <span className="text-muted-foreground w-3 flex-shrink-0">
          {hasChildren ? (open ? "▾" : "▸") : " "}
        </span>
        {direction === "upstream" && depth > 0 && (
          <span className="text-muted-foreground text-xs flex-shrink-0">←</span>
        )}
        {direction === "downstream" && depth > 0 && (
          <span className="text-muted-foreground text-xs flex-shrink-0">→</span>
        )}
        <span className={`font-mono text-xs flex-shrink-0 ${
          isSource ? "text-green-500" : isTarget ? "text-purple-400" : "text-muted-foreground"
        }`}>
          {table}.
        </span>
        <span className={`font-medium ${
          isSource ? "text-green-400" : isTarget ? "text-purple-300" : ""
        }`}>
          {col}
        </span>
        {node.edge && <TransformBadge type={node.edge.transform_type} />}
        {node.edge?.expression && (
          <span className="ml-auto text-xs text-muted-foreground truncate max-w-[220px] flex-shrink-0">
            {node.edge.expression}
          </span>
        )}
        {node.edge?.source_file && (
          <span className="text-xs text-muted-foreground flex-shrink-0">
            {node.edge.source_file}
            {node.edge.source_cell != null ? ` (cell ${node.edge.source_cell})` : ""}
          </span>
        )}
      </div>
      {open && node.children.map((child, i) => (
        <TreeNodeRow key={`${child.colId}-${i}`} node={child} depth={depth + 1} direction={direction} />
      ))}
    </div>
  );
}

type Props = {
  targetColId: string;
  upstream: LineageEdge[];
  downstream: LineageEdge[];
};

export function LineageTree({ targetColId, upstream, downstream }: Props) {
  const upRoot = buildUpstreamTree(targetColId, upstream);
  const downRoot = buildDownstreamTree(targetColId, downstream);
  const [table, col] = splitColId(targetColId);

  return (
    <div className="rounded-md border bg-background max-h-[500px] overflow-y-auto">
      {/* Upstream section */}
      {upstream.length > 0 && (
        <div className="border-b">
          <div className="px-3 py-2 text-xs font-semibold uppercase tracking-wider text-green-600 dark:text-green-400 bg-muted/30">
            ← Upstream Sources ({upstream.length} edge{upstream.length !== 1 ? "s" : ""})
          </div>
          <div className="p-2">
            {upRoot.children.map((child, i) => (
              <TreeNodeRow key={`up-${child.colId}-${i}`} node={child} depth={0} direction="upstream" />
            ))}
          </div>
        </div>
      )}

      {/* Selected column */}
      <div className="px-3 py-2.5 bg-accent/30 border-b border-t flex items-center gap-2">
        <span className="text-cyan-500 text-sm">●</span>
        <span className="font-mono text-xs text-muted-foreground">{table}.</span>
        <span className="font-semibold">{col}</span>
        <span className="text-xs text-muted-foreground ml-1">(selected)</span>
      </div>

      {/* Downstream section */}
      {downstream.length > 0 && (
        <div>
          <div className="px-3 py-2 text-xs font-semibold uppercase tracking-wider text-purple-600 dark:text-purple-400 bg-muted/30">
            → Downstream Targets ({downstream.length} edge{downstream.length !== 1 ? "s" : ""})
          </div>
          <div className="p-2">
            {downRoot.children.map((child, i) => (
              <TreeNodeRow key={`down-${child.colId}-${i}`} node={child} depth={0} direction="downstream" />
            ))}
          </div>
        </div>
      )}

      {upstream.length === 0 && downstream.length === 0 && (
        <div className="p-4 text-sm text-muted-foreground">No lineage edges found for this column.</div>
      )}
    </div>
  );
}
