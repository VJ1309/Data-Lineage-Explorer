"use client";
import { useState } from "react";
import type { LineageEdge } from "@/lib/api";
import { TransformBadge } from "./transform-badge";

type TreeNode = {
  colId: string;
  edge: LineageEdge | null;
  children: TreeNode[];
};

function buildTree(colId: string, edges: LineageEdge[], depth = 0): TreeNode {
  if (depth > 8) return { colId, edge: null, children: [] };
  const incoming = edges.filter((e) => e.target_col === colId);
  return {
    colId,
    edge: null,
    children: incoming.map((e) => ({
      colId: e.source_col,
      edge: e,
      children: buildTree(e.source_col, edges, depth + 1).children,
    })),
  };
}

function TreeNodeRow({ node, depth }: { node: TreeNode; depth: number }) {
  const [open, setOpen] = useState(true);
  const hasChildren = node.children.length > 0;
  const [, col] = node.colId.split(".");

  return (
    <div style={{ marginLeft: depth * 16 }}>
      <div
        className="flex items-center gap-2 py-1 text-sm cursor-pointer hover:bg-muted/40 rounded px-2"
        onClick={() => setOpen((v) => !v)}
      >
        <span className="text-muted-foreground w-3">{hasChildren ? (open ? "▾" : "▸") : " "}</span>
        <span className="font-mono text-xs text-muted-foreground">{node.colId.split(".")[0]}.</span>
        <span className="font-medium">{col}</span>
        {node.edge && <TransformBadge type={node.edge.transform_type} />}
        {node.edge && (
          <span className="ml-auto text-xs text-muted-foreground truncate max-w-[200px]">
            {node.edge.expression}
          </span>
        )}
        {node.edge?.source_file && (
          <span className="text-xs text-muted-foreground">
            {node.edge.source_file}
            {node.edge.source_cell != null ? ` cell ${node.edge.source_cell}` : ""}
            {node.edge.source_line != null ? `:${node.edge.source_line}` : ""}
          </span>
        )}
      </div>
      {open && node.children.map((child, i) => (
        <TreeNodeRow key={`${child.colId}-${i}`} node={child} depth={depth + 1} />
      ))}
    </div>
  );
}

export function LineageTree({ targetColId, edges }: { targetColId: string; edges: LineageEdge[] }) {
  const root = buildTree(targetColId, edges);
  return (
    <div className="rounded-md border bg-background p-3 max-h-[400px] overflow-y-auto">
      <TreeNodeRow node={{ ...root, colId: targetColId }} depth={0} />
    </div>
  );
}
