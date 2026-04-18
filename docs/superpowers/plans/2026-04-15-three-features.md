# Three UX Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add expression drill-down in Catalog, global column search in the nav, and table-level collapse/expand in the Lineage Graph.

**Architecture:** Feature 1 requires a one-field backend change + catalog UI. Feature 2 is pure frontend (backend `/search` already exists). Feature 3 is a self-contained toggle inside `lineage-graph.tsx` that switches between column-level and table-level node rendering.

**Tech Stack:** FastAPI (backend), Next.js App Router, React Query, @xyflow/react (ReactFlow), TypeScript.

---

## File Map

| File | Change |
|---|---|
| `backend/api/routes.py` | Add `expression` field to `/tables/{table}/columns` response |
| `frontend/lib/api.ts` | Add `expression` to `ColumnMeta` type; add `useSearch` hook |
| `frontend/lib/hooks.ts` | Add `useSearch` hook |
| `frontend/app/catalog/page.tsx` | Render expression with expand/collapse toggle per row |
| `frontend/components/nav.tsx` | Add global search bar with live dropdown |
| `frontend/components/lineage-graph.tsx` | Add table-collapse toggle; compute table-level nodes+edges |

---

## Task 1: Backend — expose `expression` on the columns endpoint

**Files:**
- Modify: `backend/api/routes.py:254-263`

The `/tables/{table}/columns` handler builds a dict for each column but omits `expression`. It already has `edge_data` in scope. Add it.

- [ ] **Step 1: Add `expression` to the column dict**

In `backend/api/routes.py`, change the `cols.append({...})` block (line ~254) from:

```python
cols.append({
    "id": node,
    "table": t,
    "column": col,
    "source_tables": source_tables,
    "source_file": edge_data.source_file if edge_data else None,
    "source_cell": edge_data.source_cell if edge_data else None,
    "source_line": edge_data.source_line if edge_data else None,
    "transform_type": edge_data.transform_type if edge_data else None,
})
```

to:

```python
cols.append({
    "id": node,
    "table": t,
    "column": col,
    "source_tables": source_tables,
    "source_file": edge_data.source_file if edge_data else None,
    "source_cell": edge_data.source_cell if edge_data else None,
    "source_line": edge_data.source_line if edge_data else None,
    "transform_type": edge_data.transform_type if edge_data else None,
    "expression": edge_data.expression if edge_data else None,
})
```

- [ ] **Step 2: Verify with curl (start the dev server first)**

```bash
cd backend && uvicorn main:app --reload --port 8000
# in another shell, after uploading a source:
curl -s "http://localhost:8000/tables/<table>/columns" | python -m json.tool | grep expression
```

Expected: lines like `"expression": "price * quantity"` or `"expression": null`

- [ ] **Step 3: Commit**

```bash
git add backend/api/routes.py
git commit -m "feat: expose expression field in columns endpoint"
```

---

## Task 2: Frontend types — add `expression` to `ColumnMeta`

**Files:**
- Modify: `frontend/lib/api.ts:28-37`

- [ ] **Step 1: Add field to type**

In `frontend/lib/api.ts`, change `ColumnMeta` from:

```typescript
export type ColumnMeta = {
  id: string;
  table: string;
  column: string;
  source_tables: string[];
  source_file: string | null;
  source_cell: number | null;
  source_line: number | null;
  transform_type: string | null;
};
```

to:

```typescript
export type ColumnMeta = {
  id: string;
  table: string;
  column: string;
  source_tables: string[];
  source_file: string | null;
  source_cell: number | null;
  source_line: number | null;
  transform_type: string | null;
  expression: string | null;
};
```

- [ ] **Step 2: Verify TypeScript compiles**

```bash
cd frontend && npm run build 2>&1 | tail -5
```

Expected: build completes with no TypeScript errors.

- [ ] **Step 3: Commit**

```bash
git add frontend/lib/api.ts
git commit -m "feat: add expression to ColumnMeta type"
```

---

## Task 3: Catalog — expression drill-down per row

**Files:**
- Modify: `frontend/app/catalog/page.tsx`

Each column row in the catalog table shows a `TransformBadge`. If `col.expression` is non-null and non-empty, show a small `▾` toggle that expands an inline code block beneath the row.

- [ ] **Step 1: Add per-row expanded state and expression row**

Replace the `<tbody>` section in `frontend/app/catalog/page.tsx`. The current row is:

```tsx
<tbody>
  {columns.map((col) => (
    <tr key={col.id} className="border-b hover:bg-muted/40 transition-colors">
      <td className="py-2 px-3 font-medium">{col.column}</td>
      <td className="py-2 px-3 text-xs">
        {col.source_tables.length > 0
          ? col.source_tables.map((st, i) => (
              <span key={st}>
                {i > 0 && <span className="text-muted-foreground">, </span>}
                <span className="text-blue-600 dark:text-blue-400">{st}</span>
              </span>
            ))
          : <span className="text-muted-foreground">—</span>
        }
      </td>
      <td className="py-2 px-3">
        <TransformBadge type={col.transform_type} />
      </td>
      <td className="py-2 px-3 text-xs text-muted-foreground truncate max-w-[200px]">
        {col.source_file ?? "—"}
        {col.source_cell != null ? ` (cell ${col.source_cell})` : ""}
        {col.source_line != null ? `:${col.source_line}` : ""}
      </td>
      <td className="py-2 px-3">
        <Button
          size="sm"
          variant="ghost"
          className="text-xs h-6 px-2"
          onClick={() =>
            router.push(`/lineage?table=${encodeURIComponent(selectedTable)}&column=${encodeURIComponent(col.column)}`)
          }
        >
          View Lineage →
        </Button>
      </td>
    </tr>
  ))}
</tbody>
```

Replace it with (this adds a `Set<string>` state for expanded rows and an optional expression expansion row):

```tsx
<tbody>
  {columns.map((col) => (
    <ColumnRow
      key={col.id}
      col={col}
      onLineage={() =>
        router.push(`/lineage?table=${encodeURIComponent(selectedTable!)}&column=${encodeURIComponent(col.column)}`)
      }
    />
  ))}
</tbody>
```

And add the `ColumnRow` component above `CatalogPage` (after the imports):

```tsx
function ColumnRow({ col, onLineage }: { col: import("@/lib/api").ColumnMeta; onLineage: () => void }) {
  const [open, setOpen] = useState(false);
  const hasExpr = !!col.expression;

  return (
    <>
      <tr className="border-b hover:bg-muted/40 transition-colors">
        <td className="py-2 px-3 font-medium">{col.column}</td>
        <td className="py-2 px-3 text-xs">
          {col.source_tables.length > 0
            ? col.source_tables.map((st, i) => (
                <span key={st}>
                  {i > 0 && <span className="text-muted-foreground">, </span>}
                  <span className="text-blue-600 dark:text-blue-400">{st}</span>
                </span>
              ))
            : <span className="text-muted-foreground">—</span>
          }
        </td>
        <td className="py-2 px-3">
          <div className="flex items-center gap-1.5">
            <TransformBadge type={col.transform_type} />
            {hasExpr && (
              <button
                onClick={() => setOpen((v) => !v)}
                className="text-xs text-muted-foreground hover:text-foreground transition-colors"
                title="Show expression"
              >
                {open ? "▴" : "▾"}
              </button>
            )}
          </div>
        </td>
        <td className="py-2 px-3 text-xs text-muted-foreground truncate max-w-[200px]">
          {col.source_file ?? "—"}
          {col.source_cell != null ? ` (cell ${col.source_cell})` : ""}
          {col.source_line != null ? `:${col.source_line}` : ""}
        </td>
        <td className="py-2 px-3">
          <Button size="sm" variant="ghost" className="text-xs h-6 px-2" onClick={onLineage}>
            View Lineage →
          </Button>
        </td>
      </tr>
      {open && hasExpr && (
        <tr className="border-b bg-muted/20">
          <td colSpan={5} className="px-6 py-2">
            <code className="text-xs font-mono text-purple-400 whitespace-pre-wrap break-all">
              {col.expression}
            </code>
          </td>
        </tr>
      )}
    </>
  );
}
```

- [ ] **Step 2: Build to verify no TypeScript errors**

```bash
cd frontend && npm run build 2>&1 | tail -10
```

Expected: clean build, no errors.

- [ ] **Step 3: Commit**

```bash
git add frontend/app/catalog/page.tsx
git commit -m "feat: expression drill-down in catalog column rows"
```

---

## Task 4: Global column search — hook

**Files:**
- Modify: `frontend/lib/hooks.ts`

The `api.search` function already exists in `api.ts`. Wire a React Query hook for it.

- [ ] **Step 1: Add `useSearch` hook**

Add at the end of `frontend/lib/hooks.ts`:

```typescript
export function useSearch(q: string) {
  return useQuery({
    queryKey: ["search", q],
    queryFn: () => api.search(q),
    enabled: q.length >= 2,
    staleTime: 10_000,
  });
}
```

- [ ] **Step 2: Build**

```bash
cd frontend && npm run build 2>&1 | tail -5
```

Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add frontend/lib/hooks.ts
git commit -m "feat: add useSearch hook"
```

---

## Task 5: Global column search — nav UI

**Files:**
- Modify: `frontend/components/nav.tsx`

Add a search input to the right side of the nav. As the user types (≥2 chars), show a live dropdown of matching `{table}.{column}` results. Clicking a result navigates to `/lineage?table=...&column=...`.

- [ ] **Step 1: Replace `nav.tsx` with search-enabled version**

```tsx
"use client";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useState, useRef, useEffect } from "react";
import { cn } from "@/lib/utils";
import { useSearch } from "@/lib/hooks";

const links = [
  { href: "/sources", label: "Sources" },
  { href: "/catalog", label: "Catalog" },
  { href: "/lineage", label: "Lineage" },
  { href: "/impact", label: "Impact" },
];

function SearchBox() {
  const router = useRouter();
  const [q, setQ] = useState("");
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const { data: results } = useSearch(q);

  // Close dropdown on outside click
  useEffect(() => {
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  function select(table: string, column: string) {
    setQ("");
    setOpen(false);
    router.push(`/lineage?table=${encodeURIComponent(table)}&column=${encodeURIComponent(column)}`);
  }

  return (
    <div ref={ref} className="relative ml-auto">
      <input
        type="text"
        value={q}
        placeholder="Search columns…"
        onChange={(e) => { setQ(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        className="h-7 w-52 rounded border bg-muted px-2 text-xs placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
      />
      {open && q.length >= 2 && results && results.length > 0 && (
        <div className="absolute right-0 top-8 z-50 w-80 rounded-md border bg-popover shadow-md">
          <div className="max-h-64 overflow-y-auto py-1">
            {results.slice(0, 20).map((r) => (
              <button
                key={r.id}
                onClick={() => select(r.table, r.column)}
                className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent transition-colors"
              >
                <span className="text-muted-foreground text-xs font-mono">{r.table}.</span>
                <span className="font-medium">{r.column}</span>
              </button>
            ))}
          </div>
          {results.length === 0 && (
            <p className="px-3 py-2 text-xs text-muted-foreground">No results for "{q}"</p>
          )}
        </div>
      )}
      {open && q.length >= 2 && results && results.length === 0 && (
        <div className="absolute right-0 top-8 z-50 w-72 rounded-md border bg-popover shadow-md">
          <p className="px-3 py-2 text-xs text-muted-foreground">No results for "{q}"</p>
        </div>
      )}
    </div>
  );
}

export function Nav() {
  const path = usePathname();
  return (
    <nav className="border-b bg-background px-6 py-3 flex items-center gap-6">
      <span className="font-bold text-sm tracking-tight mr-4">
        DataLineage Explorer
      </span>
      {links.map((l) => (
        <Link
          key={l.href}
          href={l.href}
          className={cn(
            "text-sm transition-colors hover:text-foreground",
            path.startsWith(l.href)
              ? "text-foreground font-medium"
              : "text-muted-foreground"
          )}
        >
          {l.label}
        </Link>
      ))}
      <SearchBox />
    </nav>
  );
}
```

- [ ] **Step 2: Build**

```bash
cd frontend && npm run build 2>&1 | tail -10
```

Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add frontend/components/nav.tsx
git commit -m "feat: global column search in nav with live dropdown"
```

---

## Task 6: Lineage Graph — table-level collapse/expand toggle

**Files:**
- Modify: `frontend/components/lineage-graph.tsx`

Add a "Group by table" toggle button above the graph. When active, each table becomes one node and edges are drawn table-to-table (deduplicated, annotated with edge count). When inactive, current column-level behavior.

- [ ] **Step 1: Replace `lineage-graph.tsx` with the table-collapse version**

```tsx
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

/** Build table-level nodes and edges from column-level data. */
function toTableLevel(
  colNodeIds: string[],
  colEdges: LineageEdge[],
  targetColId: string,
): { nodes: { id: string }[]; edges: { source: string; target: string; count: number; types: Set<string> }[] } {
  const tables = new Set<string>();
  for (const id of colNodeIds) {
    const [table] = splitColId(id);
    tables.add(table);
  }

  // Deduplicate table→table edges
  const edgeMap = new Map<string, { count: number; types: Set<string> }>();
  for (const e of colEdges) {
    const [srcTable] = splitColId(e.source_col);
    const [tgtTable] = splitColId(e.target_col);
    if (srcTable === tgtTable) continue; // skip self-loops
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

  // Column-level layout
  const colPositions = useMemo(
    () => layeredLayout(nodes.map((n) => n.id), edges.map((e) => ({ source: e.source_col, target: e.target_col }))),
    [nodes, edges],
  );

  // Table-level data
  const tableLevel = useMemo(() => toTableLevel(nodes.map((n) => n.id), edges, targetColId), [nodes, edges, targetColId]);
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

    // Table-level nodes
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

      // Show column count for this table
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
```

- [ ] **Step 2: Build**

```bash
cd frontend && npm run build 2>&1 | tail -10
```

Expected: clean build, no TypeScript errors.

- [ ] **Step 3: Commit**

```bash
git add frontend/components/lineage-graph.tsx
git commit -m "feat: table-level collapse/expand toggle in lineage graph"
```

---

## Task 7: Push and verify

- [ ] **Step 1: Final build check**

```bash
cd frontend && npm run build 2>&1 | tail -10
```

Expected: clean build.

- [ ] **Step 2: Push**

```bash
git push
```

Expected: all commits pushed, Vercel deploys automatically.

---

## Self-Review Checklist

- [x] **Expression drill-down:** Backend adds `expression` field → frontend type updated → catalog row has ▾ toggle → inline `<code>` block shows SQL
- [x] **Global search:** `useSearch` hook wraps `api.search` → `SearchBox` in nav renders live dropdown → click navigates to `/lineage?table=...&column=...`
- [x] **Table collapse:** `collapsed` state toggles between column-level and table-level `rfNodes`/`rfEdges` → button label flips → minimap and legend both work in both modes
- [x] **No placeholders:** All code blocks are complete
- [x] **Type consistency:** `splitColId` used consistently; `tableLevel.edges` uses `.source`/`.target` (not `.source_col`/`.target_col`) throughout Task 6
- [x] **`ColumnMeta.expression`** added in Task 2 before it is consumed in Task 3
- [x] **`useSearch`** defined in Task 4 before it is imported in Task 5
