# DataLineage Explorer — Frontend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Next.js 15 frontend with four pages (Source Manager, Table Catalog, Lineage Explorer, Impact Analyzer) that consume the FastAPI backend and present column lineage in graph, tree, and code views.

**Architecture:** Next.js 15 App Router with TanStack Query for data fetching, Tailwind CSS + shadcn/ui for styling, React Flow for the graph view. The backend URL is injected via `NEXT_PUBLIC_API_URL` environment variable. No SSR for data — all pages are client components that fetch on mount.

**Tech Stack:** Next.js 15, React 18, TanStack Query v5, React Flow (@xyflow/react), Tailwind CSS, shadcn/ui, react-syntax-highlighter, TypeScript

**Prerequisite:** Backend must be running at a known URL before starting frontend development. Set `NEXT_PUBLIC_API_URL=http://localhost:8000` in `.env.local`.

---

## File Map

```
frontend/
├── .env.local                          # NEXT_PUBLIC_API_URL=http://localhost:8000
├── package.json
├── tailwind.config.ts
├── app/
│   ├── layout.tsx                      # Root layout: QueryProvider, nav
│   ├── page.tsx                        # Redirect to /sources
│   ├── sources/
│   │   └── page.tsx                    # Source Manager page
│   ├── catalog/
│   │   └── page.tsx                    # Table Catalog page
│   ├── lineage/
│   │   └── page.tsx                    # Lineage Explorer page (graph/tree/code tabs)
│   └── impact/
│       └── page.tsx                    # Impact Analyzer page
├── components/
│   ├── query-provider.tsx              # TanStack QueryClientProvider wrapper
│   ├── nav.tsx                         # Top navigation bar
│   ├── source-form.tsx                 # Add-source form (Git / Databricks / Upload tabs)
│   ├── transform-badge.tsx             # Colour-coded transform type badge
│   ├── lineage-graph.tsx               # React Flow lineage graph
│   ├── lineage-tree.tsx                # Collapsible upstream tree
│   └── code-inspector.tsx             # Split panel: column list + syntax-highlighted code
└── lib/
    ├── api.ts                          # fetch wrappers for all backend endpoints
    └── hooks.ts                        # TanStack Query hooks
```

---

## Task 1: Project Bootstrap

**Files:**
- Create: `frontend/` directory with Next.js 15 project

- [ ] **Step 1: Scaffold Next.js app**

```bash
cd D:/Python/ClaudeCode
npx create-next-app@latest frontend \
  --typescript \
  --tailwind \
  --eslint \
  --app \
  --no-src-dir \
  --import-alias "@/*"
```

When prompted, accept all defaults.

- [ ] **Step 2: Install dependencies**

```bash
cd frontend
npm install @tanstack/react-query @xyflow/react react-syntax-highlighter
npm install -D @types/react-syntax-highlighter
npx shadcn@latest init
```

When `shadcn init` prompts: choose **Default** style, **Slate** base colour, yes to CSS variables.

- [ ] **Step 3: Install shadcn components used in this plan**

```bash
npx shadcn@latest add button badge tabs card input label separator
```

- [ ] **Step 4: Create `.env.local`**

```bash
echo "NEXT_PUBLIC_API_URL=http://localhost:8000" > .env.local
```

- [ ] **Step 5: Verify dev server starts**

```bash
npm run dev
```

Open `http://localhost:3000`. Expected: Next.js default page with no errors in the terminal.

- [ ] **Step 6: Commit**

```bash
cd ..
git add frontend/
git commit -m "feat: scaffold Next.js frontend project"
```

---

## Task 2: API Client and Query Hooks

**Files:**
- Create: `frontend/lib/api.ts`
- Create: `frontend/lib/hooks.ts`

- [ ] **Step 1: Create `frontend/lib/api.ts`**

```typescript
const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API ${path} failed (${res.status}): ${body}`);
  }
  return res.json() as Promise<T>;
}

// ── Types ─────────────────────────────────────────────────────────────────

export type Source = {
  id: string;
  source_type: "git" | "databricks" | "upload";
  url: string;
  status: string;
  file_count: number;
};

export type TableSummary = {
  table: string;
  column_count: number;
};

export type ColumnMeta = {
  id: string;
  table: string;
  column: string;
  source_file: string | null;
  source_cell: number | null;
  source_line: number | null;
  transform_type: string | null;
};

export type LineageEdge = {
  source_col: string;
  target_col: string;
  transform_type: string;
  expression: string;
  source_file: string;
  source_cell: number | null;
  source_line: number | null;
};

export type LineageResponse = {
  target: string;
  upstream: LineageEdge[];
  downstream: LineageEdge[];
  graph: { nodes: { id: string }[]; edges: LineageEdge[] };
};

export type ImpactResponse = {
  source: string;
  downstream: LineageEdge[];
  affected_count: number;
};

export type SearchResult = {
  id: string;
  table: string;
  column: string;
};

export type Warning = {
  file: string;
  error: string;
};

// ── API functions ─────────────────────────────────────────────────────────

export const api = {
  sources: {
    list: () => apiFetch<Source[]>("/sources"),
    delete: (id: string) =>
      apiFetch<{ ok: boolean }>(`/sources/${id}`, { method: "DELETE" }),
    refresh: (id: string) =>
      apiFetch<{ ok: boolean; file_count: number; edge_count: number }>(
        `/sources/${id}/refresh`,
        { method: "POST" }
      ),
    register: async (formData: FormData) => {
      const res = await fetch(`${BASE}/sources`, {
        method: "POST",
        body: formData,
      });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`Register source failed (${res.status}): ${body}`);
      }
      return res.json() as Promise<Source>;
    },
  },
  tables: {
    list: () => apiFetch<TableSummary[]>("/tables"),
    columns: (table: string) =>
      apiFetch<ColumnMeta[]>(`/tables/${encodeURIComponent(table)}/columns`),
  },
  lineage: (table: string, column: string) =>
    apiFetch<LineageResponse>(
      `/lineage?table=${encodeURIComponent(table)}&column=${encodeURIComponent(column)}`
    ),
  impact: (table: string, column: string) =>
    apiFetch<ImpactResponse>(
      `/impact?table=${encodeURIComponent(table)}&column=${encodeURIComponent(column)}`
    ),
  search: (q: string) =>
    apiFetch<SearchResult[]>(`/search?q=${encodeURIComponent(q)}`),
  warnings: () => apiFetch<Warning[]>("/warnings"),
};
```

- [ ] **Step 2: Create `frontend/lib/hooks.ts`**

```typescript
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./api";

export function useSources() {
  return useQuery({ queryKey: ["sources"], queryFn: api.sources.list });
}

export function useDeleteSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.sources.delete,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sources"] }),
  });
}

export function useRefreshSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.sources.refresh,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["sources"] });
      qc.invalidateQueries({ queryKey: ["tables"] });
      qc.invalidateQueries({ queryKey: ["warnings"] });
    },
  });
}

export function useRegisterSource() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: api.sources.register,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["sources"] }),
  });
}

export function useTables() {
  return useQuery({ queryKey: ["tables"], queryFn: api.tables.list });
}

export function useColumns(table: string | null) {
  return useQuery({
    queryKey: ["columns", table],
    queryFn: () => api.tables.columns(table!),
    enabled: table !== null,
  });
}

export function useLineage(table: string | null, column: string | null) {
  return useQuery({
    queryKey: ["lineage", table, column],
    queryFn: () => api.lineage(table!, column!),
    enabled: table !== null && column !== null,
  });
}

export function useImpact(table: string | null, column: string | null) {
  return useQuery({
    queryKey: ["impact", table, column],
    queryFn: () => api.impact(table!, column!),
    enabled: table !== null && column !== null,
  });
}

export function useWarnings() {
  return useQuery({ queryKey: ["warnings"], queryFn: api.warnings });
}
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
cd frontend
npx tsc --noEmit
```

Expected: No errors.

- [ ] **Step 4: Commit**

```bash
cd ..
git add frontend/lib/
git commit -m "feat: add API client and TanStack Query hooks"
```

---

## Task 3: App Shell (Layout, QueryProvider, Nav)

**Files:**
- Create: `frontend/components/query-provider.tsx`
- Create: `frontend/components/nav.tsx`
- Modify: `frontend/app/layout.tsx`
- Create: `frontend/app/page.tsx`

- [ ] **Step 1: Create `frontend/components/query-provider.tsx`**

```typescript
"use client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";

export function QueryProvider({ children }: { children: React.ReactNode }) {
  const [client] = useState(() => new QueryClient());
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
```

- [ ] **Step 2: Create `frontend/components/nav.tsx`**

```typescript
"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";

const links = [
  { href: "/sources", label: "Sources" },
  { href: "/catalog", label: "Catalog" },
  { href: "/lineage", label: "Lineage" },
  { href: "/impact", label: "Impact" },
];

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
    </nav>
  );
}
```

- [ ] **Step 3: Update `frontend/app/layout.tsx`**

```typescript
import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { QueryProvider } from "@/components/query-provider";
import { Nav } from "@/components/nav";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "DataLineage Explorer",
  description: "Column-level data lineage for Databricks pipelines",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <QueryProvider>
          <Nav />
          <main className="p-6">{children}</main>
        </QueryProvider>
      </body>
    </html>
  );
}
```

- [ ] **Step 4: Create `frontend/app/page.tsx`**

```typescript
import { redirect } from "next/navigation";
export default function Home() {
  redirect("/sources");
}
```

- [ ] **Step 5: Verify dev server renders nav**

```bash
cd frontend && npm run dev
```

Open `http://localhost:3000`. Expected: Redirects to `/sources` with nav bar showing Sources, Catalog, Lineage, Impact.

- [ ] **Step 6: Commit**

```bash
cd ..
git add frontend/components/query-provider.tsx frontend/components/nav.tsx \
        frontend/app/layout.tsx frontend/app/page.tsx
git commit -m "feat: add app shell with QueryProvider and nav"
```

---

## Task 4: Transform Badge Component

**Files:**
- Create: `frontend/components/transform-badge.tsx`

- [ ] **Step 1: Create `frontend/components/transform-badge.tsx`**

```typescript
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

const COLOURS: Record<string, string> = {
  passthrough: "bg-green-100 text-green-800 border-green-200",
  aggregation: "bg-amber-100 text-amber-800 border-amber-200",
  expression:  "bg-purple-100 text-purple-800 border-purple-200",
  join_key:    "bg-blue-100 text-blue-800 border-blue-200",
  window:      "bg-indigo-100 text-indigo-800 border-indigo-200",
  cast:        "bg-slate-100 text-slate-800 border-slate-200",
  filter:      "bg-rose-100 text-rose-800 border-rose-200",
};

export function TransformBadge({ type }: { type: string | null }) {
  if (!type) return null;
  return (
    <Badge
      variant="outline"
      className={cn("text-xs font-medium", COLOURS[type] ?? "bg-muted text-muted-foreground")}
    >
      {type}
    </Badge>
  );
}
```

- [ ] **Step 2: Verify TypeScript compiles**

```bash
cd frontend && npx tsc --noEmit
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
cd ..
git add frontend/components/transform-badge.tsx
git commit -m "feat: add TransformBadge component"
```

---

## Task 5: Source Manager Page

**Files:**
- Create: `frontend/components/source-form.tsx`
- Create: `frontend/app/sources/page.tsx`

- [ ] **Step 1: Create `frontend/components/source-form.tsx`**

```typescript
"use client";
import { useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useRegisterSource, useRefreshSource } from "@/lib/hooks";

export function SourceForm() {
  const register = useRegisterSource();
  const refresh = useRefreshSource();
  const [gitUrl, setGitUrl] = useState("");
  const [gitToken, setGitToken] = useState("");
  const [dbHost, setDbHost] = useState("");
  const [dbToken, setDbToken] = useState("");
  const [file, setFile] = useState<File | null>(null);

  async function handleGit() {
    const fd = new FormData();
    fd.append("source_type", "git");
    fd.append("url", gitUrl);
    fd.append("token", gitToken);
    const src = await register.mutateAsync(fd);
    await refresh.mutateAsync(src.id);
  }

  async function handleDatabricks() {
    const fd = new FormData();
    fd.append("source_type", "databricks");
    fd.append("url", dbHost);
    fd.append("token", dbToken);
    const src = await register.mutateAsync(fd);
    await refresh.mutateAsync(src.id);
  }

  async function handleUpload() {
    if (!file) return;
    const fd = new FormData();
    fd.append("source_type", "upload");
    fd.append("file", file);
    const src = await register.mutateAsync(fd);
    await refresh.mutateAsync(src.id);
  }

  const busy = register.isPending || refresh.isPending;

  return (
    <Tabs defaultValue="git">
      <TabsList>
        <TabsTrigger value="git">Git Repo</TabsTrigger>
        <TabsTrigger value="databricks">Databricks API</TabsTrigger>
        <TabsTrigger value="upload">Upload ZIP</TabsTrigger>
      </TabsList>

      <TabsContent value="git" className="space-y-3 pt-3">
        <div className="space-y-1">
          <Label>Repository URL</Label>
          <Input placeholder="https://github.com/org/repo" value={gitUrl} onChange={(e) => setGitUrl(e.target.value)} />
        </div>
        <div className="space-y-1">
          <Label>Personal Access Token (optional)</Label>
          <Input type="password" placeholder="ghp_..." value={gitToken} onChange={(e) => setGitToken(e.target.value)} />
        </div>
        <Button onClick={handleGit} disabled={busy || !gitUrl}>
          {busy ? "Connecting…" : "Connect & Parse"}
        </Button>
      </TabsContent>

      <TabsContent value="databricks" className="space-y-3 pt-3">
        <div className="space-y-1">
          <Label>Workspace Host</Label>
          <Input placeholder="https://adb-xxx.azuredatabricks.net" value={dbHost} onChange={(e) => setDbHost(e.target.value)} />
        </div>
        <div className="space-y-1">
          <Label>Access Token</Label>
          <Input type="password" placeholder="dapi..." value={dbToken} onChange={(e) => setDbToken(e.target.value)} />
        </div>
        <Button onClick={handleDatabricks} disabled={busy || !dbHost || !dbToken}>
          {busy ? "Connecting…" : "Connect & Parse"}
        </Button>
      </TabsContent>

      <TabsContent value="upload" className="space-y-3 pt-3">
        <div className="space-y-1">
          <Label>ZIP archive containing .ipynb / .py / .sql files</Label>
          <Input type="file" accept=".zip" onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
        </div>
        <Button onClick={handleUpload} disabled={busy || !file}>
          {busy ? "Uploading…" : "Upload & Parse"}
        </Button>
      </TabsContent>
    </Tabs>
  );
}
```

- [ ] **Step 2: Create `frontend/app/sources/page.tsx`**

```typescript
"use client";
import { useSources, useDeleteSource, useRefreshSource, useWarnings } from "@/lib/hooks";
import { SourceForm } from "@/components/source-form";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";

export default function SourcesPage() {
  const { data: sources, isLoading } = useSources();
  const { data: warnings } = useWarnings();
  const del = useDeleteSource();
  const refresh = useRefreshSource();

  return (
    <div className="max-w-3xl space-y-6">
      <h1 className="text-2xl font-semibold">Sources</h1>

      {warnings && warnings.length > 0 && (
        <div className="rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          ⚠ {warnings.length} parse warning{warnings.length > 1 ? "s" : ""} — some files may not be fully analyzed.
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Connected Sources</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {isLoading && <p className="text-sm text-muted-foreground">Loading…</p>}
          {sources?.length === 0 && (
            <p className="text-sm text-muted-foreground">No sources connected yet.</p>
          )}
          {sources?.map((src) => (
            <div key={src.id} className="flex items-center gap-3 rounded-md border px-3 py-2 text-sm">
              <span className={src.status === "parsed" ? "text-green-600" : "text-muted-foreground"}>●</span>
              <span className="flex-1 truncate font-medium">{src.url}</span>
              <span className="text-xs text-muted-foreground capitalize">{src.source_type}</span>
              <span className="text-xs text-muted-foreground">{src.file_count} files</span>
              <Button size="sm" variant="outline" onClick={() => refresh.mutate(src.id)}>
                ↻
              </Button>
              <Button size="sm" variant="ghost" onClick={() => del.mutate(src.id)}>
                ✕
              </Button>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Add Source</CardTitle>
        </CardHeader>
        <CardContent>
          <SourceForm />
        </CardContent>
      </Card>
    </div>
  );
}
```

- [ ] **Step 3: Test the page manually**

Start both backend (`uvicorn main:app --reload` in `backend/`) and frontend (`npm run dev` in `frontend/`). Open `http://localhost:3000/sources`. Expected:
- Empty sources list shown
- Add Source tabs visible (Git Repo / Databricks API / Upload ZIP)
- Upload a ZIP of sample SQL files → source appears in list with file count

- [ ] **Step 4: Commit**

```bash
git add frontend/components/source-form.tsx frontend/app/sources/
git commit -m "feat: add Source Manager page"
```

---

## Task 6: Table Catalog Page

**Files:**
- Create: `frontend/app/catalog/page.tsx`

- [ ] **Step 1: Create `frontend/app/catalog/page.tsx`**

```typescript
"use client";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { useTables, useColumns } from "@/lib/hooks";
import { TransformBadge } from "@/components/transform-badge";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

export default function CatalogPage() {
  const router = useRouter();
  const { data: tables, isLoading } = useTables();
  const [search, setSearch] = useState("");
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const { data: columns, isLoading: colsLoading } = useColumns(selectedTable);

  const filtered = tables?.filter((t) =>
    t.table.toLowerCase().includes(search.toLowerCase())
  ) ?? [];

  return (
    <div className="flex gap-6 h-[calc(100vh-120px)]">
      {/* Sidebar */}
      <div className="w-56 flex-shrink-0 space-y-2">
        <Input
          placeholder="Search tables…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="h-8 text-sm"
        />
        <div className="text-xs text-muted-foreground uppercase tracking-wide px-1">
          {isLoading ? "Loading…" : `${filtered.length} tables`}
        </div>
        <div className="space-y-0.5 overflow-y-auto max-h-[calc(100vh-200px)]">
          {filtered.map((t) => (
            <button
              key={t.table}
              onClick={() => setSelectedTable(t.table)}
              className={`w-full text-left px-2 py-1.5 rounded text-sm transition-colors ${
                selectedTable === t.table
                  ? "bg-accent text-accent-foreground font-medium"
                  : "hover:bg-muted text-muted-foreground hover:text-foreground"
              }`}
            >
              {t.table}
              <span className="ml-1 text-xs text-muted-foreground">({t.column_count})</span>
            </button>
          ))}
        </div>
      </div>

      {/* Main panel */}
      <div className="flex-1 overflow-auto">
        {!selectedTable && (
          <p className="text-sm text-muted-foreground mt-4">Select a table to view its columns.</p>
        )}
        {selectedTable && (
          <>
            <h2 className="text-lg font-semibold mb-4">{selectedTable}</h2>
            {colsLoading && <p className="text-sm text-muted-foreground">Loading columns…</p>}
            {columns && (
              <table className="w-full text-sm border-collapse">
                <thead>
                  <tr className="border-b text-xs text-muted-foreground uppercase tracking-wide">
                    <th className="text-left py-2 px-3 font-medium">Column</th>
                    <th className="text-left py-2 px-3 font-medium">Source</th>
                    <th className="text-left py-2 px-3 font-medium">Transform</th>
                    <th className="text-left py-2 px-3 font-medium">File</th>
                    <th className="py-2 px-3"></th>
                  </tr>
                </thead>
                <tbody>
                  {columns.map((col) => (
                    <tr key={col.id} className="border-b hover:bg-muted/40 transition-colors">
                      <td className="py-2 px-3 font-medium">{col.column}</td>
                      <td className="py-2 px-3 text-muted-foreground text-xs">
                        {col.source_file ?? "—"}
                        {col.source_line != null ? `:${col.source_line}` : ""}
                        {col.source_cell != null ? ` cell ${col.source_cell}` : ""}
                      </td>
                      <td className="py-2 px-3">
                        <TransformBadge type={col.transform_type} />
                      </td>
                      <td className="py-2 px-3 text-xs text-muted-foreground truncate max-w-[160px]">
                        {col.source_file ?? "—"}
                      </td>
                      <td className="py-2 px-3">
                        <Button
                          size="sm"
                          variant="ghost"
                          className="text-xs h-6 px-2"
                          onClick={() =>
                            router.push(`/lineage?table=${selectedTable}&column=${col.column}`)
                          }
                        >
                          View Lineage →
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Test the page manually**

With backend running and a source parsed, open `http://localhost:3000/catalog`. Expected:
- Table list in sidebar with column counts
- Clicking a table shows its columns with transform badges
- "View Lineage →" navigates to `/lineage?table=...&column=...`

- [ ] **Step 3: Commit**

```bash
git add frontend/app/catalog/
git commit -m "feat: add Table Catalog page"
```

---

## Task 7: Lineage Graph Component

**Files:**
- Create: `frontend/components/lineage-graph.tsx`

- [ ] **Step 1: Create `frontend/components/lineage-graph.tsx`**

```typescript
"use client";
import { useMemo } from "react";
import ReactFlow, {
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
          whiteSpace: "pre",
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
```

- [ ] **Step 2: Verify TypeScript compiles**

```bash
cd frontend && npx tsc --noEmit
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
cd ..
git add frontend/components/lineage-graph.tsx
git commit -m "feat: add React Flow lineage graph component"
```

---

## Task 8: Lineage Tree Component

**Files:**
- Create: `frontend/components/lineage-tree.tsx`

- [ ] **Step 1: Create `frontend/components/lineage-tree.tsx`**

```typescript
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
  if (depth > 8) return { colId, edge: null, children: [] }; // safety cap
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
```

- [ ] **Step 2: Commit**

```bash
git add frontend/components/lineage-tree.tsx
git commit -m "feat: add collapsible lineage tree component"
```

---

## Task 9: Code Inspector Component

**Files:**
- Create: `frontend/components/code-inspector.tsx`

- [ ] **Step 1: Create `frontend/components/code-inspector.tsx`**

```typescript
"use client";
import { useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import type { LineageEdge } from "@/lib/api";
import { TransformBadge } from "./transform-badge";

type Props = {
  targetColId: string;
  edges: LineageEdge[];
};

export function CodeInspector({ targetColId, edges }: Props) {
  const [selected, setSelected] = useState<LineageEdge | null>(
    edges.length > 0 ? edges[0] : null
  );

  if (edges.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">No upstream transformations found.</p>
    );
  }

  return (
    <div className="flex gap-4 h-[400px]">
      {/* Left: column list */}
      <div className="w-56 flex-shrink-0 border rounded-md overflow-y-auto">
        {edges.map((e, i) => (
          <button
            key={i}
            onClick={() => setSelected(e)}
            className={`w-full text-left px-3 py-2 text-sm border-b last:border-0 transition-colors ${
              selected === e ? "bg-accent text-accent-foreground" : "hover:bg-muted"
            }`}
          >
            <div className="font-mono text-xs text-muted-foreground">{e.source_col.split(".")[0]}</div>
            <div className="font-medium">{e.source_col.split(".")[1]}</div>
            <TransformBadge type={e.transform_type} />
          </button>
        ))}
      </div>

      {/* Right: code view */}
      <div className="flex-1 flex flex-col gap-2 min-w-0">
        {selected && (
          <>
            <div className="flex items-center gap-2 text-sm">
              <span className="font-medium">{selected.target_col}</span>
              <span className="text-muted-foreground">←</span>
              <span className="text-muted-foreground">{selected.source_col}</span>
              <TransformBadge type={selected.transform_type} />
            </div>
            <div className="text-xs text-muted-foreground">
              {selected.source_file}
              {selected.source_cell != null ? ` · cell ${selected.source_cell}` : ""}
              {selected.source_line != null ? ` · line ${selected.source_line}` : ""}
            </div>
            <div className="flex-1 overflow-auto rounded-md">
              <SyntaxHighlighter
                language={selected.source_file?.endsWith(".sql") ? "sql" : "python"}
                style={vscDarkPlus}
                customStyle={{ margin: 0, borderRadius: 6, fontSize: 12 }}
              >
                {selected.expression}
              </SyntaxHighlighter>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/components/code-inspector.tsx
git commit -m "feat: add code inspector split-panel component"
```

---

## Task 10: Lineage Explorer Page

**Files:**
- Create: `frontend/app/lineage/page.tsx`

- [ ] **Step 1: Create `frontend/app/lineage/page.tsx`**

```typescript
"use client";
import { useSearchParams } from "next/navigation";
import { Suspense } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useLineage } from "@/lib/hooks";
import { LineageGraph } from "@/components/lineage-graph";
import { LineageTree } from "@/components/lineage-tree";
import { CodeInspector } from "@/components/code-inspector";

function LineageContent() {
  const params = useSearchParams();
  const table = params.get("table");
  const column = params.get("column");
  const { data, isLoading, error } = useLineage(table, column);

  if (!table || !column) {
    return (
      <p className="text-sm text-muted-foreground">
        Select a column from the <a href="/catalog" className="underline">Catalog</a> to view its lineage.
      </p>
    );
  }

  if (isLoading) return <p className="text-sm text-muted-foreground">Loading lineage…</p>;
  if (error) return <p className="text-sm text-destructive">Error: {(error as Error).message}</p>;
  if (!data) return null;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <h1 className="text-2xl font-semibold">{column}</h1>
        <span className="text-muted-foreground text-sm">in {table}</span>
      </div>
      <div className="text-xs text-muted-foreground">
        {data.upstream.length} upstream source{data.upstream.length !== 1 ? "s" : ""} ·{" "}
        {data.downstream.length} downstream dependent{data.downstream.length !== 1 ? "s" : ""}
      </div>

      <Tabs defaultValue="graph">
        <TabsList>
          <TabsTrigger value="graph">⬡ Graph</TabsTrigger>
          <TabsTrigger value="tree">≡ Tree</TabsTrigger>
          <TabsTrigger value="code">&lt;/&gt; Code</TabsTrigger>
        </TabsList>

        <TabsContent value="graph" className="pt-4">
          <LineageGraph
            nodes={data.graph.nodes}
            edges={data.graph.edges}
            targetColId={data.target}
          />
        </TabsContent>

        <TabsContent value="tree" className="pt-4">
          <LineageTree targetColId={data.target} edges={data.upstream} />
        </TabsContent>

        <TabsContent value="code" className="pt-4">
          <CodeInspector targetColId={data.target} edges={data.upstream} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

export default function LineagePage() {
  return (
    <Suspense fallback={<p className="text-sm text-muted-foreground">Loading…</p>}>
      <LineageContent />
    </Suspense>
  );
}
```

- [ ] **Step 2: Test the page manually**

Open `http://localhost:3000/catalog`, click "View Lineage →" on any column. Expected:
- Lineage page loads with column name and table in heading
- Graph tab shows React Flow diagram with nodes and edges
- Tree tab shows collapsible upstream tree
- Code tab shows split panel with expression syntax highlighted

- [ ] **Step 3: Commit**

```bash
git add frontend/app/lineage/
git commit -m "feat: add Lineage Explorer page with graph/tree/code views"
```

---

## Task 11: Impact Analyzer Page

**Files:**
- Create: `frontend/app/impact/page.tsx`

- [ ] **Step 1: Create `frontend/app/impact/page.tsx`**

```typescript
"use client";
import { Suspense, useState } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { useTables, useColumns, useImpact } from "@/lib/hooks";
import { TransformBadge } from "@/components/transform-badge";
import { Button } from "@/components/ui/button";

function ImpactContent() {
  const params = useSearchParams();
  const router = useRouter();
  const [table, setTable] = useState(params.get("table") ?? "");
  const [column, setColumn] = useState(params.get("column") ?? "");

  const { data: tables } = useTables();
  const { data: columns } = useColumns(table || null);
  const { data, isLoading, error } = useImpact(table || null, column || null);

  function handleApply() {
    router.push(`/impact?table=${table}&column=${column}`);
  }

  return (
    <div className="max-w-3xl space-y-6">
      <h1 className="text-2xl font-semibold">Impact Analyzer</h1>
      <p className="text-sm text-muted-foreground">
        Select a source column to see all downstream columns affected by a change.
      </p>

      {/* Selector */}
      <div className="flex gap-3 items-end">
        <div className="space-y-1">
          <label className="text-xs text-muted-foreground uppercase tracking-wide">Table</label>
          <select
            className="border rounded px-2 py-1.5 text-sm bg-background"
            value={table}
            onChange={(e) => { setTable(e.target.value); setColumn(""); }}
          >
            <option value="">— select table —</option>
            {tables?.map((t) => <option key={t.table} value={t.table}>{t.table}</option>)}
          </select>
        </div>
        <div className="space-y-1">
          <label className="text-xs text-muted-foreground uppercase tracking-wide">Column</label>
          <select
            className="border rounded px-2 py-1.5 text-sm bg-background"
            value={column}
            onChange={(e) => setColumn(e.target.value)}
            disabled={!table}
          >
            <option value="">— select column —</option>
            {columns?.map((c) => <option key={c.column} value={c.column}>{c.column}</option>)}
          </select>
        </div>
        <Button onClick={handleApply} disabled={!table || !column} size="sm">
          Analyze
        </Button>
      </div>

      {/* Results */}
      {isLoading && <p className="text-sm text-muted-foreground">Analyzing…</p>}
      {error && <p className="text-sm text-destructive">Error: {(error as Error).message}</p>}
      {data && (
        <>
          <div className="rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
            ⚠ If <strong>{table}.{column}</strong> changes,{" "}
            <strong>{data.affected_count} downstream column{data.affected_count !== 1 ? "s" : ""}</strong>{" "}
            {data.affected_count !== 1 ? "are" : "is"} affected.
          </div>

          <div className="space-y-2">
            {data.downstream.length === 0 && (
              <p className="text-sm text-muted-foreground">No downstream dependents found.</p>
            )}
            {data.downstream.map((edge, i) => (
              <div
                key={i}
                className="flex items-center gap-3 rounded-md border border-l-4 border-l-amber-400 px-3 py-2 text-sm"
                style={{ marginLeft: `${Math.min(i, 4) * 16}px` }}
              >
                <span className="text-amber-500">↓</span>
                <span className="font-medium">{edge.target_col}</span>
                <TransformBadge type={edge.transform_type} />
                <span className="text-xs text-muted-foreground truncate">{edge.expression}</span>
                <span className="ml-auto text-xs text-muted-foreground">
                  {edge.source_file}
                  {edge.source_cell != null ? ` cell ${edge.source_cell}` : ""}
                  {edge.source_line != null ? `:${edge.source_line}` : ""}
                </span>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

export default function ImpactPage() {
  return (
    <Suspense fallback={<p className="text-sm text-muted-foreground">Loading…</p>}>
      <ImpactContent />
    </Suspense>
  );
}
```

- [ ] **Step 2: Test the page manually**

Open `http://localhost:3000/impact`. Expected:
- Table and column dropdowns populated from the catalog
- Selecting a source column and clicking Analyze shows cascading list of downstream dependents
- Amber warning banner shows affected column count

- [ ] **Step 3: Commit**

```bash
git add frontend/app/impact/
git commit -m "feat: add Impact Analyzer page"
```

---

## Task 12: Build Check and Deployment

**Files:**
- Modify: `frontend/.env.local` (for local), Vercel env vars (for prod)

- [ ] **Step 1: Run production build locally**

```bash
cd frontend
npm run build
```

Expected: Build completes with no TypeScript or ESLint errors. Note any warnings.

- [ ] **Step 2: Fix any build errors**

Common issues:
- `useSearchParams()` must be wrapped in `<Suspense>` — already handled in Tasks 10 and 11
- React Flow SSR issues — `lineage-graph.tsx` is a `"use client"` component so this is already handled
- Missing `"use client"` on any component that uses hooks — add it if needed

- [ ] **Step 3: Set Vercel environment variable**

In Vercel dashboard → Project Settings → Environment Variables:
```
NEXT_PUBLIC_API_URL = https://your-railway-backend.up.railway.app
```

- [ ] **Step 4: Push to trigger Vercel deploy**

```bash
cd ..
git add frontend/
git commit -m "chore: frontend production build verified"
git push origin main
```

Expected: Vercel auto-deploys from `main`. Check Vercel dashboard for successful deployment.

- [ ] **Step 5: Smoke test production URL**

Open the Vercel deployment URL. Expected:
- Nav renders
- `/sources` loads (even with empty state)
- No console errors related to missing env vars

---

## Self-Review Checklist

**Spec coverage:**
- [x] Source Manager with Git/Databricks/Upload tabs — Tasks 5
- [x] Table Catalog with searchable sidebar, column table, transform badges, View Lineage link — Task 6
- [x] Lineage Explorer with three switchable views (graph/tree/code) — Tasks 7, 8, 9, 10
- [x] React Flow graph with pan/zoom, edge labels — Task 7
- [x] Collapsible tree with file/cell/line references — Task 8
- [x] Code inspector with syntax highlighting — Task 9
- [x] Impact Analyzer with cascading list and affected count banner — Task 11
- [x] TanStack Query for all API calls — Task 2
- [x] Parse warnings banner — Task 5
- [x] `NEXT_PUBLIC_API_URL` env var — Tasks 1, 12
- [x] Vercel deployment — Task 12

**No placeholders found.**

**Type consistency:** `LineageEdge`, `Source`, `ColumnMeta`, etc. defined once in `lib/api.ts` (Task 2) and imported consistently in Tasks 7–11. `useLineage`, `useImpact`, `useColumns` hooks defined in Task 2 and called in Tasks 6, 10, 11.
