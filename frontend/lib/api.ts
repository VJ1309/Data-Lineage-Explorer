const BASE = "/api/backend";

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
  role: "source" | "target" | "intermediate" | "result";
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
