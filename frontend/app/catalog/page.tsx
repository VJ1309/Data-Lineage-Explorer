"use client";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { useTables, useColumns } from "@/lib/hooks";
import { TransformBadge } from "@/components/transform-badge";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import type { TableSummary, ColumnMeta } from "@/lib/api";
import { ArrowRight } from "lucide-react";

const ROLE_CONFIG: Record<string, { label: string; description: string; color: string; dot: string }> = {
  target: {
    label: "Target Tables",
    description: "Final output tables — written to but not read from within these files",
    color: "text-green-400",
    dot: "bg-green-500",
  },
  intermediate: {
    label: "Intermediate Tables",
    description: "Both read from and written to — staging or transformation tables",
    color: "text-amber-400",
    dot: "bg-amber-500",
  },
  source: {
    label: "Source Tables",
    description: "External source tables — read from but not created in these files",
    color: "text-blue-400",
    dot: "bg-blue-500",
  },
  result: {
    label: "Ungrouped Queries",
    description: "Standalone SELECT queries with no INSERT INTO target",
    color: "text-muted-foreground",
    dot: "bg-muted-foreground",
  },
};

const ROLE_ORDER = ["target", "intermediate", "source", "result"];

function groupByRole(tables: TableSummary[]): Record<string, TableSummary[]> {
  const groups: Record<string, TableSummary[]> = {};
  for (const t of tables) {
    const role = t.role || "source";
    if (!groups[role]) groups[role] = [];
    groups[role].push(t);
  }
  return groups;
}

function ColumnRow({ col, onLineage }: { col: ColumnMeta; onLineage: () => void }) {
  const [open, setOpen] = useState(false);
  const hasExpr = !!col.expression && col.transform_type !== "passthrough";

  return (
    <>
      <tr className="border-b border-border hover:bg-accent/50 transition-colors group">
        <td className="py-2 px-3">
          <span className="font-mono text-xs text-foreground">{col.column}</span>
        </td>
        <td className="py-2 px-3 text-xs">
          {col.source_tables.length > 0
            ? col.source_tables.map((st, i) => (
                <span key={st}>
                  {i > 0 && <span className="text-muted-foreground">, </span>}
                  <span className="font-mono text-blue-400">{st}</span>
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
        <td className="py-2 px-3 text-xs text-muted-foreground">
          <span className="font-mono">
            {col.source_file ?? "—"}
            {col.source_cell != null ? ` (cell ${col.source_cell})` : ""}
            {col.source_line != null ? `:${col.source_line}` : ""}
          </span>
        </td>
        <td className="py-2 px-3">
          <button
            onClick={onLineage}
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-primary transition-colors opacity-0 group-hover:opacity-100"
          >
            Lineage <ArrowRight className="h-3 w-3" />
          </button>
        </td>
      </tr>
      {open && hasExpr && (
        <tr className="border-b border-border bg-secondary/50">
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

export default function CatalogPage() {
  const router = useRouter();
  const { data: tables, isLoading } = useTables();
  const [search, setSearch] = useState("");
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const { data: columns, isLoading: colsLoading } = useColumns(selectedTable);

  const filtered = tables?.filter((t) =>
    t.table.toLowerCase().includes(search.toLowerCase())
  ) ?? [];

  const grouped = groupByRole(filtered);
  const selectedRole = tables?.find((t) => t.table === selectedTable)?.role;
  const selectedConfig = ROLE_CONFIG[selectedRole || "source"];

  return (
    <div className="flex gap-0 h-[calc(100vh-120px)] -mx-6 -mt-6">
      {/* Sidebar */}
      <div className="w-64 flex-shrink-0 border-r border-border bg-card flex flex-col">
        <div className="p-3 border-b border-border">
          <Input
            placeholder="Search tables…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 text-sm bg-secondary border-border"
          />
          <div className="text-xs text-muted-foreground mt-2 px-0.5">
            {isLoading ? "Loading…" : `${filtered.length} table${filtered.length !== 1 ? "s" : ""}`}
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-2 space-y-3">
          {ROLE_ORDER.map((role) => {
            const group = grouped[role];
            if (!group || group.length === 0) return null;
            const config = ROLE_CONFIG[role];
            return (
              <div key={role}>
                <div className={`px-2 py-1 text-[10px] font-semibold uppercase tracking-widest ${config.color} opacity-70`}>
                  {config.label} ({group.length})
                </div>
                <div className="space-y-0.5 mt-0.5">
                  {group.map((t) => (
                    <button
                      key={t.table}
                      title={t.table}
                      onClick={() => setSelectedTable(t.table)}
                      className={`w-full text-left px-2 py-1.5 rounded text-xs transition-colors flex items-center gap-2 ${
                        selectedTable === t.table
                          ? "bg-primary/15 text-primary font-medium"
                          : "hover:bg-accent text-muted-foreground hover:text-foreground"
                      }`}
                    >
                      <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${config.dot}`} />
                      <span className="truncate font-mono">{t.table}</span>
                      <span className="ml-auto text-muted-foreground flex-shrink-0 tabular-nums">
                        {t.column_count}
                      </span>
                    </button>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Main panel */}
      <div className="flex-1 overflow-auto p-6">
        {!selectedTable && (
          <div className="space-y-5">
            <p className="text-sm text-muted-foreground">Select a table to view its columns and lineage.</p>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {ROLE_ORDER.map((role) => {
                const count = grouped[role]?.length ?? 0;
                const config = ROLE_CONFIG[role];
                return (
                  <div key={role} className="border border-border rounded-lg p-4 bg-card">
                    <div className={`text-3xl font-bold tabular-nums ${config.color}`}>{count}</div>
                    <div className="text-sm font-medium mt-1.5 text-foreground">{config.label}</div>
                    <div className="text-xs text-muted-foreground mt-1 leading-relaxed">{config.description}</div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {selectedTable && (
          <>
            <div className="flex items-center gap-2.5 mb-5 pb-4 border-b border-border">
              <span className={`w-2 h-2 rounded-full flex-shrink-0 ${selectedConfig.dot}`} />
              <h2 className="font-mono text-base font-semibold text-foreground">{selectedTable}</h2>
              <span className={`text-[10px] font-semibold uppercase tracking-widest ml-1 ${selectedConfig.color}`}>
                {selectedRole}
              </span>
              {columns && (
                <span className="ml-auto text-xs text-muted-foreground tabular-nums">
                  {columns.length} column{columns.length !== 1 ? "s" : ""}
                </span>
              )}
            </div>

            {colsLoading && <p className="text-sm text-muted-foreground">Loading columns…</p>}
            {columns && (
              <table className="w-full text-sm border-collapse">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-2 px-3 text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">Column</th>
                    <th className="text-left py-2 px-3 text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">Source Table</th>
                    <th className="text-left py-2 px-3 text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">Transform</th>
                    <th className="text-left py-2 px-3 text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">Defined In</th>
                    <th className="py-2 px-3"></th>
                  </tr>
                </thead>
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
              </table>
            )}
          </>
        )}
      </div>
    </div>
  );
}
