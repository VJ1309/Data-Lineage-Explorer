"use client";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { useTables, useColumns } from "@/lib/hooks";
import { TransformBadge } from "@/components/transform-badge";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import type { TableSummary } from "@/lib/api";

const ROLE_CONFIG: Record<string, { label: string; description: string; color: string }> = {
  target: {
    label: "Target Tables",
    description: "Final output tables — written to but not read from within these files",
    color: "text-green-600 dark:text-green-400",
  },
  intermediate: {
    label: "Intermediate Tables",
    description: "Both read from and written to — staging or transformation tables",
    color: "text-amber-600 dark:text-amber-400",
  },
  source: {
    label: "Source Tables",
    description: "External source tables — read from but not created in these files",
    color: "text-blue-600 dark:text-blue-400",
  },
  result: {
    label: "Ungrouped Queries",
    description: "Standalone SELECT queries with no INSERT INTO target",
    color: "text-muted-foreground",
  },
};

const ROLE_ORDER = ["target", "intermediate", "source", "result"];

const ROLE_DOT: Record<string, string> = {
  target: "bg-green-500",
  intermediate: "bg-amber-500",
  source: "bg-blue-500",
  result: "bg-gray-400",
};

function groupByRole(tables: TableSummary[]): Record<string, TableSummary[]> {
  const groups: Record<string, TableSummary[]> = {};
  for (const t of tables) {
    const role = t.role || "source";
    if (!groups[role]) groups[role] = [];
    groups[role].push(t);
  }
  return groups;
}

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

  // Find the role of the selected table for display
  const selectedRole = tables?.find((t) => t.table === selectedTable)?.role;

  return (
    <div className="flex gap-6 h-[calc(100vh-120px)]">
      {/* Sidebar */}
      <div className="w-64 flex-shrink-0 space-y-2">
        <Input
          placeholder="Search tables…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="h-8 text-sm"
        />
        <div className="text-xs text-muted-foreground uppercase tracking-wide px-1">
          {isLoading ? "Loading…" : `${filtered.length} tables`}
        </div>
        <div className="space-y-3 overflow-y-auto max-h-[calc(100vh-200px)]">
          {ROLE_ORDER.map((role) => {
            const group = grouped[role];
            if (!group || group.length === 0) return null;
            const config = ROLE_CONFIG[role];
            return (
              <div key={role}>
                <div className={`px-2 py-1 text-xs font-semibold uppercase tracking-wider ${config.color}`}>
                  {config.label} ({group.length})
                </div>
                <div className="space-y-0.5 mt-0.5">
                  {group.map((t) => (
                    <button
                      key={t.table}
                      onClick={() => setSelectedTable(t.table)}
                      className={`w-full text-left px-2 py-1.5 rounded text-sm transition-colors flex items-center gap-2 ${
                        selectedTable === t.table
                          ? "bg-accent text-accent-foreground font-medium"
                          : "hover:bg-muted text-muted-foreground hover:text-foreground"
                      }`}
                    >
                      <span className={`w-2 h-2 rounded-full flex-shrink-0 ${ROLE_DOT[role]}`} />
                      <span className="truncate">{t.table}</span>
                      <span className="ml-auto text-xs text-muted-foreground flex-shrink-0">
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
      <div className="flex-1 overflow-auto">
        {!selectedTable && (
          <div className="mt-4 space-y-4">
            <p className="text-sm text-muted-foreground">Select a table to view its columns and lineage.</p>
            {/* Summary cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {ROLE_ORDER.map((role) => {
                const count = grouped[role]?.length ?? 0;
                const config = ROLE_CONFIG[role];
                return (
                  <div key={role} className="border rounded-lg p-3">
                    <div className={`text-2xl font-bold ${config.color}`}>{count}</div>
                    <div className="text-xs font-medium mt-1">{config.label}</div>
                    <div className="text-xs text-muted-foreground mt-0.5">{config.description}</div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
        {selectedTable && (
          <>
            <div className="flex items-center gap-3 mb-4">
              <span className={`w-2.5 h-2.5 rounded-full ${ROLE_DOT[selectedRole || "source"]}`} />
              <h2 className="text-lg font-semibold">{selectedTable}</h2>
              <span className={`text-xs font-medium uppercase ${ROLE_CONFIG[selectedRole || "source"]?.color}`}>
                {selectedRole}
              </span>
            </div>
            {colsLoading && <p className="text-sm text-muted-foreground">Loading columns…</p>}
            {columns && (
              <table className="w-full text-sm border-collapse">
                <thead>
                  <tr className="border-b text-xs text-muted-foreground uppercase tracking-wide">
                    <th className="text-left py-2 px-3 font-medium">Column</th>
                    <th className="text-left py-2 px-3 font-medium">Source Table</th>
                    <th className="text-left py-2 px-3 font-medium">Transform</th>
                    <th className="text-left py-2 px-3 font-medium">Defined In</th>
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
