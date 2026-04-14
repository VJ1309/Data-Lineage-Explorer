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
