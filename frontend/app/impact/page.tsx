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
