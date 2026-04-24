"use client";
import { Suspense, useState } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { useTables, useColumns, useImpact } from "@/lib/hooks";
import { TransformBadge } from "@/components/transform-badge";
import { Button } from "@/components/ui/button";
import { ChevronDown } from "lucide-react";

function StyledSelect({
  value,
  onChange,
  disabled,
  children,
}: {
  value: string;
  onChange: (v: string) => void;
  disabled?: boolean;
  children: React.ReactNode;
}) {
  return (
    <div className="relative">
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
        className="h-9 w-full appearance-none rounded-md border border-border bg-secondary px-3 pr-8 text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-ring/50 focus:border-ring disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer"
      >
        {children}
      </select>
      <ChevronDown className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
    </div>
  );
}

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
      <div>
        <h1 className="text-xl font-semibold tracking-tight">Impact Analyzer</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Trace all downstream columns affected when a source column changes.
        </p>
      </div>

      <div className="flex gap-3 items-end rounded-lg border border-border bg-card p-4">
        <div className="flex-1 space-y-1.5">
          <label className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">Table</label>
          <StyledSelect value={table} onChange={(v) => { setTable(v); setColumn(""); }}>
            <option value="">— select table —</option>
            {tables?.map((t) => <option key={t.table} value={t.table}>{t.table}</option>)}
          </StyledSelect>
        </div>
        <div className="flex-1 space-y-1.5">
          <label className="text-[10px] font-semibold text-muted-foreground uppercase tracking-widest">Column</label>
          <StyledSelect value={column} onChange={setColumn} disabled={!table}>
            <option value="">— select column —</option>
            {columns?.map((c) => <option key={c.column} value={c.column}>{c.column}</option>)}
          </StyledSelect>
        </div>
        <Button onClick={handleApply} disabled={!table || !column} className="shrink-0">
          Analyze
        </Button>
      </div>

      {isLoading && <p className="text-sm text-muted-foreground">Analyzing…</p>}
      {error && <p className="text-sm text-destructive">Error: {(error as Error).message}</p>}

      {data && (
        <>
          <div className="rounded-md border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-300">
            If <span className="font-mono font-medium text-amber-200">{column}</span> in{" "}
            <span className="font-mono font-medium text-amber-200">{table.split(".").pop()}</span> changes,{" "}
            <strong>{data.affected_count} downstream column{data.affected_count !== 1 ? "s" : ""}</strong>{" "}
            {data.affected_count !== 1 ? "are" : "is"} affected.
          </div>

          <div className="space-y-1.5">
            {data.downstream.length === 0 && (
              <p className="text-sm text-muted-foreground">No downstream dependents found.</p>
            )}
            {data.downstream.map((edge, i) => (
              <div
                key={i}
                className="flex items-center gap-3 rounded-md border border-border bg-card px-3 py-2 text-sm"
                style={{ marginLeft: `${Math.min(i, 4) * 20}px` }}
              >
                <span className="text-amber-500 text-xs font-mono">↓</span>
                <span className="font-mono text-xs font-medium text-foreground">{edge.target_col}</span>
                <TransformBadge type={edge.transform_type} />
                {edge.expression && (
                  <span className="text-xs text-muted-foreground truncate font-mono">{edge.expression}</span>
                )}
                <span className="ml-auto text-xs text-muted-foreground font-mono shrink-0">
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
