"use client";
import { useState } from "react";
import { ChevronRight, FileText, Filter, Link2, Loader2, AlertCircle } from "lucide-react";
import { useLineageTrace } from "@/lib/hooks";
import { splitColumnId } from "@/lib/utils";
import type { TraceStep } from "@/lib/api";

const MAX_VISIBLE_TEMP_VIEWS = 3;

function shortFile(path: string): string {
  return path.split(/[\\/]/).at(-1) ?? path;
}

function FilterIcon() {
  return <Filter className="h-3 w-3 text-rose-400" aria-hidden />;
}

function JoinIcon() {
  return <Link2 className="h-3 w-3 text-blue-400" aria-hidden />;
}

function FilePill({ file, line }: { file: string; line: number | null }) {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-md border border-border/60 bg-muted/50 px-2 py-1 font-mono text-xs text-muted-foreground">
      <FileText className="h-3.5 w-3.5" aria-hidden />
      <span>{shortFile(file)}</span>
      {line != null && <span className="text-muted-foreground/80">·{line}</span>}
    </span>
  );
}

function ViaTempViewBadge({ names }: { names: string[] }) {
  if (names.length === 0) return null;
  const visible = names.slice(0, MAX_VISIBLE_TEMP_VIEWS);
  const overflow = names.length - visible.length;
  return (
    <span
      className="inline-flex items-center rounded-full border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 text-xs font-medium text-amber-300"
      title={`Predicates rolled up via: ${names.join(", ")}`}
    >
      via {visible.join(", ")}
      {overflow > 0 && ` … +${overflow}`}
    </span>
  );
}

function StepCard({
  step,
  selectedColId,
  onExpand,
  expandedCol,
}: {
  step: TraceStep;
  selectedColId: string;
  onExpand: (colId: string) => void;
  expandedCol: string | null;
}) {
  const [, selectedColName] = splitColumnId(selectedColId);
  const tracedWrites = step.writes.filter((w) => w.column_id === selectedColId);
  const otherWrites = step.writes.filter((w) => w.column_id !== selectedColId);
  const [showAllWrites, setShowAllWrites] = useState(false);

  const hasContent =
    step.filters.length > 0 || step.joins.length > 0 || tracedWrites.length > 0;

  return (
    <article className="rounded-md border border-border/70 bg-card/60 p-3.5 space-y-3">
      {/* Header: target table, file:line, via temp views */}
      <header className="flex items-center justify-between gap-2 flex-wrap">
        <div className="flex items-center gap-2">
          <span className="font-mono text-sm font-semibold text-foreground">
            {step.target_table}
          </span>
          {step.kind === "pyspark" && (
            <span className="rounded border border-border/60 bg-muted/40 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
              PySpark
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5">
          <ViaTempViewBadge names={step.via_temp_views} />
          <FilePill file={step.source_file} line={step.source_line} />
        </div>
      </header>

      {!hasContent && (
        <p className="text-xs italic text-muted-foreground">
          No filter or join recorded for this step.
        </p>
      )}

      {/* Traced column writes (the column being inspected) */}
      {tracedWrites.length > 0 && (
        <div className="space-y-1.5">
          {tracedWrites.map((w, i) => (
            <div
              key={`tw-${i}`}
              className="rounded bg-muted/40 px-2.5 py-2 text-xs font-mono text-foreground leading-relaxed"
            >
              <span className="text-foreground font-medium">{selectedColName}</span>
              {w.expression && w.expression !== "*" && (
                <span className="text-muted-foreground"> · </span>
              )}
              {w.expression && w.expression !== "*" && (
                <span className="break-all text-foreground/95">{w.expression}</span>
              )}
            </div>
          ))}
          {otherWrites.length > 0 && (
            <button
              type="button"
              onClick={() => setShowAllWrites((s) => !s)}
              className="text-xs text-muted-foreground hover:text-foreground transition-colors"
            >
              {showAllWrites
                ? `Hide ${otherWrites.length} other write${otherWrites.length === 1 ? "" : "s"}`
                : `Show ${otherWrites.length} other write${otherWrites.length === 1 ? "" : "s"} from this step`}
            </button>
          )}
          {showAllWrites &&
            otherWrites.map((w, i) => {
              const [, c] = splitColumnId(w.column_id);
              return (
                <div
                  key={`ow-${i}`}
                  className="rounded bg-muted/20 px-2.5 py-1.5 text-xs font-mono text-muted-foreground leading-relaxed"
                >
                  <span>{c}</span>
                  {w.expression && w.expression !== "*" && (
                    <>
                      <span className="text-muted-foreground/70"> · </span>
                      <span className="break-all">{w.expression}</span>
                    </>
                  )}
                </div>
              );
            })}
        </div>
      )}

      {/* Filters */}
      {step.filters.length > 0 && (
        <div className="space-y-1.5">
          {step.filters.map((f, i) => (
            <div
              key={`f-${i}`}
              className="flex items-start gap-2.5 rounded bg-rose-500/8 px-2.5 py-2 text-xs"
            >
              <span className="mt-0.5">
                <FilterIcon />
              </span>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-1.5">
                  <span className="text-[10px] uppercase tracking-wider text-rose-300 font-medium">
                    {f.kind}
                  </span>
                </div>
                <div className="font-mono text-foreground break-all leading-relaxed">
                  {f.expression ?? <span className="italic text-muted-foreground">(predicate)</span>}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Joins */}
      {step.joins.length > 0 && (
        <div className="space-y-1.5">
          {step.joins.map((j, i) => (
            <div
              key={`j-${i}`}
              className="flex items-start gap-2.5 rounded bg-blue-500/8 px-2.5 py-2 text-xs"
            >
              <span className="mt-0.5">
                <JoinIcon />
              </span>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-1.5">
                  <span className="text-[10px] uppercase tracking-wider text-blue-300 font-medium">
                    join on
                  </span>
                </div>
                <div className="font-mono text-foreground break-all leading-relaxed">
                  {j.expression ?? <span className="italic text-muted-foreground">(join key)</span>}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Upstream chips */}
      {step.upstream_columns.length > 0 && (
        <footer className="pt-2 space-y-2 border-t border-border/50">
          <p className="text-[10px] uppercase tracking-[0.14em] text-muted-foreground font-medium">
            Upstream
          </p>
          <div className="flex flex-wrap gap-1.5">
            {step.upstream_columns.map((up) => {
              const [upTbl, upCol] = splitColumnId(up);
              const upTblShort = upTbl.split(".").at(-1) ?? upTbl;
              const isExpanded = expandedCol === up;
              return (
                <div key={up} className="flex flex-col gap-1.5">
                  <button
                    type="button"
                    onClick={() => onExpand(up)}
                    aria-expanded={isExpanded}
                    className={`inline-flex items-center gap-1 rounded-md border px-2.5 py-1.5 text-xs font-mono transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-1 focus:ring-offset-background ${
                      isExpanded
                        ? "border-primary/60 bg-primary/10 text-foreground"
                        : "border-border/70 bg-muted/40 text-foreground/90 hover:text-foreground hover:bg-muted/60 hover:border-border"
                    }`}
                  >
                    <ChevronRight
                      className={`h-3 w-3 transition-transform ${
                        isExpanded ? "rotate-90" : ""
                      }`}
                    />
                    <span className="text-muted-foreground">{upTblShort}.</span>
                    <span>{upCol}</span>
                  </button>
                  {isExpanded && (
                    <div className="ml-3 border-l border-border/40 pl-3">
                      <LineageTrace colId={up} />
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </footer>
      )}
    </article>
  );
}

export function LineageTrace({ colId }: { colId: string }) {
  const [table, column] = splitColumnId(colId);
  const [expandedCol, setExpandedCol] = useState<string | null>(null);
  const { data, isLoading, isError, error } = useLineageTrace(table, column);

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
        <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
        <span>Loading trace…</span>
      </div>
    );
  }

  if (isError) {
    return (
      <div className="flex items-start gap-2 rounded-md border border-rose-500/30 bg-rose-500/5 px-3 py-2 text-sm">
        <AlertCircle className="h-3.5 w-3.5 mt-0.5 text-rose-400" aria-hidden />
        <span className="text-rose-200">
          {(error as Error)?.message ?? "Failed to load trace"}
        </span>
      </div>
    );
  }

  if (!data || data.steps.length === 0) {
    return (
      <p className="text-sm italic text-muted-foreground">
        Source column — end of trace.
      </p>
    );
  }

  return (
    <div className="space-y-2.5">
      {data.steps.length > 1 && (
        <div className="rounded border border-amber-500/40 bg-amber-500/8 px-3 py-2 text-xs text-amber-200 font-medium">
          {data.steps.length} writers · all scopes shown
        </div>
      )}
      {data.steps.map((step, i) => (
        <StepCard
          key={`${step.source_file}:${step.source_line}:${i}`}
          step={step}
          selectedColId={colId}
          onExpand={(up) => setExpandedCol((curr) => (curr === up ? null : up))}
          expandedCol={expandedCol}
        />
      ))}
    </div>
  );
}
