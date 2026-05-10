"use client";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import { GitBranch } from "lucide-react";
import { TransformBadge } from "./transform-badge";
import { LineageTrace } from "./lineage-trace";
import type { LineageEdge } from "@/lib/api";
import { splitColumnId } from "@/lib/utils";

function detectLang(file: string | null): string {
  if (!file) return "sql";
  return file.endsWith(".sql") ? "sql" : "python";
}

const TRANSFORM_PRIORITY: Record<string, number> = {
  window: 5, cast: 4, aggregation: 3, expression: 2, passthrough: 1,
};

function bestPredecessor(sourceCol: string, allEdges: LineageEdge[]): LineageEdge | undefined {
  const preds = allEdges.filter((e) => e.target_col === sourceCol);
  if (!preds.length) return undefined;
  return preds.reduce((best, e) =>
    (TRANSFORM_PRIORITY[e.transform_type ?? "passthrough"] ?? 0) >
    (TRANSFORM_PRIORITY[best.transform_type ?? "passthrough"] ?? 0)
      ? e
      : best
  );
}

export function ColumnInspector({
  colId,
  edges,
}: {
  colId: string | null;
  edges: LineageEdge[];
}) {
  if (!colId) {
    return (
      <div className="flex flex-col items-center justify-center h-[40vh] gap-4 text-center">
        <div className="flex h-14 w-14 items-center justify-center rounded-2xl border border-border bg-card text-muted-foreground">
          <GitBranch className="h-6 w-6" />
        </div>
        <div className="space-y-1.5">
          <p className="text-sm font-medium text-foreground">No column selected</p>
          <p className="text-sm text-muted-foreground max-w-[280px]">
            Click any column node in the Graph to trace its SQL logic and transformations.
          </p>
        </div>
      </div>
    );
  }

  const [table, col] = splitColumnId(colId);

  const incoming = edges.filter((e) => e.target_col === colId);

  const seen = new Set<string>();
  const withExpression = incoming
    .filter((e) => e.expression && e.expression !== "*")
    .filter((e) => {
      const key = `${e.expression}|${e.source_file ?? ""}|${e.source_line ?? ""}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

  return (
    <div className="space-y-6 max-w-2xl">
      {/* Column header */}
      <div>
        <p className="text-xs font-mono text-muted-foreground/60 mb-0.5">{table}</p>
        <h2 className="font-mono font-semibold text-xl text-foreground">{col}</h2>
      </div>

      {incoming.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          Source column — no upstream transformations recorded.
        </p>
      ) : (
        <>
          {/* SQL Logic */}
          <section className="space-y-2.5">
            <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
              SQL Logic
            </p>
            {withExpression.length > 0 ? (
              withExpression.map((e, i) => (
                <div key={i} className="space-y-1">
                  {e.source_file && (
                    <p className="text-[10px] font-mono text-muted-foreground/50">
                      {e.source_file.split(/[\\/]/).at(-1)}
                      {e.source_line != null ? ` · line ${e.source_line}` : ""}
                    </p>
                  )}
                  <div className="rounded-md overflow-hidden ring-1 ring-border/40 max-h-48 overflow-y-auto">
                    <SyntaxHighlighter
                      language={detectLang(e.source_file)}
                      style={vscDarkPlus}
                      customStyle={{ margin: 0, fontSize: 12, padding: "10px 14px", lineHeight: "1.6" }}
                    >
                      {e.expression!}
                    </SyntaxHighlighter>
                  </div>
                </div>
              ))
            ) : (
              <p className="text-sm text-muted-foreground italic">
                Passed through unchanged — no expression recorded.
              </p>
            )}
          </section>

          {/* Column Transformations */}
          <section className="space-y-2.5">
            <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
              Column Transformations
            </p>
            <div className="rounded-md border border-border overflow-hidden">
              {incoming.map((e, i) => {
                const [srcTbl, srcCol] = splitColumnId(e.source_col);
                const srcTblShort = srcTbl.split(".").at(-1) || srcTbl;

                // For intermediate source columns, show how they were produced (one hop back).
                // For base-table sources, fall back to the direct incoming edge.
                const pred = bestPredecessor(e.source_col, edges);
                const displayEdge = pred ?? e;
                const displayType = displayEdge.transform_type ?? "passthrough";
                const logic =
                  displayEdge.expression && displayEdge.expression !== "*"
                    ? displayEdge.expression
                    : null;

                return (
                  <div
                    key={i}
                    className={`grid grid-cols-[minmax(100px,1fr)_auto_minmax(0,2fr)] items-center gap-3 px-3 py-2.5 text-xs${
                      i > 0 ? " border-t border-border" : ""
                    }`}
                  >
                    <span
                      className="font-mono text-muted-foreground truncate"
                      title={e.source_col}
                    >
                      <span className="text-muted-foreground/50">{srcTblShort}.</span>
                      {srcCol}
                    </span>
                    <TransformBadge type={displayType} />
                    <span
                      className="font-mono text-foreground/70 truncate"
                      title={logic ?? "passthrough"}
                    >
                      {logic ?? (
                        <span className="italic text-muted-foreground/50">passthrough</span>
                      )}
                    </span>
                  </div>
                );
              })}
            </div>
          </section>

          {/* Lineage Trace */}
          <section className="space-y-2.5">
            <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
              Lineage Trace
            </p>
            <LineageTrace colId={colId} />
          </section>
        </>
      )}
    </div>
  );
}
