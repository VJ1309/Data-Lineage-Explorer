"use client";
import { useCallback, useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import { Check, Copy, GitBranch } from "lucide-react";
import { TransformBadge } from "./transform-badge";
import { LineageTrace } from "./lineage-trace";
import type { LineageEdge } from "@/lib/api";
import { splitColumnId } from "@/lib/utils";

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-3 pb-1 border-b border-border/60">
      <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-foreground/85">
        {children}
      </p>
      <div className="flex-1 h-px bg-border/30" />
    </div>
  );
}

function CopyExpression({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const onCopy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1400);
    });
  }, [text]);
  return (
    <button
      type="button"
      onClick={onCopy}
      title={copied ? "Copied" : "Copy expression"}
      aria-label={copied ? "Copied" : "Copy expression"}
      className="absolute top-2 right-2 inline-flex items-center gap-1 rounded-md border border-border/60 bg-background/70 px-1.5 py-1 text-[11px] font-medium text-muted-foreground opacity-0 group-hover:opacity-100 focus:opacity-100 hover:text-foreground hover:bg-background/90 transition-all"
    >
      {copied ? (
        <>
          <Check className="h-3 w-3 text-green-400" />
          <span className="text-green-400">Copied</span>
        </>
      ) : (
        <>
          <Copy className="h-3 w-3" />
          <span>Copy</span>
        </>
      )}
    </button>
  );
}

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
    <div className="space-y-7 max-w-3xl">
      {/* Column header */}
      <div>
        <p className="text-sm font-mono text-muted-foreground mb-1">{table}</p>
        <h2 className="font-mono font-semibold text-2xl text-foreground tracking-tight">{col}</h2>
      </div>

      {incoming.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          Source column — no upstream transformations recorded.
        </p>
      ) : (
        <>
          {/* SQL Logic */}
          <section className="space-y-3">
            <SectionLabel>SQL Logic</SectionLabel>
            {withExpression.length > 0 ? (
              withExpression.map((e, i) => (
                <div key={i} className="space-y-1.5">
                  {e.source_file && (
                    <p className="text-xs font-mono text-muted-foreground">
                      {e.source_file.split(/[\\/]/).at(-1)}
                      {e.source_line != null ? (
                        <span className="text-muted-foreground/70"> · line {e.source_line}</span>
                      ) : null}
                    </p>
                  )}
                  <div className="relative group rounded-md overflow-hidden ring-1 ring-border/50 max-h-56 overflow-y-auto bg-[#1e1e1e]">
                    <SyntaxHighlighter
                      language={detectLang(e.source_file)}
                      style={vscDarkPlus}
                      customStyle={{ margin: 0, fontSize: 13, padding: "12px 16px", lineHeight: "1.65" }}
                    >
                      {e.expression!}
                    </SyntaxHighlighter>
                    <CopyExpression text={e.expression!} />
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
          <section className="space-y-3">
            <SectionLabel>Column Transformations</SectionLabel>
            <div className="rounded-md border border-border overflow-hidden bg-card/40">
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
                    className={`grid grid-cols-[minmax(140px,1fr)_auto_minmax(0,2fr)] items-center gap-3 px-3.5 py-3 text-sm transition-colors hover:bg-muted/30${
                      i > 0 ? " border-t border-border/70" : ""
                    }`}
                  >
                    <span
                      className="font-mono text-foreground truncate"
                      title={e.source_col}
                    >
                      <span className="text-muted-foreground">{srcTblShort}.</span>
                      {srcCol}
                    </span>
                    <TransformBadge type={displayType} />
                    <span
                      className="font-mono text-foreground truncate"
                      title={logic ?? "passthrough"}
                    >
                      {logic ?? (
                        <span className="italic text-muted-foreground">passthrough</span>
                      )}
                    </span>
                  </div>
                );
              })}
            </div>
          </section>

          {/* Lineage Trace */}
          <section className="space-y-3">
            <SectionLabel>Lineage Trace</SectionLabel>
            <LineageTrace colId={colId} />
          </section>
        </>
      )}
    </div>
  );
}
