"use client";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import { GitBranch } from "lucide-react";
import { TransformBadge } from "./transform-badge";
import type { LineageEdge } from "@/lib/api";

function detectLang(file: string | null): string {
  if (!file) return "sql";
  return file.endsWith(".sql") ? "sql" : "python";
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

  const dot = colId.lastIndexOf(".");
  const col = dot === -1 ? colId : colId.slice(dot + 1);
  const table = dot === -1 ? "" : colId.slice(0, dot);

  const incoming = edges.filter((e) => e.target_col === colId);

  const withExpression = incoming.filter(
    (e) => e.expression && e.expression !== "*"
  );

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
                const srcDot = e.source_col.lastIndexOf(".");
                const srcCol = srcDot === -1 ? e.source_col : e.source_col.slice(srcDot + 1);
                const srcTbl = srcDot === -1 ? "" : e.source_col.slice(0, srcDot);
                const srcTblShort = srcTbl.split(".").at(-1) || srcTbl;
                const logic =
                  e.expression && e.expression !== "*" ? e.expression : null;

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
                    <TransformBadge type={e.transform_type ?? "passthrough"} />
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
        </>
      )}
    </div>
  );
}
