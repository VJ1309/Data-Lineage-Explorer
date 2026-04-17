"use client";
import { useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import { TransformBadge } from "./transform-badge";
import type { LineagePath, PathStep } from "@/lib/api";

type Props = {
  paths: LineagePath[];
  truncated: boolean;
};

function colLabel(colId: string) {
  const parts = colId.split(".");
  const col = parts.at(-1) ?? colId;
  const tbl = parts.at(-2) ?? "";
  return { col, tbl, full: colId };
}

function StepArrow({ step, isLast }: { step: PathStep; isLast: boolean }) {
  const [open, setOpen] = useState(false);
  const hasExpr = !!step.expression && step.expression !== "*" && step.transform_type !== "passthrough";
  const src = colLabel(step.source_col);
  const tgt = colLabel(step.target_col);

  return (
    <div className="flex flex-col items-start gap-0">
      {/* Source node */}
      <div className="flex items-center gap-2 px-3 py-2 rounded-lg border bg-card min-w-[220px]">
        <div>
          <div className="text-xs text-muted-foreground font-mono">{src.tbl}</div>
          <div className="font-medium text-sm">{src.col}</div>
        </div>
      </div>

      {/* Arrow + transform */}
      <div className="flex items-start gap-2 pl-4 py-1">
        <div className="flex flex-col items-center">
          <div className="w-px h-3 bg-border" />
          <div className="text-muted-foreground text-xs">↓</div>
          <div className="w-px h-3 bg-border" />
        </div>
        <div className="flex flex-col gap-0.5 pt-1">
          <div className="flex items-center gap-1.5">
            <TransformBadge type={step.transform_type ?? "passthrough"} />
            {hasExpr && (
              <button
                onClick={() => setOpen((v) => !v)}
                className="text-xs text-muted-foreground hover:text-foreground transition-colors"
                title="Toggle expression"
              >
                {open ? "▴" : "▾"}
              </button>
            )}
            {step.source_file && (
              <span className="text-xs text-muted-foreground">
                {step.source_file.split(/[\\/]/).at(-1)}
                {step.source_cell != null ? ` · cell ${step.source_cell}` : ""}
              </span>
            )}
          </div>
          {open && hasExpr && (
            <div className="rounded overflow-hidden mt-1 max-w-[480px]">
              <SyntaxHighlighter
                language={step.source_file?.endsWith(".sql") ? "sql" : "python"}
                style={vscDarkPlus}
                customStyle={{ margin: 0, borderRadius: 6, fontSize: 11, padding: "8px 12px" }}
              >
                {step.expression!}
              </SyntaxHighlighter>
            </div>
          )}
        </div>
      </div>

      {/* Target node shown only for last step */}
      {isLast && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-primary/40 bg-primary/5 min-w-[220px]">
          <div>
            <div className="text-xs text-muted-foreground font-mono">{tgt.tbl}</div>
            <div className="font-medium text-sm text-primary">{tgt.col}</div>
          </div>
        </div>
      )}
    </div>
  );
}

function PathChain({ path }: { path: LineagePath }) {
  if (path.steps.length === 0) {
    return <p className="text-sm text-muted-foreground">No steps in this path.</p>;
  }
  return (
    <div className="flex flex-col gap-0">
      {path.steps.map((step, i) => (
        <StepArrow key={i} step={step} isLast={i === path.steps.length - 1} />
      ))}
    </div>
  );
}

export function PathInspector({ paths, truncated }: Props) {
  const [selectedPath, setSelectedPath] = useState(0);

  if (paths.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">
        No transformation paths found. This column may come directly from a source table.
      </p>
    );
  }

  return (
    <div className="space-y-4">
      {paths.length > 1 && (
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-muted-foreground">
            {paths.length} path{paths.length !== 1 ? "s" : ""}{truncated ? " (truncated)" : ""}:
          </span>
          {paths.map((p, i) => {
            const src = colLabel(p.steps[0]?.source_col ?? "");
            return (
              <button
                key={i}
                onClick={() => setSelectedPath(i)}
                className={`text-xs px-2 py-1 rounded border transition-colors ${
                  selectedPath === i
                    ? "bg-accent text-accent-foreground border-accent"
                    : "text-muted-foreground hover:text-foreground border-transparent hover:border-border"
                }`}
              >
                via {src.tbl || src.col}
              </button>
            );
          })}
        </div>
      )}
      <PathChain path={paths[selectedPath]} />
    </div>
  );
}
