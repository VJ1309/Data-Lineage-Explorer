"use client";
import { useState, useRef, useCallback } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import { Copy } from "lucide-react";
import { TransformBadge } from "./transform-badge";
import type { LineagePath, PathStep } from "@/lib/api";

type Props = {
  paths: LineagePath[];
  truncated: boolean;
  isLoading: boolean;
  isError: boolean;
  errorMessage?: string;
};

const VERB_MAP: Record<string, string> = {
  aggregation: "Aggregated",
  expression: "Derived by expression",
  window: "Computed (window)",
  cast: "Cast",
  join_key: "Used as join key",
  filter: "Filtered",
  passthrough: "Passed through unchanged",
};

function colLabel(colId: string) {
  const dot = colId.lastIndexOf(".");
  const col = dot === -1 ? colId : colId.slice(dot + 1);
  const tbl = dot === -1 ? "" : colId.slice(0, dot);
  return { col, tbl, full: colId };
}

function detectLang(sourceFile: string | null): string {
  if (!sourceFile) return "sql";
  return sourceFile.endsWith(".sql") ? "sql" : "python";
}

function deriveSummary(steps: PathStep[]) {
  const dominant = [...steps].reverse().find((s) => s.transform_type !== "passthrough") ?? steps[steps.length - 1];
  const src = colLabel(steps[0].source_col);
  const hops = steps.length;
  const verb = VERB_MAP[dominant?.transform_type ?? "passthrough"] ?? "Transformed";
  const rawFile = dominant?.source_file ?? null;
  const file = rawFile ? rawFile.split(/[\\/]/).at(-1) ?? null : null;
  return { verb, source: src, hops, file };
}

function computePillLabels(paths: LineagePath[]): string[] {
  const rawLabels = paths.map((p) => {
    const tbl = colLabel(p.steps[0]?.source_col ?? "").tbl;
    return tbl ? `via ${tbl}` : "via (unknown)";
  });
  const counts: Record<string, number> = {};
  const seen: Record<string, number> = {};
  for (const l of rawLabels) counts[l] = (counts[l] ?? 0) + 1;
  return rawLabels.map((l) => {
    if (counts[l] === 1) return l;
    seen[l] = (seen[l] ?? 0) + 1;
    return `${l} (${seen[l]})`;
  });
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);
  return (
    <button
      onClick={handleCopy}
      title={copied ? "Copied!" : "Copy expression"}
      className="absolute top-1.5 right-1.5 p-1 rounded opacity-0 group-hover:opacity-100 transition-opacity bg-background/60 hover:bg-background/90"
    >
      <Copy className="h-3 w-3 text-muted-foreground" />
    </button>
  );
}

function HopCard({
  step,
  isLast,
  allExpanded,
}: {
  step: PathStep;
  isLast: boolean;
  allExpanded: boolean;
}) {
  const src = colLabel(step.source_col);
  const tgt = colLabel(step.target_col);
  const isPassthrough = step.transform_type === "passthrough";
  const isNullData = !step.expression || step.expression === "*";
  const hasExpr = !isPassthrough && !isNullData;

  const fileParts: string[] = [];
  if (step.source_file) fileParts.push(step.source_file.split(/[\\/]/).at(-1)!);
  if (step.source_cell != null) fileParts.push(`cell ${step.source_cell}`);
  if (step.source_line != null) fileParts.push(`line ${step.source_line}`);
  const fileRef = fileParts.join(" · ");

  return (
    <div className="flex flex-col items-start gap-0">
      {/* Source column card */}
      <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-border bg-card min-w-[220px] md:min-w-[260px] w-full md:w-auto">
        <div>
          <div className="text-xs text-muted-foreground font-mono">{src.tbl}</div>
          <div className="font-medium text-sm">{src.col}</div>
        </div>
      </div>

      {/* Connector */}
      <div className="flex items-start gap-2 pl-4 py-1">
        <div className="flex flex-col items-center flex-shrink-0">
          <div className="w-px h-3 bg-border" />
          <div className="text-muted-foreground text-xs">↓</div>
          <div className="w-px h-3 bg-border" />
        </div>
        <div className="flex flex-col gap-1 pt-1 min-w-0">
          <div className="flex items-center gap-1.5 flex-wrap">
            <TransformBadge type={step.transform_type ?? "passthrough"} />
            {fileRef && (
              <span className="text-xs text-muted-foreground">{fileRef}</span>
            )}
          </div>

          {isPassthrough ? (
            <p className="text-xs text-muted-foreground italic">
              No transformation — passed through unchanged
            </p>
          ) : isNullData ? (
            <p className="text-xs text-muted-foreground italic">
              Structural hop — no expression recorded
            </p>
          ) : (
            allExpanded && (
              <div
                className="relative group rounded overflow-hidden mt-1 max-w-[480px] max-h-28 overflow-y-auto"
              >
                <SyntaxHighlighter
                  language={detectLang(step.source_file)}
                  style={vscDarkPlus}
                  customStyle={{ margin: 0, borderRadius: 6, fontSize: 11, padding: "8px 12px" }}
                >
                  {step.expression!}
                </SyntaxHighlighter>
                <CopyButton text={step.expression!} />
              </div>
            )
          )}
        </div>
      </div>

      {/* Target column card — only on last hop */}
      {isLast && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-primary/40 bg-primary/5 min-w-[220px] md:min-w-[260px] w-full md:w-auto">
          <div>
            <div className="text-xs text-muted-foreground font-mono">{tgt.tbl}</div>
            <div className="font-medium text-sm text-primary">{tgt.col}</div>
          </div>
        </div>
      )}
    </div>
  );
}

export function TransformInspector({
  paths,
  truncated,
  isLoading,
  isError,
  errorMessage,
}: Props) {
  const [selectedPath, setSelectedPath] = useState(0);
  const [allExpanded, setAllExpanded] = useState(true);
  const pillRefs = useRef<(HTMLButtonElement | null)[]>([]);

  if (isLoading) {
    return <p className="text-sm text-muted-foreground">Loading…</p>;
  }
  if (isError) {
    return (
      <p className="text-sm text-destructive">
        Error: {errorMessage ?? "Failed to load transformation paths"}
      </p>
    );
  }
  if (paths.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">
        This is a source column — no upstream transformations.
      </p>
    );
  }

  const steps = paths[selectedPath]?.steps ?? [];
  const summary = steps.length > 0 ? deriveSummary(steps) : null;
  const pillLabels = computePillLabels(paths);

  const handlePillKeyDown = (e: React.KeyboardEvent, idx: number) => {
    if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
      e.preventDefault();
      const next = e.key === "ArrowRight"
        ? (idx + 1) % paths.length
        : (idx - 1 + paths.length) % paths.length;
      setSelectedPath(next);
      pillRefs.current[next]?.focus();
    }
  };

  return (
    <div className="space-y-4">
      {/* Summary banner */}
      {summary && (
        <div className="rounded-lg border border-border bg-card px-4 py-3">
          <p className="text-sm font-medium text-foreground">
            {summary.verb} from{" "}
            <span className="font-mono text-xs">
              {summary.source.tbl}.{summary.source.col}
            </span>
          </p>
          <p className="text-xs text-muted-foreground mt-0.5">
            {summary.hops} {summary.hops === 1 ? "hop" : "hops"}
            {summary.file ? ` · ${summary.file}` : ""}
          </p>
        </div>
      )}

      {/* Path selector pills */}
      {paths.length > 1 && (
        <div
          role="radiogroup"
          aria-label="Transformation paths"
          className="flex items-center gap-1.5 flex-wrap md:flex-wrap overflow-x-auto"
        >
          {pillLabels.map((label, i) => (
            <button
              key={i}
              ref={(el) => { pillRefs.current[i] = el; }}
              role="radio"
              aria-checked={selectedPath === i}
              tabIndex={selectedPath === i ? 0 : -1}
              onClick={() => setSelectedPath(i)}
              onKeyDown={(e) => handlePillKeyDown(e, i)}
              className={`text-xs px-2 py-1 rounded border transition-colors whitespace-nowrap ${
                selectedPath === i
                  ? "bg-accent text-accent-foreground border-accent"
                  : "text-muted-foreground border-transparent hover:text-foreground hover:border-border"
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      )}

      {/* Truncation note */}
      {truncated && (
        <p className="text-xs text-muted-foreground">
          Showing {paths.length} path{paths.length !== 1 ? "s" : ""} — more paths may exist.
        </p>
      )}

      {/* Chain header with collapse toggle */}
      <div className="flex items-center justify-between">
        <span className="text-xs text-muted-foreground">Transformation chain</span>
        <button
          onClick={() => setAllExpanded((v) => !v)}
          className="text-xs text-muted-foreground hover:text-foreground transition-colors"
        >
          {allExpanded ? "Collapse all" : "Expand all"}
        </button>
      </div>

      {/* Hop chain */}
      <div className="flex flex-col gap-0">
        {steps.map((step, i) => (
          <HopCard
            key={i}
            step={step}
            isLast={i === steps.length - 1}
            allExpanded={allExpanded}
          />
        ))}
      </div>
    </div>
  );
}
