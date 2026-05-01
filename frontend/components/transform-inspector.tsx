"use client";
import { useState, useRef, useCallback, useMemo } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import { Copy, Search, GitBranch } from "lucide-react";
import { TransformBadge } from "./transform-badge";
import type { LineagePath, PathStep } from "@/lib/api";
import { splitColumnId } from "@/lib/utils";

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

// Full class strings required so Tailwind JIT includes them
const ROW_BORDER_IDLE: Record<string, string> = {
  passthrough: "border-l-green-400/30",
  aggregation: "border-l-amber-400/30",
  expression:  "border-l-purple-400/30",
  join_key:    "border-l-blue-400/30",
  window:      "border-l-indigo-400/30",
  cast:        "border-l-slate-400/30",
  filter:      "border-l-rose-400/30",
};

const ROW_BORDER_ACTIVE: Record<string, string> = {
  passthrough: "border-l-green-400",
  aggregation: "border-l-amber-400",
  expression:  "border-l-purple-400",
  join_key:    "border-l-blue-400",
  window:      "border-l-indigo-400",
  cast:        "border-l-slate-400",
  filter:      "border-l-rose-400",
};

function colLabel(colId: string) {
  const [tbl, col] = splitColumnId(colId);
  return { col, tbl, full: colId };
}

function tableShort(colId: string): string {
  const { tbl } = colLabel(colId);
  const parts = tbl.split(".");
  return parts[parts.length - 1] || tbl;
}

function pathVia(path: LineagePath): string {
  const { steps } = path;
  if (steps.length === 0) return "(empty)";
  if (steps.length === 1) return "direct";
  const intermediates = steps.slice(0, -1).map((s) => tableShort(s.target_col));
  if (intermediates.length === 1) return `via ${intermediates[0]}`;
  return `via ${intermediates[0]} → ${intermediates[intermediates.length - 1]}`;
}

function pathOrigin(path: LineagePath): string {
  if (path.steps.length === 0) return "(unknown)";
  return tableShort(path.steps[0].source_col);
}

function dominantTransform(steps: PathStep[]): string {
  const dom = [...steps].reverse().find((s) => s.transform_type !== "passthrough") ?? steps[steps.length - 1];
  return dom?.transform_type ?? "passthrough";
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

type GroupedItem = { index: number; via: string };
type PathGroup = { origin: string; items: GroupedItem[] };

function buildGroups(paths: LineagePath[], query: string): PathGroup[] {
  const q = query.toLowerCase();
  const map = new Map<string, GroupedItem[]>();

  paths.forEach((path, i) => {
    const origin = pathOrigin(path);
    const via = pathVia(path);
    if (q && !origin.toLowerCase().includes(q) && !via.toLowerCase().includes(q)) return;
    if (!map.has(origin)) map.set(origin, []);
    map.get(origin)!.push({ index: i, via });
  });

  const result: PathGroup[] = [];
  for (const [origin, rawItems] of map.entries()) {
    const viaCounts: Record<string, number> = {};
    const viaSeen: Record<string, number> = {};
    for (const { via } of rawItems) viaCounts[via] = (viaCounts[via] ?? 0) + 1;
    const items = rawItems.map(({ index, via }) => {
      if (viaCounts[via] === 1) return { index, via };
      viaSeen[via] = (viaSeen[via] ?? 0) + 1;
      return { index, via: `${via} (${viaSeen[via]})` };
    });
    result.push({ origin, items });
  }

  result.sort((a, b) => b.items.length - a.items.length);
  return result;
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

  const fileParts: string[] = [];
  if (step.source_file) fileParts.push(step.source_file.split(/[\\/]/).at(-1)!);
  if (step.source_cell != null) fileParts.push(`cell ${step.source_cell}`);
  if (step.source_line != null) fileParts.push(`line ${step.source_line}`);
  const fileRef = fileParts.join(" · ");

  return (
    <div className="flex flex-col items-start gap-0">
      {/* Source column node */}
      <div className="flex items-center gap-2.5 px-3 py-2.5 rounded-lg border border-border bg-card min-w-[220px] md:min-w-[280px] w-full md:w-auto shadow-sm">
        <div className="w-1.5 h-1.5 rounded-full bg-muted-foreground/40 flex-shrink-0 mt-px" />
        <div className="min-w-0">
          <div className="text-[10px] text-muted-foreground/60 font-mono truncate">{src.tbl}</div>
          <div className="font-mono font-medium text-sm text-foreground">{src.col}</div>
        </div>
      </div>

      {/* Connector */}
      <div className="flex items-start gap-2 pl-4 py-0.5">
        <div className="flex flex-col items-center flex-shrink-0 mt-1">
          <div className="w-px h-2.5 bg-border/60" />
          <div className="text-muted-foreground/50 text-[10px] leading-none">▼</div>
          <div className="w-px h-2.5 bg-border/60" />
        </div>
        <div className="flex flex-col gap-1 py-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <TransformBadge type={step.transform_type ?? "passthrough"} />
            {fileRef && (
              <span className="text-[10px] font-mono text-muted-foreground/70 bg-muted/40 px-1.5 py-0.5 rounded">
                {fileRef}
              </span>
            )}
          </div>

          {isPassthrough ? (
            <p className="text-[11px] text-muted-foreground/60 italic">passed through unchanged</p>
          ) : isNullData ? (
            <p className="text-[11px] text-muted-foreground/60 italic">structural hop — no expression recorded</p>
          ) : (
            allExpanded && (
              <div className="relative group rounded-md overflow-hidden mt-1 max-w-[520px] max-h-32 overflow-y-auto ring-1 ring-border/50">
                <SyntaxHighlighter
                  language={detectLang(step.source_file)}
                  style={vscDarkPlus}
                  customStyle={{ margin: 0, borderRadius: 6, fontSize: 11, padding: "8px 14px", lineHeight: "1.5" }}
                >
                  {step.expression!}
                </SyntaxHighlighter>
                <CopyButton text={step.expression!} />
              </div>
            )
          )}
        </div>
      </div>

      {/* Target column node (last hop only) */}
      {isLast && (
        <div className="flex items-center gap-2.5 px-3 py-2.5 rounded-lg border border-primary/30 bg-primary/5 min-w-[220px] md:min-w-[280px] w-full md:w-auto shadow-sm">
          <div className="w-1.5 h-1.5 rounded-full bg-primary/60 flex-shrink-0 mt-px" />
          <div className="min-w-0">
            <div className="text-[10px] text-muted-foreground/60 font-mono truncate">{tgt.tbl}</div>
            <div className="font-mono font-medium text-sm text-primary">{tgt.col}</div>
          </div>
        </div>
      )}
    </div>
  );
}

function GroupSeparator({ origin, count }: { origin: string; count: number }) {
  return (
    <div className="flex items-center gap-2 px-1 pt-2.5 pb-1">
      <div className="h-px flex-1 bg-border/40" />
      <span className="font-mono text-[10px] text-muted-foreground/50 whitespace-nowrap">{origin}</span>
      <span className="text-[10px] text-muted-foreground/35 tabular-nums">{count}</span>
      <div className="h-px flex-1 bg-border/40" />
    </div>
  );
}

function PathSelectorList({
  paths,
  selectedPath,
  onSelect,
}: {
  paths: LineagePath[];
  selectedPath: number | null;
  onSelect: (i: number) => void;
}) {
  const [search, setSearch] = useState("");
  const selectedRef = useRef<HTMLButtonElement | null>(null);
  const searchRef = useRef<HTMLInputElement | null>(null);

  const groups = useMemo(() => buildGroups(paths, search), [paths, search]);

  const flatIndices = useMemo(
    () => groups.flatMap((g) => g.items.map((it) => it.index)),
    [groups]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent, currentIndex: number) => {
      if (e.key !== "ArrowDown" && e.key !== "ArrowUp") return;
      e.preventDefault();
      const pos = flatIndices.indexOf(currentIndex);
      if (pos === -1) return;
      const nextPos =
        e.key === "ArrowDown"
          ? (pos + 1) % flatIndices.length
          : (pos - 1 + flatIndices.length) % flatIndices.length;
      onSelect(flatIndices[nextPos]);
    },
    [flatIndices, onSelect]
  );

  const multiOrigin = groups.length > 1;
  const totalVisible = flatIndices.length;

  return (
    <div className="space-y-1.5">
      {/* Search */}
      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground/50 pointer-events-none" />
        <input
          ref={searchRef}
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Filter paths…"
          className="w-full pl-7 pr-8 py-1.5 text-xs font-mono rounded-md border border-border bg-muted/20 text-foreground placeholder:text-muted-foreground/40 focus:outline-none focus:ring-1 focus:ring-ring focus:bg-background transition-colors"
        />
        {search ? (
          <button
            onClick={() => { setSearch(""); searchRef.current?.focus(); }}
            className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground/50 hover:text-muted-foreground transition-colors text-xs leading-none"
            aria-label="Clear search"
          >
            ✕
          </button>
        ) : (
          <span className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[10px] text-muted-foreground/30 font-mono pointer-events-none">
            {paths.length}
          </span>
        )}
      </div>

      {/* Path list */}
      <div
        role="radiogroup"
        aria-label="Transformation paths"
        className="max-h-72 overflow-y-auto rounded-md border border-border/60 bg-card/30"
      >
        {groups.length === 0 ? (
          <div className="flex flex-col items-center gap-1.5 py-6 text-center">
            <Search className="h-4 w-4 text-muted-foreground/30" />
            <p className="text-xs text-muted-foreground/50">No paths match &ldquo;{search}&rdquo;</p>
          </div>
        ) : (
          <div className="p-1">
            {groups.map(({ origin, items }, gi) => (
              <div key={origin}>
                {multiOrigin && (
                  <GroupSeparator origin={origin} count={items.length} />
                )}

                {items.map(({ index, via }, ii) => {
                  const steps = paths[index]?.steps ?? [];
                  const domTransform = dominantTransform(steps);
                  const isSelected = selectedPath === index;
                  const borderIdle = ROW_BORDER_IDLE[domTransform] ?? "border-l-border";
                  const borderActive = ROW_BORDER_ACTIVE[domTransform] ?? "border-l-primary";

                  return (
                    <button
                      key={index}
                      ref={isSelected ? selectedRef : null}
                      role="radio"
                      aria-checked={isSelected}
                      tabIndex={isSelected || (selectedPath === null && index === flatIndices[0]) ? 0 : -1}
                      title={via}
                      onClick={() => onSelect(index)}
                      onKeyDown={(e) => handleKeyDown(e, index)}
                      className={[
                        "w-full flex items-center justify-between gap-3 pl-2.5 pr-2 py-1.5 rounded-sm text-left",
                        "border-l-2 transition-all duration-100",
                        isSelected
                          ? `${borderActive} bg-accent/20 text-foreground`
                          : `${borderIdle} hover:bg-muted/40 text-muted-foreground hover:text-foreground`,
                      ].join(" ")}
                    >
                      <span className="font-mono text-xs truncate min-w-0 flex-1">{via}</span>
                      <div className="flex items-center gap-1.5 flex-shrink-0">
                        <span className="text-[10px] tabular-nums text-muted-foreground/50">
                          {steps.length}×
                        </span>
                        <TransformBadge type={domTransform} />
                      </div>
                    </button>
                  );
                })}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Footer: filter result count */}
      {search && totalVisible > 0 && (
        <p className="text-[10px] text-muted-foreground/40 text-right font-mono">
          {totalVisible} of {paths.length} shown
        </p>
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
  const [selectedPath, setSelectedPath] = useState<number | null>(null);
  const [allExpanded, setAllExpanded] = useState(true);

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

  const originCount = new Set(paths.map(pathOrigin)).size;
  const selectedSteps = selectedPath !== null ? (paths[selectedPath]?.steps ?? []) : null;
  const summary = selectedSteps && selectedSteps.length > 0 ? deriveSummary(selectedSteps) : null;

  return (
    <div className="space-y-4">
      {/* Aggregate summary — always visible */}
      <div className="rounded-lg border border-border bg-card px-4 py-3 space-y-0.5">
        {summary ? (
          <>
            <p className="text-sm font-medium text-foreground">
              {summary.verb} from{" "}
              <span className="font-mono text-xs bg-muted/50 px-1 py-0.5 rounded">
                {summary.source.tbl}.{summary.source.col}
              </span>
            </p>
            <p className="text-xs text-muted-foreground/70 font-mono">
              {summary.hops} {summary.hops === 1 ? "hop" : "hops"}
              {summary.file && <span className="text-muted-foreground/40"> · {summary.file}</span>}
              {paths.length > 1 && (
                <span className="text-muted-foreground/40">
                  {" · "}{paths.length} path{paths.length !== 1 ? "s" : ""}
                  {originCount > 1 && `, ${originCount} sources`}
                  {truncated && " (showing first 500)"}
                </span>
              )}
            </p>
          </>
        ) : (
          <p className="text-xs text-muted-foreground/70 font-mono">
            {paths.length} path{paths.length !== 1 ? "s" : ""}
            {originCount > 1 && `, ${originCount} sources`}
            {truncated && <span className="text-muted-foreground/40"> · showing first 500</span>}
            {" · "}
            <span className="text-muted-foreground/40">select a path below to inspect</span>
          </p>
        )}
      </div>

      {/* Path selector */}
      {paths.length > 1 ? (
        <PathSelectorList
          paths={paths}
          selectedPath={selectedPath}
          onSelect={setSelectedPath}
        />
      ) : null}

      {/* Chain — only when a path is selected */}
      {selectedSteps && selectedSteps.length > 0 && (
        <>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1.5 text-xs text-muted-foreground/60">
              <GitBranch className="h-3 w-3" />
              <span className="uppercase tracking-wider text-[10px] font-medium">transformation chain</span>
            </div>
            <div className="flex-1 h-px bg-border/40" />
            <button
              onClick={() => setAllExpanded((v) => !v)}
              className="text-[10px] text-muted-foreground/50 hover:text-muted-foreground transition-colors uppercase tracking-wider"
            >
              {allExpanded ? "collapse" : "expand"}
            </button>
          </div>

          <div className="flex flex-col gap-0">
            {selectedSteps.map((step, i) => (
              <HopCard
                key={i}
                step={step}
                isLast={i === selectedSteps.length - 1}
                allExpanded={allExpanded}
              />
            ))}
          </div>
        </>
      )}
    </div>
  );
}
