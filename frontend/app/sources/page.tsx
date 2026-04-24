"use client";
import { useState } from "react";
import { useSources, useDeleteSource, useRefreshSource, useSourceFiles } from "@/lib/hooks";
import { SourceForm } from "@/components/source-form";
import { Button } from "@/components/ui/button";
import { ChevronDown, ChevronRight, FileText, Plus, RefreshCw, X } from "lucide-react";
import { Source, SourceFile } from "@/lib/api";

function confidenceBadge(level: SourceFile["confidence"]) {
  const map = {
    high: "bg-green-500/15 text-green-400 border-green-500/30",
    medium: "bg-amber-500/15 text-amber-400 border-amber-500/30",
    low: "bg-red-500/15 text-red-400 border-red-500/30",
  };
  return (
    <span className={`inline-flex items-center rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${map[level]}`}>
      {level}
    </span>
  );
}

function SourceFileList({ sourceId }: { sourceId: string }) {
  const { data: files, isLoading } = useSourceFiles(sourceId);

  if (isLoading) {
    return <p className="px-1 py-2 text-xs text-muted-foreground">Loading files…</p>;
  }
  if (!files || files.length === 0) {
    return <p className="px-1 py-2 text-xs text-muted-foreground">No parsed files. Refresh the source to parse.</p>;
  }

  return (
    <div className="mt-3 space-y-0.5 border-t border-border pt-3">
      {files.map((f) => (
        <div key={f.file} className="flex items-center gap-2 rounded px-1 py-1.5 text-xs hover:bg-accent/50 transition-colors">
          <FileText className="h-3 w-3 shrink-0 text-muted-foreground" />
          <span className="flex-1 truncate font-mono text-muted-foreground" title={f.file}>
            {f.file.split("/").pop() ?? f.file}
          </span>
          <span className="text-muted-foreground tabular-nums">{f.edge_count} edges</span>
          {confidenceBadge(f.confidence)}
        </div>
      ))}
    </div>
  );
}

function SourceRow({ src }: { src: Source }) {
  const [expanded, setExpanded] = useState(false);
  const del = useDeleteSource();
  const refresh = useRefreshSource();

  const statusColor =
    src.warning_count > 0
      ? "text-amber-400"
      : src.status === "parsed"
      ? "text-green-400"
      : "text-muted-foreground";

  return (
    <div className="rounded-md border border-border bg-secondary/30 px-3 py-2.5 text-sm transition-colors hover:bg-secondary/50">
      <div className="flex items-center gap-3">
        <span className={`text-xs ${statusColor}`}>●</span>
        <button
          className="flex flex-1 items-center gap-1.5 truncate text-left"
          onClick={() => setExpanded((v) => !v)}
        >
          <span className="flex-1 truncate font-mono text-sm font-medium text-foreground">{src.url}</span>
          {expanded
            ? <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            : <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
          }
        </button>
        <span className="text-xs text-muted-foreground capitalize">{src.source_type}</span>
        <span className="text-xs text-muted-foreground tabular-nums">{src.file_count} files</span>
        {src.warning_count > 0 && (
          <span className="text-xs text-amber-400">{src.warning_count} warnings</span>
        )}
        <Button
          size="icon-sm"
          variant="ghost"
          onClick={() => refresh.mutate(src.id)}
          disabled={refresh.isPending}
          title="Refresh"
        >
          <RefreshCw className="h-3.5 w-3.5" />
        </Button>
        <Button
          size="icon-sm"
          variant="ghost"
          onClick={() => del.mutate(src.id)}
          title="Remove"
          className="hover:text-destructive"
        >
          <X className="h-3.5 w-3.5" />
        </Button>
      </div>
      {expanded && <SourceFileList sourceId={src.id} />}
    </div>
  );
}

export default function SourcesPage() {
  const { data: sources, isLoading } = useSources();

  return (
    <div className="max-w-2xl space-y-5">
      <h1 className="text-xl font-semibold tracking-tight">Sources</h1>

      {/* Connected sources */}
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="flex items-center justify-between px-4 py-3 border-b border-border">
          <span className="text-sm font-medium text-foreground">Connected Sources</span>
          {sources && sources.length > 0 && (
            <span className="text-xs text-muted-foreground tabular-nums">{sources.length} source{sources.length !== 1 ? "s" : ""}</span>
          )}
        </div>
        <div className="p-3 space-y-2">
          {isLoading && <p className="text-sm text-muted-foreground px-1 py-1">Loading…</p>}
          {!isLoading && sources?.length === 0 && (
            <div className="flex flex-col items-center gap-2 py-8 text-center">
              <p className="text-sm text-muted-foreground">No sources connected yet.</p>
              <p className="text-xs text-muted-foreground/60">Add a source below to start tracing lineage.</p>
            </div>
          )}
          {sources?.map((src) => (
            <SourceRow key={src.id} src={src} />
          ))}
        </div>
      </div>

      {/* Add source */}
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="flex items-center gap-2 px-4 py-3 border-b border-border">
          <Plus className="h-3.5 w-3.5 text-muted-foreground" />
          <span className="text-sm font-medium text-foreground">Add Source</span>
        </div>
        <div className="p-4">
          <SourceForm />
        </div>
      </div>

      {/* Confidence legend */}
      <div className="rounded-lg border border-border bg-card/50 px-4 py-3">
        <p className="text-xs font-semibold text-muted-foreground uppercase tracking-widest mb-2">Confidence Tiers</p>
        <div className="space-y-1.5">
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            {confidenceBadge("high")}
            <span>All edges resolved with certainty</span>
          </div>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            {confidenceBadge("medium")}
            <span>Some edges are approximate or have warnings</span>
          </div>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            {confidenceBadge("low")}
            <span>Parse errors — lineage may be incomplete</span>
          </div>
        </div>
      </div>
    </div>
  );
}
