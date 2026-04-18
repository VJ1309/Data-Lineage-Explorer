"use client";
import { useState } from "react";
import { useSources, useDeleteSource, useRefreshSource, useWarnings, useSourceFiles } from "@/lib/hooks";
import { SourceForm } from "@/components/source-form";
import { WarningsPanel } from "@/components/warnings-panel";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ChevronDown, ChevronRight, FileText } from "lucide-react";
import { Source, SourceFile } from "@/lib/api";

function confidenceBadge(level: SourceFile["confidence"]) {
  const map = {
    high: "bg-green-500/15 text-green-600 border-green-500/30",
    medium: "bg-amber-500/15 text-amber-600 border-amber-500/30",
    low: "bg-red-500/15 text-red-500 border-red-500/30",
  };
  return (
    <span className={`inline-flex items-center rounded border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide ${map[level]}`}>
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
    <div className="mt-3 space-y-1 border-t pt-3">
      {files.map((f) => (
        <div key={f.file} className="flex items-center gap-2 rounded px-1 py-1 text-xs hover:bg-muted/40">
          <FileText className="h-3 w-3 shrink-0 text-muted-foreground" />
          <span className="flex-1 truncate font-mono text-muted-foreground" title={f.file}>
            {f.file.split("/").pop() ?? f.file}
          </span>
          <span className="text-muted-foreground">{f.edge_count} edges</span>
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

  const warningDot =
    src.warning_count > 0
      ? "text-amber-500"
      : src.status === "parsed"
      ? "text-green-600"
      : "text-muted-foreground";

  return (
    <div className="rounded-md border px-3 py-2 text-sm">
      <div className="flex items-center gap-3">
        <span className={warningDot}>●</span>
        <button
          className="flex flex-1 items-center gap-1 truncate text-left"
          onClick={() => setExpanded((v) => !v)}
        >
          <span className="flex-1 truncate font-medium">{src.url}</span>
          {expanded ? (
            <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
          )}
        </button>
        <span className="text-xs text-muted-foreground capitalize">{src.source_type}</span>
        <span className="text-xs text-muted-foreground">{src.file_count} files</span>
        {src.warning_count > 0 && (
          <span className="text-xs text-amber-500">{src.warning_count} warnings</span>
        )}
        <Button
          size="sm"
          variant="outline"
          onClick={() => refresh.mutate(src.id)}
          disabled={refresh.isPending}
        >
          ↻
        </Button>
        <Button size="sm" variant="ghost" onClick={() => del.mutate(src.id)}>
          ✕
        </Button>
      </div>
      {expanded && <SourceFileList sourceId={src.id} />}
    </div>
  );
}

export default function SourcesPage() {
  const { data: sources, isLoading } = useSources();
  const { data: warnings } = useWarnings();
  const [showAll, setShowAll] = useState(false);

  return (
    <div className="max-w-3xl space-y-6">
      <h1 className="text-2xl font-semibold">Sources</h1>

      {warnings && warnings.length > 0 && (
        <WarningsPanel
          warnings={warnings}
          expanded={showAll}
          onToggle={() => setShowAll((v) => !v)}
        />
      )}

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Connected Sources</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {isLoading && <p className="text-sm text-muted-foreground">Loading…</p>}
          {sources?.length === 0 && (
            <p className="text-sm text-muted-foreground">No sources connected yet.</p>
          )}
          {sources?.map((src) => (
            <SourceRow key={src.id} src={src} />
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Add Source</CardTitle>
        </CardHeader>
        <CardContent>
          <SourceForm />
        </CardContent>
      </Card>

      <div className="rounded-md border border-muted bg-muted/30 px-4 py-3 text-xs text-muted-foreground">
        <p className="font-medium text-foreground">Confidence tiers</p>
        <div className="mt-1.5 flex flex-wrap gap-3">
          <span className="flex items-center gap-1.5">
            {confidenceBadge("high")}
            <span>All edges resolved with certainty</span>
          </span>
          <span className="flex items-center gap-1.5">
            {confidenceBadge("medium")}
            <span>Some edges are approximate or have warnings</span>
          </span>
          <span className="flex items-center gap-1.5">
            {confidenceBadge("low")}
            <span>Parse errors — lineage may be incomplete</span>
          </span>
        </div>
      </div>
    </div>
  );
}
