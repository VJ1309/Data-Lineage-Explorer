"use client";
import { useSources, useDeleteSource, useRefreshSource, useWarnings } from "@/lib/hooks";
import { SourceForm } from "@/components/source-form";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function SourcesPage() {
  const { data: sources, isLoading } = useSources();
  const { data: warnings } = useWarnings();
  const del = useDeleteSource();
  const refresh = useRefreshSource();

  return (
    <div className="max-w-3xl space-y-6">
      <h1 className="text-2xl font-semibold">Sources</h1>

      {warnings && warnings.length > 0 && (
        <div className="rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          ⚠ {warnings.length} parse warning{warnings.length > 1 ? "s" : ""} — some files may not be fully analyzed.
        </div>
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
            <div key={src.id} className="flex items-center gap-3 rounded-md border px-3 py-2 text-sm">
              <span className={src.status === "parsed" ? "text-green-600" : "text-muted-foreground"}>●</span>
              <span className="flex-1 truncate font-medium">{src.url}</span>
              <span className="text-xs text-muted-foreground capitalize">{src.source_type}</span>
              <span className="text-xs text-muted-foreground">{src.file_count} files</span>
              <Button size="sm" variant="outline" onClick={() => refresh.mutate(src.id)}>
                ↻
              </Button>
              <Button size="sm" variant="ghost" onClick={() => del.mutate(src.id)}>
                ✕
              </Button>
            </div>
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
    </div>
  );
}
