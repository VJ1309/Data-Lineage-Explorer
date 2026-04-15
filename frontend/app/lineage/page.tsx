"use client";
import { useSearchParams } from "next/navigation";
import { Suspense } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useLineage } from "@/lib/hooks";
import { LineageGraph } from "@/components/lineage-graph";
import { LineageTree } from "@/components/lineage-tree";
import { CodeInspector } from "@/components/code-inspector";

function LineageContent() {
  const params = useSearchParams();
  const table = params.get("table");
  const column = params.get("column");
  const { data, isLoading, error } = useLineage(table, column);

  if (!table || !column) {
    return (
      <p className="text-sm text-muted-foreground">
        Select a column from the <a href="/catalog" className="underline">Catalog</a> to view its lineage.
      </p>
    );
  }

  if (isLoading) return <p className="text-sm text-muted-foreground">Loading lineage…</p>;
  if (error) return <p className="text-sm text-destructive">Error: {(error as Error).message}</p>;
  if (!data) return null;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <h1 className="text-2xl font-semibold">{column}</h1>
        <span className="text-muted-foreground text-sm">in {table}</span>
      </div>
      <div className="text-xs text-muted-foreground">
        {data.upstream.length} upstream source{data.upstream.length !== 1 ? "s" : ""} ·{" "}
        {data.downstream.length} downstream dependent{data.downstream.length !== 1 ? "s" : ""}
      </div>

      <Tabs defaultValue="graph">
        <TabsList>
          <TabsTrigger value="graph">⬡ Graph</TabsTrigger>
          <TabsTrigger value="tree">≡ Tree</TabsTrigger>
          <TabsTrigger value="code">&lt;/&gt; Code</TabsTrigger>
        </TabsList>

        <TabsContent value="graph" className="pt-4">
          <LineageGraph
            nodes={data.graph.nodes}
            edges={data.graph.edges}
            targetColId={data.target}
          />
        </TabsContent>

        <TabsContent value="tree" className="pt-4">
          <LineageTree targetColId={data.target} upstream={data.upstream} downstream={data.downstream} />
        </TabsContent>

        <TabsContent value="code" className="pt-4">
          <CodeInspector targetColId={data.target} edges={data.upstream} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

export default function LineagePage() {
  return (
    <Suspense fallback={<p className="text-sm text-muted-foreground">Loading…</p>}>
      <LineageContent />
    </Suspense>
  );
}
