"use client";
import { useSearchParams } from "next/navigation";
import { Suspense, useState, useEffect } from "react";
import Link from "next/link";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useLineage, usePaths } from "@/lib/hooks";
import { LineageGraph } from "@/components/lineage-graph";
import { LineageTree } from "@/components/lineage-tree";
import { TransformInspector } from "@/components/transform-inspector";
import { GitBranch } from "lucide-react";
import { splitColumnId } from "@/lib/utils";

function LineageContent() {
  const params = useSearchParams();
  const table = params.get("table");
  const column = params.get("column");

  const [selectedColId, setSelectedColId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState("graph");

  const { data, isLoading, error } = useLineage(table, column);
  const inspectedColId = selectedColId ?? data?.target ?? null;
  const [inspectedTable, inspectedColumn] = inspectedColId
    ? splitColumnId(inspectedColId)
    : [null, null];
  const {
    data: pathsData,
    isLoading: pathsLoading,
    isError: pathsError,
    error: pathsErrorValue,
  } = usePaths(inspectedTable, inspectedColumn, activeTab === "transform");

  // Reset inspector when the viewed column changes
  useEffect(() => {
    setSelectedColId(null);
    setActiveTab("graph");
  }, [table, column]);

  function handleColumnClick(colId: string) {
    setSelectedColId(colId);
    setActiveTab("transform");
  }

  if (!table || !column) {
    return (
      <div className="flex flex-col items-center justify-center h-[60vh] gap-5 text-center">
        <div className="flex h-16 w-16 items-center justify-center rounded-2xl border border-border bg-card text-muted-foreground">
          <GitBranch className="h-7 w-7" />
        </div>
        <div className="space-y-1.5">
          <p className="text-sm font-medium text-foreground">No column selected</p>
          <p className="text-sm text-muted-foreground max-w-xs">
            Browse the{" "}
            <Link href="/catalog" className="text-primary underline underline-offset-2 hover:text-primary/80 transition-colors">
              Catalog
            </Link>{" "}
            and click &ldquo;Lineage&rdquo; on any column to trace its data flow.
          </p>
        </div>
      </div>
    );
  }

  if (isLoading) return <p className="text-sm text-muted-foreground">Loading lineage…</p>;
  if (error) return <p className="text-sm text-destructive">Error: {(error as Error).message}</p>;
  if (!data) return null;

  const tableParts = table.split(".");
  const tableName = tableParts[tableParts.length - 1];
  const tablePrefix = tableParts.slice(0, -1).join(".");

  return (
    <div className="space-y-4">
      <div>
        <div className="flex items-baseline gap-2">
          <h1 className="font-mono text-2xl font-semibold text-foreground">{column}</h1>
          <span className="text-muted-foreground text-sm">
            in{" "}
            {tablePrefix && <span className="text-muted-foreground/60">{tablePrefix}.</span>}
            <span className="text-muted-foreground font-medium">{tableName}</span>
          </span>
        </div>
        <div className="flex items-center gap-3 mt-1.5">
          <span className="text-xs text-muted-foreground">
            <span className="text-green-400 font-medium">{data.upstream.length}</span>
            {" "}upstream source{data.upstream.length !== 1 ? "s" : ""}
          </span>
          <span className="text-muted-foreground/30 text-xs">·</span>
          <span className="text-xs text-muted-foreground">
            <span className="text-purple-400 font-medium">{data.downstream.length}</span>
            {" "}downstream dependent{data.downstream.length !== 1 ? "s" : ""}
          </span>
        </div>
      </div>

      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="graph">⬡ Graph</TabsTrigger>
          <TabsTrigger value="tree">≡ Tree</TabsTrigger>
          <TabsTrigger value="transform">⇢ Transform</TabsTrigger>
        </TabsList>

        <TabsContent value="graph" className="pt-4">
          <LineageGraph
            nodes={data.graph.nodes}
            edges={data.graph.edges}
            targetColId={data.target}
            onColumnClick={handleColumnClick}
          />
        </TabsContent>

        <TabsContent value="tree" className="pt-4">
          <LineageTree targetColId={data.target} upstream={data.upstream} downstream={data.downstream} />
        </TabsContent>

        <TabsContent value="transform" className="pt-4">
          <TransformInspector
            paths={pathsData?.paths ?? []}
            truncated={pathsData?.truncated ?? false}
            isLoading={pathsLoading}
            isError={pathsError}
            errorMessage={(pathsErrorValue as Error | null)?.message}
          />
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
