"use client";
import { useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism";
import type { LineageEdge } from "@/lib/api";
import { TransformBadge } from "./transform-badge";

type Props = {
  targetColId: string;
  edges: LineageEdge[];
};

export function CodeInspector({ targetColId, edges }: Props) {
  const [selected, setSelected] = useState<LineageEdge | null>(
    edges.length > 0 ? edges[0] : null
  );

  if (edges.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">No upstream transformations found.</p>
    );
  }

  return (
    <div className="flex gap-4 h-[400px]">
      {/* Left: column list */}
      <div className="w-56 flex-shrink-0 border rounded-md overflow-y-auto">
        {edges.map((e, i) => (
          <button
            key={i}
            onClick={() => setSelected(e)}
            className={`w-full text-left px-3 py-2 text-sm border-b last:border-0 transition-colors ${
              selected === e ? "bg-accent text-accent-foreground" : "hover:bg-muted"
            }`}
          >
            <div className="font-mono text-xs text-muted-foreground">{e.source_col.split(".").at(-2) ?? ""}</div>
            <div className="font-medium">{e.source_col.split(".").at(-1) ?? e.source_col}</div>
            <TransformBadge type={e.transform_type} />
          </button>
        ))}
      </div>

      {/* Right: code view */}
      <div className="flex-1 flex flex-col gap-2 min-w-0">
        {selected && (
          <>
            <div className="flex items-center gap-2 text-sm">
              <span className="font-medium">{selected.target_col}</span>
              <span className="text-muted-foreground">←</span>
              <span className="text-muted-foreground">{selected.source_col}</span>
              <TransformBadge type={selected.transform_type} />
            </div>
            <div className="text-xs text-muted-foreground">
              {selected.source_file}
              {selected.source_cell != null ? ` · cell ${selected.source_cell}` : ""}
              {selected.source_line != null ? ` · line ${selected.source_line}` : ""}
            </div>
            <div className="flex-1 overflow-auto rounded-md">
              <SyntaxHighlighter
                language={selected.source_file?.endsWith(".sql") ? "sql" : "python"}
                style={vscDarkPlus}
                customStyle={{ margin: 0, borderRadius: 6, fontSize: 12 }}
              >
                {selected.expression ?? ""}
              </SyntaxHighlighter>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
