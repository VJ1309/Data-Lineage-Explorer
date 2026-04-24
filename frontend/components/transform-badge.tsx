import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

const COLOURS: Record<string, string> = {
  passthrough: "bg-green-500/15 text-green-400 border-green-500/30",
  aggregation: "bg-amber-500/15 text-amber-400 border-amber-500/30",
  expression:  "bg-purple-500/15 text-purple-400 border-purple-500/30",
  join_key:    "bg-blue-500/15 text-blue-400 border-blue-500/30",
  window:      "bg-indigo-500/15 text-indigo-400 border-indigo-500/30",
  cast:        "bg-slate-500/15 text-slate-400 border-slate-500/30",
  filter:      "bg-rose-500/15 text-rose-400 border-rose-500/30",
};

const LABELS: Record<string, string> = {
  passthrough: "Passthrough",
  aggregation: "Aggregation",
  expression:  "Expression",
  join_key:    "Join Key",
  window:      "Window",
  cast:        "Cast",
  filter:      "Filter",
};

const DESCRIPTIONS: Record<string, string> = {
  passthrough: "Column is copied directly from source with no transformation (may be renamed)",
  aggregation: "Column is computed by an aggregate function: SUM, COUNT, AVG, MAX, MIN, etc.",
  expression:  "Column is derived via arithmetic (+, -, *, /), CASE WHEN, IF, or COALESCE",
  join_key:    "Column is used as a join key between tables",
  window:      "Column is computed by a window function using OVER (PARTITION BY ...)",
  cast:        "Column value is type-cast to a different data type via CAST(...)",
  filter:      "Column is used as a filter condition (WHERE / HAVING)",
};

export function TransformBadge({ type }: { type: string | null }) {
  if (!type) return null;
  return (
    <Badge
      variant="outline"
      title={DESCRIPTIONS[type]}
      className={cn("text-xs font-medium cursor-help", COLOURS[type] ?? "bg-muted text-muted-foreground")}
    >
      {LABELS[type] ?? type}
    </Badge>
  );
}
