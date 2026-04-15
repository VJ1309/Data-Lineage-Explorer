import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

const COLOURS: Record<string, string> = {
  passthrough: "bg-green-100 text-green-800 border-green-200",
  aggregation: "bg-amber-100 text-amber-800 border-amber-200",
  expression:  "bg-purple-100 text-purple-800 border-purple-200",
  join_key:    "bg-blue-100 text-blue-800 border-blue-200",
  window:      "bg-indigo-100 text-indigo-800 border-indigo-200",
  cast:        "bg-slate-100 text-slate-800 border-slate-200",
  filter:      "bg-rose-100 text-rose-800 border-rose-200",
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
