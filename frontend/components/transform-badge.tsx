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

export function TransformBadge({ type }: { type: string | null }) {
  if (!type) return null;
  return (
    <Badge
      variant="outline"
      className={cn("text-xs font-medium", COLOURS[type] ?? "bg-muted text-muted-foreground")}
    >
      {type}
    </Badge>
  );
}
