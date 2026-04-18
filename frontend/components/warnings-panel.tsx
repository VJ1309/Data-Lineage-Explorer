"use client";
import type { Warning } from "@/lib/api";

type Severity = "info" | "warn" | "error";

const SEVERITY_STYLES: Record<Severity, { icon: string; border: string; bg: string; text: string; accent: string }> = {
  error: {
    icon: "✕",
    border: "border-rose-500/40",
    bg: "bg-rose-500/10",
    text: "text-rose-200",
    accent: "text-rose-400",
  },
  warn: {
    icon: "⚠",
    border: "border-amber-500/40",
    bg: "bg-amber-500/10",
    text: "text-amber-100",
    accent: "text-amber-400",
  },
  info: {
    icon: "ⓘ",
    border: "border-sky-500/40",
    bg: "bg-sky-500/10",
    text: "text-sky-100",
    accent: "text-sky-400",
  },
};

const SEVERITY_ORDER: Severity[] = ["error", "warn", "info"];

function severityOf(w: Warning): Severity {
  const s = w.severity;
  if (s === "info" || s === "error") return s;
  return "warn";
}

type Props = {
  warnings: Warning[];
  expanded: boolean;
  onToggle: () => void;
};

export function WarningsPanel({ warnings, expanded, onToggle }: Props) {
  const counts: Record<Severity, number> = { error: 0, warn: 0, info: 0 };
  for (const w of warnings) counts[severityOf(w)]++;

  const dominant: Severity =
    counts.error > 0 ? "error" : counts.warn > 0 ? "warn" : "info";
  const s = SEVERITY_STYLES[dominant];

  return (
    <div className={`rounded-md border ${s.border} ${s.bg} text-sm`}>
      <button
        type="button"
        onClick={onToggle}
        className={`flex w-full items-center gap-3 px-4 py-3 text-left ${s.text}`}
      >
        <span className={s.accent}>{s.icon}</span>
        <span className="flex-1">
          {warnings.length} parse warning{warnings.length > 1 ? "s" : ""}
          {SEVERITY_ORDER.some((k) => counts[k] > 0) && (
            <span className="ml-2 text-xs opacity-70">
              {SEVERITY_ORDER.filter((k) => counts[k] > 0)
                .map((k) => `${counts[k]} ${k}`)
                .join(" · ")}
            </span>
          )}
        </span>
        <span className="text-xs opacity-70">{expanded ? "Hide" : "Show"}</span>
      </button>
      {expanded && (
        <div className="border-t border-white/5">
          <ul className="divide-y divide-white/5">
            {warnings.map((w, i) => {
              const sev = severityOf(w);
              const st = SEVERITY_STYLES[sev];
              return (
                <li key={i} className="flex gap-3 px-4 py-2 font-mono text-xs">
                  <span className={`${st.accent} shrink-0 w-4`} aria-label={sev}>
                    {st.icon}
                  </span>
                  <span className="text-muted-foreground shrink-0 w-40 truncate" title={w.file}>
                    {w.file}
                  </span>
                  <span className={`flex-1 ${st.text} break-all`}>{w.error}</span>
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}
