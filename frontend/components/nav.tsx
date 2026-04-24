"use client";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useState, useRef, useEffect } from "react";
import { cn } from "@/lib/utils";
import { useSearch } from "@/lib/hooks";
import { Search } from "lucide-react";

const links = [
  { href: "/sources", label: "Sources" },
  { href: "/catalog", label: "Catalog" },
  { href: "/lineage", label: "Lineage" },
  { href: "/impact", label: "Impact" },
];

function SearchBox() {
  const router = useRouter();
  const [q, setQ] = useState("");
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const { data: results } = useSearch(q);

  useEffect(() => {
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  function select(table: string, column: string) {
    setQ("");
    setOpen(false);
    router.push(`/lineage?table=${encodeURIComponent(table)}&column=${encodeURIComponent(column)}`);
  }

  return (
    <div ref={ref} className="relative ml-auto">
      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground pointer-events-none" />
        <input
          type="text"
          value={q}
          placeholder="Search columns…"
          onChange={(e) => { setQ(e.target.value); setOpen(true); }}
          onFocus={() => setOpen(true)}
          className="h-8 w-56 rounded-md border border-border bg-secondary pl-8 pr-3 text-xs placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring/50 focus:border-ring transition-colors"
        />
      </div>
      {open && q.length >= 2 && results && results.length > 0 && (
        <div className="absolute right-0 top-10 z-50 w-80 rounded-md border border-border bg-card shadow-xl shadow-black/40">
          <div className="max-h-64 overflow-y-auto py-1">
            {results.slice(0, 20).map((r) => (
              <button
                key={r.id}
                onClick={() => select(r.table, r.column)}
                className="w-full text-left px-3 py-2 text-sm hover:bg-accent transition-colors group"
              >
                <span className="font-mono text-xs text-muted-foreground group-hover:text-muted-foreground/80">{r.table}.</span>
                <span className="font-medium text-foreground">{r.column}</span>
              </button>
            ))}
          </div>
        </div>
      )}
      {open && q.length >= 2 && results && results.length === 0 && (
        <div className="absolute right-0 top-10 z-50 w-72 rounded-md border border-border bg-card shadow-xl shadow-black/40">
          <p className="px-3 py-2.5 text-xs text-muted-foreground">No results for &ldquo;{q}&rdquo;</p>
        </div>
      )}
    </div>
  );
}

export function Nav() {
  const path = usePathname();
  return (
    <nav className="border-b border-border bg-card px-6 h-12 flex items-center gap-1">
      <Link href="/sources" className="flex items-center gap-2 mr-5">
        <span className="flex h-6 w-6 items-center justify-center rounded bg-primary/15 text-primary text-xs font-bold leading-none">
          DL
        </span>
        <span className="text-sm font-semibold tracking-tight text-foreground">
          DataLineage
        </span>
      </Link>

      <div className="flex items-center gap-0.5">
        {links.map((l) => {
          const active = path.startsWith(l.href);
          return (
            <Link
              key={l.href}
              href={l.href}
              className={cn(
                "relative px-3 py-1.5 text-sm rounded-md transition-colors",
                active
                  ? "text-primary font-medium bg-primary/10"
                  : "text-muted-foreground hover:text-foreground hover:bg-accent"
              )}
            >
              {l.label}
              {active && (
                <span className="absolute bottom-0 left-3 right-3 h-px bg-primary rounded-full" />
              )}
            </Link>
          );
        })}
      </div>

      <SearchBox />
    </nav>
  );
}
