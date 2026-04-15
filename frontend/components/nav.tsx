"use client";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useState, useRef, useEffect } from "react";
import { cn } from "@/lib/utils";
import { useSearch } from "@/lib/hooks";

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
      <input
        type="text"
        value={q}
        placeholder="Search columns…"
        onChange={(e) => { setQ(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        className="h-7 w-52 rounded border bg-muted px-2 text-xs placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
      />
      {open && q.length >= 2 && results && results.length > 0 && (
        <div className="absolute right-0 top-8 z-50 w-80 rounded-md border bg-popover shadow-md">
          <div className="max-h-64 overflow-y-auto py-1">
            {results.slice(0, 20).map((r) => (
              <button
                key={r.id}
                onClick={() => select(r.table, r.column)}
                className="w-full text-left px-3 py-1.5 text-sm hover:bg-accent transition-colors"
              >
                <span className="text-muted-foreground text-xs font-mono">{r.table}.</span>
                <span className="font-medium">{r.column}</span>
              </button>
            ))}
          </div>
        </div>
      )}
      {open && q.length >= 2 && results && results.length === 0 && (
        <div className="absolute right-0 top-8 z-50 w-72 rounded-md border bg-popover shadow-md">
          <p className="px-3 py-2 text-xs text-muted-foreground">No results for "{q}"</p>
        </div>
      )}
    </div>
  );
}

export function Nav() {
  const path = usePathname();
  return (
    <nav className="border-b bg-background px-6 py-3 flex items-center gap-6">
      <span className="font-bold text-sm tracking-tight mr-4">
        DataLineage Explorer
      </span>
      {links.map((l) => (
        <Link
          key={l.href}
          href={l.href}
          className={cn(
            "text-sm transition-colors hover:text-foreground",
            path.startsWith(l.href)
              ? "text-foreground font-medium"
              : "text-muted-foreground"
          )}
        >
          {l.label}
        </Link>
      ))}
      <SearchBox />
    </nav>
  );
}
