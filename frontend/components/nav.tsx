"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";

const links = [
  { href: "/sources", label: "Sources" },
  { href: "/catalog", label: "Catalog" },
  { href: "/lineage", label: "Lineage" },
  { href: "/impact", label: "Impact" },
];

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
    </nav>
  );
}
