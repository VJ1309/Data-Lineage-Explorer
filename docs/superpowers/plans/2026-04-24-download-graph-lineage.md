# Download Graph Lineage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Download ↓" dropdown to the lineage graph toolbar that exports the current graph as a PNG image or a JSON data file.

**Architecture:** All changes are in `frontend/components/lineage-graph.tsx`. A `containerRef` targets the ReactFlow container div for `html-to-image` PNG capture. JSON export serializes `visibleNodes`/`visibleEdges` to a Blob download via browser APIs. A dropdown pill with `useEffect` click-outside handling controls the menu.

**Tech Stack:** React (`useRef`, `useEffect`, `useCallback`), `html-to-image` (`toPng`), browser Blob/URL APIs.

---

## File Map

| Action | File |
|--------|------|
| Modify | `frontend/package.json` — add `html-to-image` dependency |
| Modify | `frontend/components/lineage-graph.tsx` — all feature logic and UI |

---

### Task 1: Install html-to-image

**Files:**
- Modify: `frontend/package.json`

- [ ] **Step 1: Install the package**

```bash
cd frontend && npm install html-to-image
```

Expected output ends with something like: `added 1 package` and no errors.

- [ ] **Step 2: Verify the import resolves**

```bash
cd frontend && node -e "require('html-to-image'); console.log('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add frontend/package.json frontend/package-lock.json
git commit -m "deps(frontend): add html-to-image for PNG graph export"
```

---

### Task 2: Add imports, state, refs, and handlers to LineageGraph

**Files:**
- Modify: `frontend/components/lineage-graph.tsx`

The component currently imports only `useMemo` and `useState` from React. We need `useRef`, `useEffect`, and `useCallback` too. Handlers must be placed **after** the `visibleNodes` and `visibleEdges` `useMemo` calls they depend on.

- [ ] **Step 1: Update the React import line**

Find (line 2):
```tsx
import { useMemo, useState } from "react";
```

Replace with:
```tsx
import { useMemo, useState, useRef, useEffect, useCallback } from "react";
```

- [ ] **Step 2: Add the html-to-image import**

After the existing imports block (after line 12, `import type { LineageEdge } from "@/lib/api";`), add:

```tsx
import { toPng } from "html-to-image";
```

- [ ] **Step 3: Add state and refs inside the component**

Inside `export function LineageGraph(...)`, after the existing state declarations (`const [collapsed, ...]`, `const [showFilters, ...]`, `const [showJoinKeys, ...]`, `const [targetTable] = ...`), add:

```tsx
  const containerRef = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [downloadOpen, setDownloadOpen] = useState(false);
  const [downloadStatus, setDownloadStatus] = useState<"idle" | "loading" | "failed">("idle");
```

- [ ] **Step 4: Add click-outside handler**

After the four new declarations above (still before the `useMemo` calls), add:

```tsx
  useEffect(() => {
    if (!downloadOpen) return;
    function handleClickOutside(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setDownloadOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [downloadOpen]);
```

- [ ] **Step 5: Add JSON and PNG download handlers**

These reference `visibleNodes` and `visibleEdges`, so they must go **after** those two `useMemo` declarations. Find the line where `tableLevel` is defined (`const tableLevel = useMemo(...)`) and add the handlers directly before it:

```tsx
  const handleDownloadJson = useCallback(() => {
    const colId = targetColId.replace(/[^a-zA-Z0-9._-]/g, "_");
    const payload = JSON.stringify({ nodes: visibleNodes, edges: visibleEdges }, null, 2);
    const blob = new Blob([payload], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `lineage-${colId}-data.json`;
    a.click();
    URL.revokeObjectURL(url);
    setDownloadOpen(false);
  }, [visibleNodes, visibleEdges, targetColId]);

  const handleDownloadPng = useCallback(async () => {
    if (!containerRef.current) return;
    setDownloadStatus("loading");
    setDownloadOpen(false);
    try {
      const colId = targetColId.replace(/[^a-zA-Z0-9._-]/g, "_");
      const dataUrl = await toPng(containerRef.current, { backgroundColor: "#0a0f1a" });
      const a = document.createElement("a");
      a.href = dataUrl;
      a.download = `lineage-${colId}-graph.png`;
      a.click();
      setDownloadStatus("idle");
    } catch {
      setDownloadStatus("failed");
      setTimeout(() => setDownloadStatus("idle"), 1500);
    }
  }, [targetColId]);
```

- [ ] **Step 6: Verify TypeScript**

```bash
cd frontend && npx tsc --noEmit 2>&1 | grep "lineage-graph"
```

Expected: no output (no errors from that file).

- [ ] **Step 7: Commit**

```bash
git add frontend/components/lineage-graph.tsx
git commit -m "feat(lineage-graph): add download state, refs, and handlers"
```

---

### Task 3: Add Download dropdown UI and wire the containerRef

**Files:**
- Modify: `frontend/components/lineage-graph.tsx`

- [ ] **Step 1: Attach containerRef to the ReactFlow container div**

Find:
```tsx
      <div style={{ height: 500, background: "#0a0f1a", borderRadius: 8 }}>
```

Replace with:
```tsx
      <div ref={containerRef} style={{ height: 500, background: "#0a0f1a", borderRadius: 8 }}>
```

- [ ] **Step 2: Add the Download pill and dropdown to the toolbar**

The toolbar `<div className="flex justify-end mb-2 gap-2">` ends with the "Group by table" button. Find:

```tsx
        <button
          onClick={() => setCollapsed((v) => !v)}
          className={pillClass(collapsed)}
        >
          {collapsed ? "⊞ Expand columns" : "⊟ Group by table"}
        </button>
      </div>
```

Replace with:

```tsx
        <button
          onClick={() => setCollapsed((v) => !v)}
          className={pillClass(collapsed)}
        >
          {collapsed ? "⊞ Expand columns" : "⊟ Group by table"}
        </button>

        <div ref={dropdownRef} style={{ position: "relative" }}>
          <button
            onClick={() => setDownloadOpen((v) => !v)}
            className={pillClass(downloadOpen)}
            disabled={downloadStatus === "loading"}
          >
            {downloadStatus === "loading"
              ? "…"
              : downloadStatus === "failed"
              ? "Failed"
              : "↓ Download"}
          </button>
          {downloadOpen && (
            <div
              style={{
                position: "absolute",
                top: "100%",
                right: 0,
                marginTop: 4,
                background: "#1a2233",
                border: "1px solid #3d4f6b",
                borderRadius: 6,
                overflow: "hidden",
                zIndex: 50,
                minWidth: 130,
              }}
            >
              <button
                onClick={handleDownloadPng}
                style={{
                  display: "block",
                  width: "100%",
                  textAlign: "left",
                  padding: "8px 12px",
                  fontSize: 12,
                  color: "#a0b4c8",
                  background: "transparent",
                  border: "none",
                  cursor: "pointer",
                }}
                onMouseEnter={(e) => (e.currentTarget.style.background = "#243047")}
                onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
              >
                ↓ PNG
              </button>
              <button
                onClick={handleDownloadJson}
                style={{
                  display: "block",
                  width: "100%",
                  textAlign: "left",
                  padding: "8px 12px",
                  fontSize: 12,
                  color: "#a0b4c8",
                  background: "transparent",
                  border: "none",
                  cursor: "pointer",
                }}
                onMouseEnter={(e) => (e.currentTarget.style.background = "#243047")}
                onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
              >
                ↓ JSON
              </button>
            </div>
          )}
        </div>
      </div>
```

- [ ] **Step 3: Verify TypeScript**

```bash
cd frontend && npx tsc --noEmit 2>&1 | grep "lineage-graph"
```

Expected: no output.

- [ ] **Step 4: Run production build**

```bash
cd frontend && npm run build 2>&1 | tail -30
```

Expected: build completes successfully with no type errors.

- [ ] **Step 5: Manual smoke test**

```bash
cd frontend && npm run dev
```

Open `http://localhost:3000`, navigate to a column's lineage page (e.g. via the Catalog), open the **Graph** tab and verify:

1. "↓ Download" pill appears in the toolbar row, right-aligned.
2. Clicking it opens a dropdown with "↓ PNG" and "↓ JSON".
3. Clicking outside the dropdown closes it without downloading anything.
4. Clicking "↓ JSON" downloads `lineage-<col>-data.json`; opening it shows `{ "nodes": [...], "edges": [...] }`.
5. Clicking "↓ PNG" downloads `lineage-<col>-graph.png`; the image shows the graph on a dark background.
6. Toggle "Group by table" then download JSON — the JSON reflects the collapsed node set (table-level IDs, not column-level).
7. If filter pills are visible, hide them and download JSON — hidden edges are absent.

- [ ] **Step 6: Commit**

```bash
git add frontend/components/lineage-graph.tsx
git commit -m "feat(lineage-graph): add Download dropdown for PNG and JSON export"
```
