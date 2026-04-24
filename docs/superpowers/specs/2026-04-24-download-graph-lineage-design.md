# Download Graph Lineage — Design Spec

**Date:** 2026-04-24
**Status:** Approved

---

## Summary

Add a "Download ↓" dropdown to the lineage graph toolbar that lets users export the current graph as a PNG image or a JSON data file. All changes are client-side, contained within `frontend/components/lineage-graph.tsx`.

---

## Architecture

No new files. No backend changes. One new dependency: `html-to-image`.

### Key additions to `lineage-graph.tsx`

- `containerRef` — `useRef<HTMLDivElement>` attached to the outer `<div style={{ height: 500 }}>` ReactFlow container, used as the screenshot target.
- `isDownloading` — `boolean` state; true while `toPng` is in flight, shows a loading indicator on the button.
- `downloadOpen` — `boolean` state controlling dropdown visibility.
- `handleDownloadPng()` — calls `toPng(containerRef.current)`, triggers a browser `<a>` download. Wrapped in try/catch; on failure sets a 1.5 s "Failed" flash then resets.
- `handleDownloadJson()` — synchronously serializes `{ nodes: visibleNodes, edges: visibleEdges }` to a `Blob`, triggers download.

---

## UI

The toolbar row (currently: Filters pill · Join keys pill · Group by table pill) gains a **Download ↓** pill on the right side, styled identically to the other pills.

Clicking it toggles a small dropdown directly below the button with two items:

| Item | Label | Output |
|------|-------|--------|
| PNG  | `↓ PNG` | Screenshot of the ReactFlow canvas (current zoom/pan/filter/collapse state) |
| JSON | `↓ JSON` | `{ nodes, edges }` of the currently visible graph |

**File names:**
- PNG: `lineage-<targetColId>-graph.png`
- JSON: `lineage-<targetColId>-data.json`

Dropdown closes when: an option is selected, or the user clicks outside it (handled via a `useEffect` listening to `mousedown` on `document`).

---

## Data exported (JSON)

```json
{
  "nodes": [{ "id": "catalog.schema.table.column" }, ...],
  "edges": [
    {
      "source_col": "...",
      "target_col": "...",
      "transform_type": "passthrough",
      "expression": "...",
      "source_file": "...",
      "source_cell": null,
      "source_line": 42,
      "confidence": "certain",
      "qualified": true
    },
    ...
  ]
}
```

The export reflects the active filter/collapse state — if join keys are hidden, they are absent from the JSON.

---

## Error handling

- PNG capture is async and can fail (e.g. CORS on webfonts, canvas taint). On error: button label flashes "Failed" for 1.5 s then resets. No modal or toast — keep it lightweight.
- JSON export is synchronous and cannot fail under normal conditions; no special handling needed.

---

## Dependencies

Add `html-to-image` to `frontend/package.json`. No other new dependencies.

---

## Out of scope

- SVG export
- Backend rendering
- Exporting the tree or path tabs
- Full-graph (all columns, not just the current selection) export
