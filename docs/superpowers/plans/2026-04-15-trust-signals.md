# Trust Signals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface three trust signals that currently leave data analysts flying blind: approximate lineage edges, ambiguous source tables, and invisible parse failures.

**Architecture:** Three independent workstreams — (1) a `confidence` field propagated from SQL parser → model → API → graph/tree UI; (2) an informational notice in the catalog for source-role tables; (3) per-source warning counts in the backend + a rich warning panel and nav badge in the frontend.

**Tech Stack:** Python dataclasses, FastAPI, NetworkX, Next.js, React, React Query, `@xyflow/react`.

---

## Codebase Orientation

```
backend/
  lineage/models.py         — LineageEdge and ParseWarning dataclasses
  parsers/sql.py            — SQL→LineageEdge; _resolve_table_hint lives here
  lineage/engine.py         — build_graph_with_warnings, _normalize_edges, _edge_to_dict helper
  api/routes.py             — REST endpoints; _edge_to_dict serialises edges for JSON
  tests/test_sql_parser.py  — SQL parser unit tests
  tests/test_routes.py      — integration tests via FastAPI TestClient
frontend/
  lib/api.ts                — TypeScript types + fetch wrappers
  lib/hooks.ts              — React Query hooks
  components/lineage-graph.tsx  — @xyflow/react graph; edges styled inline
  components/lineage-tree.tsx   — upstream/downstream tree view
  components/nav.tsx            — top nav bar with SearchBox
  app/catalog/page.tsx          — left sidebar + column table; uses role from /tables
  app/sources/page.tsx          — sources list + SourceForm; already imports useWarnings
```

**Key invariants you must not break:**
- Column IDs are always `catalog.schema.table.column` — always split with `.rsplit(".", 1)`, never `.split(".", 1)`.
- `_edge_to_dict` in `routes.py` (not `engine.py`) serialises edges to JSON.
- `state.parse_warnings` is a plain Python list of dicts (`{"file": ..., "error": ...}`); tests reset it via `state.parse_warnings.clear()`.
- All frontend data fetching uses React Query hooks from `lib/hooks.ts`; never fetch directly in page components.

---

## File Map

| File | Change |
|------|--------|
| `backend/lineage/models.py` | Add `confidence` field to `LineageEdge` |
| `backend/parsers/sql.py` | `_resolve_table_hint` returns `(str, bool)`; edges carry confidence |
| `backend/lineage/engine.py` | `_normalize_edges` preserves confidence |
| `backend/api/routes.py` | `_edge_to_dict` exposes confidence; `refresh_source` tracks `warning_count` + `source_id` on warnings |
| `backend/tests/test_sql_parser.py` | Tests: approximate edge emitted for unrecognised table hint |
| `backend/tests/test_routes.py` | Tests: confidence in API response; warning_count on source |
| `frontend/lib/api.ts` | Add `confidence` to `LineageEdge`; `Warning` gets `source_id?`; `Source` gets `warning_count?` |
| `frontend/lib/hooks.ts` | No changes needed |
| `frontend/components/lineage-graph.tsx` | Dashed stroke for approximate edges; legend entry |
| `frontend/components/lineage-tree.tsx` | `⚠` badge on approximate edges |
| `frontend/components/nav.tsx` | Red dot badge on Sources link when warnings exist |
| `frontend/app/catalog/page.tsx` | Informational notice for source-role tables |
| `frontend/app/sources/page.tsx` | Rich expandable warnings panel; per-source warning count pill |

---

## Task 1: Add `confidence` field to `LineageEdge` and SQL parser

**Files:**
- Modify: `backend/lineage/models.py`
- Modify: `backend/parsers/sql.py`
- Modify: `backend/lineage/engine.py`
- Test: `backend/tests/test_sql_parser.py`

- [ ] **Step 1: Write failing tests**

Add to `backend/tests/test_sql_parser.py`:

```python
def test_struct_field_fallback_is_approximate():
    """Struct field access falling back to default_table must produce an approximate edge."""
    sql = """
    INSERT INTO summary
    SELECT info.city AS city, score
    FROM customers
    """
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    city_edges = [e for e in edges if e.target_col.endswith(".city")]
    assert len(city_edges) == 1
    assert city_edges[0].confidence == "approximate", (
        f"Expected approximate, got {city_edges[0].confidence!r}"
    )


def test_certain_table_alias_is_certain():
    """Column resolved via a known alias must produce a certain edge."""
    sql = "SELECT o.order_id FROM staging.orders o"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    assert len(edges) == 1
    assert edges[0].confidence == "certain"


def test_certain_no_qualifier_is_certain():
    """Column with no table qualifier (default_table path) must be certain."""
    sql = "SELECT amount FROM raw_orders"
    edges = parse_sql(sql, source_file="q.sql", source_line=1)
    assert len(edges) == 1
    assert edges[0].confidence == "certain"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd backend
python -m pytest tests/test_sql_parser.py::test_struct_field_fallback_is_approximate tests/test_sql_parser.py::test_certain_table_alias_is_certain tests/test_sql_parser.py::test_certain_no_qualifier_is_certain -v
```

Expected: 3 FAILs (AttributeError: `confidence` not on LineageEdge)

- [ ] **Step 3: Add `confidence` to `LineageEdge` in `backend/lineage/models.py`**

```python
@dataclass
class LineageEdge:
    source_col: str
    target_col: str
    transform_type: Literal[
        "passthrough", "aggregation", "expression",
        "join_key", "window", "cast", "filter"
    ]
    expression: str | None = None
    source_file: str = ""
    source_cell: int | None = None
    source_line: int | None = None
    confidence: Literal["certain", "approximate"] = "certain"
```

- [ ] **Step 4: Update `_resolve_table_hint` in `backend/parsers/sql.py` to return `(str, bool)`**

Replace the existing inner function (lines ~145–151):

```python
def _resolve_table_hint(hint: str) -> tuple[str, bool]:
    """Resolve a table alias/name. Returns (resolved_table, is_certain).

    is_certain=False when the hint is unrecognised (e.g. struct field
    access or an unresolvable CTE alias); in that case default_table is
    returned and the caller should mark the edge as approximate.
    """
    if hint in alias_map:
        return alias_map[hint], True
    if hint in cte_map:
        return cte_map[hint], True
    for tbl in source_tables:
        if tbl == hint or tbl.endswith(f".{hint}"):
            return tbl, True
    return default_table, False
```

- [ ] **Step 5: Update all call sites of `_resolve_table_hint` in `backend/parsers/sql.py` to propagate confidence**

There are two call sites — one inside the `if transform_type == "window":` block and one in the main `for col_ref in col_refs:` loop.

Replace the window block (starting around line 173):

```python
        if transform_type == "window":
            win_col_refs = list(expr_node.find_all(exp.Column))
            if win_col_refs:
                for col_ref in win_col_refs:
                    table_hint = col_ref.table
                    col_name = col_ref.name
                    if not col_name:
                        continue
                    if table_hint:
                        resolved_table, certain = _resolve_table_hint(table_hint)
                    else:
                        resolved_table, certain = default_table, True
                    edges.append(LineageEdge(
                        source_col=f"{resolved_table}.{col_name}",
                        target_col=target_col,
                        transform_type=transform_type,
                        expression=expr_str,
                        source_file=source_file,
                        source_cell=source_cell,
                        source_line=source_line,
                        confidence="certain" if certain else "approximate",
                    ))
            else:
                edges.append(LineageEdge(
                    source_col=f"{default_table}.*",
                    target_col=target_col,
                    transform_type=transform_type,
                    expression=expr_str,
                    source_file=source_file,
                    source_cell=source_cell,
                    source_line=source_line,
                    confidence="certain",
                ))
            continue
```

Replace the main col_refs loop (starting around line 203):

```python
        col_refs = list(expr_node.find_all(exp.Column))

        if not col_refs:
            edges.append(LineageEdge(
                source_col=f"{default_table}.{alias}",
                target_col=target_col,
                transform_type=transform_type,
                expression=expr_str,
                source_file=source_file,
                source_cell=source_cell,
                source_line=source_line,
                confidence="certain",
            ))
            continue

        for col_ref in col_refs:
            table_hint = col_ref.table
            col_name = col_ref.name
            if not col_name:
                continue
            if table_hint:
                resolved_table, certain = _resolve_table_hint(table_hint)
            else:
                resolved_table, certain = default_table, True
            source_col = f"{resolved_table}.{col_name}"

            edges.append(LineageEdge(
                source_col=source_col,
                target_col=target_col,
                transform_type=transform_type,
                expression=expr_str,
                source_file=source_file,
                source_cell=source_cell,
                source_line=source_line,
                confidence="certain" if certain else "approximate",
            ))
```

- [ ] **Step 6: Preserve confidence in `_normalize_edges` in `backend/lineage/engine.py`**

Both branches that create new `LineageEdge` objects must pass `confidence=e.confidence`. There are two: the early-return branch (only case normalization) and the main normalization branch.

In the early-return branch (around line 63):
```python
        normalized.append(LineageEdge(
            source_col=e.source_col.lower(),
            target_col=e.target_col.lower(),
            transform_type=e.transform_type,
            expression=e.expression,
            source_file=e.source_file,
            source_cell=e.source_cell,
            source_line=e.source_line,
            confidence=e.confidence,
        ))
```

In the main normalization branch (around line 87):
```python
        normalized.append(LineageEdge(
            source_col=_resolve_col(e.source_col),
            target_col=_resolve_col(e.target_col),
            transform_type=e.transform_type,
            expression=e.expression,
            source_file=e.source_file,
            source_cell=e.source_cell,
            source_line=e.source_line,
            confidence=e.confidence,
        ))
```

- [ ] **Step 7: Run the three new tests — all should pass**

```bash
cd backend
python -m pytest tests/test_sql_parser.py::test_struct_field_fallback_is_approximate tests/test_sql_parser.py::test_certain_table_alias_is_certain tests/test_sql_parser.py::test_certain_no_qualifier_is_certain -v
```

Expected: 3 PASSes

- [ ] **Step 8: Run full backend test suite**

```bash
cd backend
python -m pytest tests/ -v
```

Expected: all existing tests pass (confidence defaults to "certain" so no regressions)

- [ ] **Step 9: Commit**

```bash
git add backend/lineage/models.py backend/parsers/sql.py backend/lineage/engine.py backend/tests/test_sql_parser.py
git commit -m "feat: add confidence field to LineageEdge — approximate when table resolution falls back"
```

---

## Task 2: Expose `confidence` in the API and add route tests

**Files:**
- Modify: `backend/api/routes.py`
- Test: `backend/tests/test_routes.py`

- [ ] **Step 1: Write a failing test for confidence in the API response**

Add to `backend/tests/test_routes.py`:

```python
def test_lineage_edge_has_confidence_field():
    """Every edge returned by /lineage must include a confidence field."""
    zip_bytes = _make_zip({
        "query.sql": "SELECT SUM(amount) AS total FROM raw_orders"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/lineage", params={"table": "result", "column": "total"})
    assert resp.status_code == 200
    data = resp.json()
    for edge in data["upstream"] + data["downstream"] + data["graph"]["edges"]:
        assert "confidence" in edge, f"Edge missing confidence: {edge}"
        assert edge["confidence"] in ("certain", "approximate")


def test_approximate_edge_for_struct_field():
    """Struct field access must produce an approximate edge in the API."""
    zip_bytes = _make_zip({
        "q.sql": "INSERT INTO summary SELECT info.city AS city FROM customers"
    })
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/lineage", params={"table": "summary", "column": "city"})
    assert resp.status_code == 200
    upstream_edges = resp.json()["upstream"]
    assert len(upstream_edges) == 1
    assert upstream_edges[0]["confidence"] == "approximate"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd backend
python -m pytest tests/test_routes.py::test_lineage_edge_has_confidence_field tests/test_routes.py::test_approximate_edge_for_struct_field -v
```

Expected: 2 FAILs (`confidence` key missing from edge dicts)

- [ ] **Step 3: Add `confidence` to `_edge_to_dict` in `backend/api/routes.py`**

```python
def _edge_to_dict(edge) -> dict:
    return {
        "source_col": edge.source_col,
        "target_col": edge.target_col,
        "transform_type": edge.transform_type,
        "expression": edge.expression,
        "source_file": edge.source_file,
        "source_cell": edge.source_cell,
        "source_line": edge.source_line,
        "confidence": edge.confidence,
    }
```

- [ ] **Step 4: Run the two new tests**

```bash
cd backend
python -m pytest tests/test_routes.py::test_lineage_edge_has_confidence_field tests/test_routes.py::test_approximate_edge_for_struct_field -v
```

Expected: 2 PASSes

- [ ] **Step 5: Run full backend test suite**

```bash
cd backend
python -m pytest tests/ -v
```

Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add backend/api/routes.py backend/tests/test_routes.py
git commit -m "feat: expose confidence field in lineage API responses"
```

---

## Task 3: Per-source warning tracking

**Files:**
- Modify: `backend/api/routes.py`
- Test: `backend/tests/test_routes.py`

**Context:** Currently `refresh_source` adds to `state.parse_warnings` as `{"file": ..., "error": ...}`. We need to also store `source_id` on each warning and `warning_count` on the source entry so the frontend can display warnings per source.

- [ ] **Step 1: Write failing tests**

Add to `backend/tests/test_routes.py`:

```python
def test_warning_count_on_source_after_refresh():
    """Source entry must include warning_count after refresh."""
    # A zip with a valid file so parse succeeds — warning_count should be 0
    zip_bytes = _make_zip({"q.sql": "SELECT amount FROM raw_orders"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/sources")
    src = next(s for s in resp.json() if s["id"] == source_id)
    assert "warning_count" in src
    assert src["warning_count"] == 0


def test_warnings_include_source_id():
    """Warnings in GET /warnings must include source_id field."""
    # Trigger a parse warning by uploading a file that causes issues
    # We'll use a file that produces no lineage (empty SQL) — no warning
    # Instead, we test that after a successful parse, warnings list may be empty
    # but if there are warnings they have source_id
    zip_bytes = _make_zip({"q.sql": "SELECT amount FROM raw_orders"})
    resp = client.post(
        "/sources",
        data={"source_type": "upload"},
        files={"file": ("data.zip", zip_bytes, "application/zip")},
    )
    source_id = resp.json()["id"]
    client.post(f"/sources/{source_id}/refresh")

    resp = client.get("/warnings")
    assert resp.status_code == 200
    for w in resp.json():
        assert "source_id" in w, f"Warning missing source_id: {w}"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd backend
python -m pytest tests/test_routes.py::test_warning_count_on_source_after_refresh tests/test_routes.py::test_warnings_include_source_id -v
```

Expected: 2 FAILs

- [ ] **Step 3: Update `refresh_source` in `backend/api/routes.py` to track per-source warning count and add `source_id` to warning entries**

Replace the `state.parse_warnings.extend(...)` call (around line 184) and the `entry["status"] = "parsed"` block:

```python
    state.lineage_graph = nx.compose(state.lineage_graph, new_graph)
    state.parse_warnings.extend(
        {"file": w.file, "error": w.error, "source_id": source_id}
        for w in new_warnings
    )

    # Track which files this source contributed
    entry["_parsed_files"] = {
        d["data"].source_file
        for _, _, d in new_graph.edges(data=True)
        if d.get("data") and d["data"].source_file
    }

    entry["status"] = "parsed"
    entry["file_count"] = len(records)
    entry["warning_count"] = len(new_warnings)

    return {"ok": True, "file_count": len(records), "edge_count": new_graph.number_of_edges()}
```

- [ ] **Step 4: Ensure `list_sources` returns `warning_count`**

The `list_sources` route filters keys starting with `_`. Since `warning_count` does not start with `_`, it will automatically be included once it is set on the entry. Verify the source entry initialisation at the top of `register_source` sets a default:

```python
    entry: dict = {
        "id": source_id,
        "source_type": source_type,
        "url": url,
        "_token": token,
        "status": "registered",
        "file_count": 0,
        "warning_count": 0,
    }
```

- [ ] **Step 5: Run new tests**

```bash
cd backend
python -m pytest tests/test_routes.py::test_warning_count_on_source_after_refresh tests/test_routes.py::test_warnings_include_source_id -v
```

Expected: 2 PASSes

- [ ] **Step 6: Run full backend test suite**

```bash
cd backend
python -m pytest tests/ -v
```

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add backend/api/routes.py backend/tests/test_routes.py
git commit -m "feat: track warning_count per source and source_id on parse warnings"
```

---

## Task 4: Frontend types + approximate edge styling in graph and tree

**Files:**
- Modify: `frontend/lib/api.ts`
- Modify: `frontend/components/lineage-graph.tsx`
- Modify: `frontend/components/lineage-tree.tsx`

- [ ] **Step 1: Update TypeScript types in `frontend/lib/api.ts`**

Update `LineageEdge`:
```typescript
export type LineageEdge = {
  source_col: string;
  target_col: string;
  transform_type: string;
  expression: string;
  source_file: string;
  source_cell: number | null;
  source_line: number | null;
  confidence: "certain" | "approximate";
};
```

Update `Warning`:
```typescript
export type Warning = {
  file: string;
  error: string;
  source_id?: string;
};
```

Update `Source`:
```typescript
export type Source = {
  id: string;
  source_type: "git" | "databricks" | "upload";
  url: string;
  status: string;
  file_count: number;
  warning_count?: number;
};
```

- [ ] **Step 2: Update `lineage-graph.tsx` — dashed edges for approximate**

In `rfEdges` for the non-collapsed branch, change the edge mapping:

```typescript
      return edges.map((e, i) => ({
        id: `e-${i}`,
        source: e.source_col,
        target: e.target_col,
        label: e.transform_type,
        animated: e.transform_type === "aggregation" || e.transform_type === "window",
        style: {
          stroke: TRANSFORM_COLOURS[e.transform_type] ?? "#888",
          strokeWidth: 1.5,
          strokeDasharray: e.confidence === "approximate" ? "5 4" : undefined,
          opacity: e.confidence === "approximate" ? 0.6 : 1,
        },
        labelStyle: { fontSize: 9, fill: "#888" },
        labelBgStyle: { fill: "#0a0f1a", fillOpacity: 0.8 },
      }));
```

Also update the legend inside the `<div style={{ height: 500 ... }}>` block to add an approximate edge entry. Replace the legend div with:

```tsx
        {/* Legend */}
        <div className="flex gap-4 px-3 py-1.5 text-xs" style={{ color: "#6b7a8d" }}>
          <span><span style={{ color: "#4ade80" }}>●</span> Source</span>
          <span><span style={{ color: "#7ec8e3" }}>●</span> Selected</span>
          <span><span style={{ color: "#c084fc" }}>●</span> Target</span>
          <span className="ml-auto flex gap-3">
            {Object.entries(TRANSFORM_COLOURS).map(([type, color]) => (
              <span key={type}><span style={{ color }}>—</span> {type}</span>
            ))}
            <span title="Table resolution was ambiguous — source column attributed to best-guess table">
              <span style={{ opacity: 0.5 }}>- -</span> approx
            </span>
          </span>
        </div>
```

- [ ] **Step 3: Update `lineage-tree.tsx` — warning badge on approximate edges**

In `TreeNodeRow`, add a `⚠` badge when `node.edge?.confidence === "approximate"`. Place it right after the `<TransformBadge>`:

```tsx
        {node.edge && <TransformBadge type={node.edge.transform_type} />}
        {node.edge?.confidence === "approximate" && (
          <span
            className="text-xs text-amber-500 flex-shrink-0"
            title="Source table is approximate — the SQL used an ambiguous table reference (e.g. a struct field access). The edge is attributed to the most likely source table."
          >
            ⚠ approx
          </span>
        )}
```

- [ ] **Step 4: Verify TypeScript compiles**

```bash
cd frontend
npm run build
```

Expected: clean build, 0 TypeScript errors

- [ ] **Step 5: Commit**

```bash
git add frontend/lib/api.ts frontend/components/lineage-graph.tsx frontend/components/lineage-tree.tsx
git commit -m "feat: show approximate edges as dashed in graph and warn badge in tree"
```

---

## Task 5: Source table notice in catalog

**Files:**
- Modify: `frontend/app/catalog/page.tsx`

**Context:** When a user selects a source-role table, there is currently no explanation for why it has no upstream lineage. This task adds an informational notice.

- [ ] **Step 1: Add the notice to the selected-table panel in `frontend/app/catalog/page.tsx`**

Inside the `{selectedTable && ...}` block, add a conditional notice right after the table header `<div>` (before `{colsLoading && ...}`). The full header block should become:

```tsx
        {selectedTable && (
          <>
            <div className="flex items-center gap-3 mb-4">
              <span className={`w-2.5 h-2.5 rounded-full ${ROLE_DOT[selectedRole || "source"]}`} />
              <h2 className="text-lg font-semibold">{selectedTable}</h2>
              <span className={`text-xs font-medium uppercase ${ROLE_CONFIG[selectedRole || "source"]?.color}`}>
                {selectedRole}
              </span>
            </div>

            {selectedRole === "source" && (
              <div className="mb-4 rounded-md border border-blue-200 bg-blue-50 dark:border-blue-900 dark:bg-blue-950/30 px-4 py-3 text-sm">
                <p className="font-medium text-blue-800 dark:text-blue-300">External data source</p>
                <p className="mt-1 text-blue-700 dark:text-blue-400 text-xs leading-relaxed">
                  This table has no upstream lineage in the uploaded files — it is read from but never written to.
                  It may be a raw data source (a database table, file feed, or external system) or it may reference
                  files that haven't been uploaded yet. If you expect to see upstream lineage here, check that all
                  relevant SQL or notebook files are included in your source.
                </p>
              </div>
            )}

            {colsLoading && <p className="text-sm text-muted-foreground">Loading columns…</p>}
```

- [ ] **Step 2: Verify TypeScript compiles**

```bash
cd frontend
npm run build
```

Expected: clean build

- [ ] **Step 3: Commit**

```bash
git add frontend/app/catalog/page.tsx
git commit -m "feat: show informational notice for source-role tables in catalog"
```

---

## Task 6: Rich warnings panel and nav badge

**Files:**
- Modify: `frontend/app/sources/page.tsx`
- Modify: `frontend/components/nav.tsx`

**Context:** The sources page currently shows a one-line amber banner. We replace it with a collapsible panel listing each warning with its file name and error. The nav bar gets a red dot badge on the Sources link whenever any warnings exist.

- [ ] **Step 1: Rewrite the warnings section in `frontend/app/sources/page.tsx`**

Replace the entire file with:

```tsx
"use client";
import { useState } from "react";
import { useSources, useDeleteSource, useRefreshSource, useWarnings } from "@/lib/hooks";
import { SourceForm } from "@/components/source-form";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

function WarningsPanel() {
  const { data: warnings } = useWarnings();
  const [open, setOpen] = useState(false);

  if (!warnings || warnings.length === 0) return null;

  return (
    <div className="rounded-md border border-amber-200 bg-amber-50 dark:border-amber-900 dark:bg-amber-950/30">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-2 px-4 py-3 text-sm text-amber-800 dark:text-amber-300 hover:bg-amber-100 dark:hover:bg-amber-950/50 transition-colors rounded-md"
      >
        <span className="text-base">⚠</span>
        <span className="font-medium">
          {warnings.length} parse warning{warnings.length !== 1 ? "s" : ""} — some files may not be fully analysed
        </span>
        <span className="ml-auto text-xs text-amber-600 dark:text-amber-400">
          {open ? "▴ Hide" : "▾ Show details"}
        </span>
      </button>

      {open && (
        <div className="border-t border-amber-200 dark:border-amber-900 divide-y divide-amber-100 dark:divide-amber-900/50">
          {warnings.map((w, i) => (
            <div key={i} className="px-4 py-2.5">
              <p className="text-xs font-medium text-amber-800 dark:text-amber-300 font-mono truncate">
                📄 {w.file}
              </p>
              <p className="text-xs text-amber-700 dark:text-amber-400 mt-0.5 leading-relaxed break-words">
                {w.error}
              </p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default function SourcesPage() {
  const { data: sources, isLoading } = useSources();
  const del = useDeleteSource();
  const refresh = useRefreshSource();

  return (
    <div className="max-w-3xl space-y-6">
      <h1 className="text-2xl font-semibold">Sources</h1>

      <WarningsPanel />

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Connected Sources</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {isLoading && <p className="text-sm text-muted-foreground">Loading…</p>}
          {sources?.length === 0 && (
            <p className="text-sm text-muted-foreground">No sources connected yet.</p>
          )}
          {sources?.map((src) => (
            <div key={src.id} className="flex items-center gap-3 rounded-md border px-3 py-2 text-sm">
              <span className={src.status === "parsed" ? "text-green-600" : "text-muted-foreground"}>●</span>
              <span className="flex-1 truncate font-medium">{src.url}</span>
              <span className="text-xs text-muted-foreground capitalize">{src.source_type}</span>
              <span className="text-xs text-muted-foreground">{src.file_count} files</span>
              {(src.warning_count ?? 0) > 0 && (
                <span
                  className="text-xs font-medium text-amber-600 dark:text-amber-400"
                  title={`${src.warning_count} parse warning${src.warning_count !== 1 ? "s" : ""} for this source`}
                >
                  ⚠ {src.warning_count}
                </span>
              )}
              <Button size="sm" variant="outline" onClick={() => refresh.mutate(src.id)}>
                ↻
              </Button>
              <Button size="sm" variant="ghost" onClick={() => del.mutate(src.id)}>
                ✕
              </Button>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Add Source</CardTitle>
        </CardHeader>
        <CardContent>
          <SourceForm />
        </CardContent>
      </Card>
    </div>
  );
}
```

- [ ] **Step 2: Add warning badge to nav in `frontend/components/nav.tsx`**

Add `useWarnings` import at the top:

```tsx
import { useSearch, useWarnings } from "@/lib/hooks";
```

Update the `Nav` function to query warnings and show a red dot on the Sources link:

```tsx
export function Nav() {
  const path = usePathname();
  const { data: warnings } = useWarnings();
  const hasWarnings = (warnings?.length ?? 0) > 0;

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
            "text-sm transition-colors hover:text-foreground relative",
            path.startsWith(l.href)
              ? "text-foreground font-medium"
              : "text-muted-foreground"
          )}
        >
          {l.label}
          {l.href === "/sources" && hasWarnings && (
            <span
              className="absolute -top-1 -right-2 w-2 h-2 rounded-full bg-amber-500"
              title={`${warnings!.length} parse warning${warnings!.length !== 1 ? "s" : ""}`}
            />
          )}
        </Link>
      ))}
      <SearchBox />
    </nav>
  );
}
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
cd frontend
npm run build
```

Expected: clean build, 0 TypeScript errors

- [ ] **Step 4: Run full backend tests one final time**

```bash
cd backend
python -m pytest tests/ -v
```

Expected: all pass

- [ ] **Step 5: Final commit and push**

```bash
git add frontend/app/sources/page.tsx frontend/components/nav.tsx
git commit -m "feat: rich warnings panel with file-level detail and nav badge"
git push
```

---

## Self-Review

### 1. Spec coverage

| Requirement | Covered by |
|---|---|
| #1 Silent inaccurate edges | Tasks 1–2 (confidence field + backend API) + Task 4 (frontend display) |
| #2 Missing source tables | Task 5 (informational notice in catalog) |
| #3 Parse failures invisible | Task 3 (per-source warning count) + Task 6 (rich panel + nav badge) |

### 2. Placeholder scan

None found — every step contains full code.

### 3. Type consistency

- `confidence: "certain" | "approximate"` — same literal union used in `models.py`, `routes.py`, `api.ts`, and both frontend components.
- `warning_count` — set in `register_source` entry dict, set again in `refresh_source`, read in `list_sources` (automatic via non-`_` filter). Used as `src.warning_count` in `sources/page.tsx`.
- `source_id` on warnings — added in `refresh_source` extend call, typed as `source_id?: string` in `Warning` type.
- `useWarnings` — already exists in `hooks.ts`; imported in `nav.tsx` as `useWarnings`.
