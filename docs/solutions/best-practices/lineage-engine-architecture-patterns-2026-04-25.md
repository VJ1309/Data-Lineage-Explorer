---
title: "DataLineage backend architecture patterns — engine layer, route layer, and eager query hygiene"
date: 2026-04-25
category: docs/solutions/best-practices
module: lineage
problem_type: best_practice
component: service_object
severity: medium
applies_when:
  - "Adding a new graph traversal function to the backend"
  - "Adding or modifying route handlers in api/routes.py"
  - "Adding a new React Query hook that may be expensive to call"
tags:
  - single-responsibility
  - engine-layer
  - graph-traversal
  - deduplication
  - react-query
  - lazy-activation
  - routes
related_components:
  - frontend_stimulus
---

# DataLineage backend architecture patterns — engine layer, route layer, and eager query hygiene

## Context

Three architecture violations were found during a review of the Transform tab PR: a 60-line DFS traversal implemented in the route layer instead of the engine layer; duplicated graph mutation code in two route handlers; and a React Query hook that fired on every page load regardless of which tab was active. Later cleanup also tightened source lifecycle handling: source deletion must remove warnings for that source, invalid ZIP uploads must fail fast, and source mutations must invalidate all lineage-related frontend caches. None caused visible bugs, but all created maintenance debt and latent risk.

## Guidance

### 1. Graph traversal belongs in `lineage/engine.py`, not in `api/routes.py`

`lineage/engine.py` is the single owner of all graph traversal logic. Route handlers call engine functions — they do not implement algorithms.

**Existing examples of the correct pattern:**
- `engine.upstream(graph, col_id)` — called by `GET /lineage`
- `engine.downstream(graph, col_id)` — called by `GET /lineage` and `GET /impact`
- `engine.trace_paths(raw_graph, col_id)` — called by `GET /lineage/paths`

**Wrong (route layer owns traversal):**
```python
# api/routes.py — 60 lines of DFS that belongs in engine.py
def _trace_raw_paths(raw_graph, col_id, max_paths=50):
    def get_preds(node, visited): ...
    def dfs(node, steps_so_far, visited): ...
    dfs(col_id, [], {col_id})
    return all_paths, truncated

@router.get("/lineage/paths")
def get_lineage_paths(table, column):
    paths, truncated = _trace_raw_paths(state.raw_graph, col_id)
```

**Correct (route handler delegates to engine):**
```python
# lineage/engine.py — traversal lives here
def trace_paths(raw_graph: nx.DiGraph, col_id: str, max_paths: int = 50):
    ...

# api/routes.py — handler is just dispatch + shaping
@router.get("/lineage/paths")
def get_lineage_paths(table: str, column: str):
    col_id = f"{table}.{column}"
    paths, truncated = engine_trace_paths(state.raw_graph, col_id)
    return {"target": col_id, "paths": [{"steps": p} for p in paths], "truncated": truncated}
```

Engine functions are also independently testable without going through FastAPI.

### 2. Extract shared graph mutation to a helper

`state.lineage_graph` and `state.raw_graph` are always mutated together when removing a source's contributions. Any code that mutates one must mutate the other with identical logic. Duplicated blocks will inevitably diverge. Source deletion must also remove any `state.parse_warnings` entries for that source id; otherwise `/warnings` can show stale warnings for deleted sources.

**Wrong (identical block in both `delete_source` and `refresh_source`):**
```python
old_files = entry.get("_parsed_files", set())
if old_files:
    edges = [(u, v) for u, v, d in state.lineage_graph.edges(data=True)
             if d.get("data") and d["data"].source_file in old_files]
    state.lineage_graph.remove_edges_from(edges)
    state.lineage_graph.remove_nodes_from(
        [n for n in state.lineage_graph.nodes() if state.lineage_graph.degree(n) == 0])
    # ...identical 5 lines again for raw_graph...
```

**Correct (extracted helper):**
```python
def _remove_source_files(files: set[str]) -> None:
    if not files:
        return
    edges = [(u, v) for u, v, d in state.lineage_graph.edges(data=True)
             if d.get("data") and d["data"].source_file in files]
    state.lineage_graph.remove_edges_from(edges)
    state.lineage_graph.remove_nodes_from(
        [n for n in state.lineage_graph.nodes() if state.lineage_graph.degree(n) == 0])
    raw_edges = [(u, v) for u, v, d in state.raw_graph.edges(data=True)
                 if d.get("data") and d["data"].source_file in files]
    state.raw_graph.remove_edges_from(raw_edges)
    state.raw_graph.remove_nodes_from(
        [n for n in state.raw_graph.nodes() if state.raw_graph.degree(n) == 0])
```

In `delete_source`, call `_remove_source_files(entry.parsed_files)` and then filter `state.parse_warnings` by `source_id`.

### 3. Reject invalid uploads at the route boundary

`ingestion/upload.py` owns ZIP extraction and file classification. Invalid archives should not silently produce an empty source; they should raise `ValueError("Invalid ZIP file")`. `api/routes.py` catches that and returns `400 Invalid ZIP file`.

This keeps ingestion framework-agnostic while making the API response explicit.

### 4. Gate expensive React Query calls with `enabled` + activation state

API calls that are expensive or only relevant to a specific UI state should not fire on every page load. Use React Query's `enabled` parameter combined with a boolean activation flag.

**Wrong (fires on every lineage page load):**
```typescript
// Always fires — even when user is on Graph or Tree tab
const { data: pathsData } = usePaths(table, column);
```

**Correct (fires only on first Transform tab activation):**
```typescript
// lib/hooks.ts — add enabled parameter with default
export function usePaths(table: string | null, column: string | null, enabled = true) {
  return useQuery({
    queryKey: ["paths", table, column],
    queryFn: () => api.paths(table!, column!),
    enabled: enabled && table !== null && column !== null,
  });
}

// app/lineage/page.tsx — latch on first tab activation
const [transformActivated, setTransformActivated] = useState(false);
const { data: pathsData } = usePaths(table, column, transformActivated);

<Tabs onValueChange={(v) => { if (v === "transform") setTransformActivated(true); }}>
```

The flag is a latch — it flips to `true` on first tab click and never resets. Once activated, React Query's own caching handles subsequent tab switches without re-fetching.

### 5. Invalidate all lineage data after source mutations

Source register, refresh, and delete all change the effective graph. In `frontend/lib/hooks.ts`, keep cache invalidation centralized in `invalidateLineageData()` and include every query family derived from source content:

```typescript
function invalidateLineageData(qc: ReturnType<typeof useQueryClient>) {
  qc.invalidateQueries({ queryKey: ["sources"] });
  qc.invalidateQueries({ queryKey: ["source-files"] });
  qc.invalidateQueries({ queryKey: ["tables"] });
  qc.invalidateQueries({ queryKey: ["columns"] });
  qc.invalidateQueries({ queryKey: ["lineage"] });
  qc.invalidateQueries({ queryKey: ["paths"] });
  qc.invalidateQueries({ queryKey: ["impact"] });
  qc.invalidateQueries({ queryKey: ["search"] });
  qc.invalidateQueries({ queryKey: ["warnings"] });
}
```

## Why This Matters

- **Engine/route separation**: Without it, traversal logic accumulates in route handlers, making it untestable in isolation and difficult to locate. Future traversal functions will be added to the wrong layer by default.
- **Shared mutation helper**: Two diverging copies of graph mutation code will eventually diverge — one gets a bug fix the other doesn't. The parallel `lineage_graph`/`raw_graph` state contract makes this especially risky because both must stay in sync, and warnings must not outlive their source.
- **Explicit upload errors**: A silent empty source is harder to understand than a direct 400 response for an invalid archive.
- **Lazy query activation**: Path tracing is a DFS that can produce many paths on highly connected graphs. Firing it unconditionally on every page load penalizes users who never open the Transform tab.
- **Centralized cache invalidation**: Source changes affect almost every lineage view. A single invalidation helper prevents stale catalog, graph, impact, and warning UI after mutations.

## When to Apply

- When adding a new graph traversal function — put it in `engine.py`, not `routes.py`
- When a route handler grows beyond: request validation + one engine call + response shaping
- When the same graph mutation block appears in more than one place in `routes.py`
- When changing source registration, refresh, or deletion behavior
- When adding a React Query hook that calls a computationally expensive endpoint
- When adding a new query family derived from source content; include it in `invalidateLineageData()`

## Examples

See `backend/lineage/engine.py` for canonical engine-layer traversal functions (`upstream`, `downstream`, `trace_paths`). See `backend/api/routes.py` for the `_remove_source_files` helper and the correct route-handler shape (thin dispatch + response shaping only).

## Related

- `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md` — prior fix in the `api/routes.py` / `engine.py` area
