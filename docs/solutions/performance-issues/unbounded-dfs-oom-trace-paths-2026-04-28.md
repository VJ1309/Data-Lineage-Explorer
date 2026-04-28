---
title: Unbounded DFS in trace_paths causes Railway OOM and wipes all in-memory state
date: 2026-04-28
category: docs/solutions/performance-issues/
module: lineage/engine
problem_type: performance_issue
component: tooling
symptoms:
  - Sources list appears empty after navigating between pages
  - Railway backend process silently restarts mid-session
  - Clicking the Transform tab on a column with many upstream paths triggers OOM
  - All uploaded source data lost without user action
root_cause: logic_error
resolution_type: code_fix
severity: high
tags:
  - dfs
  - oom
  - railway
  - in-memory-state
  - trace-paths
  - lineage-engine
---

# Unbounded DFS in trace_paths causes Railway OOM and wipes all in-memory state

## Problem

Removing the `max_paths` cap from `trace_paths` in `backend/lineage/engine.py` allowed the DFS
to enumerate every combinatorial path through high-fanout graphs (COALESCE × UNION fan-outs),
exhausting Railway's process memory. The killed process restarted with empty in-memory state,
making the app appear to silently lose all uploaded data on page navigation.

## Symptoms

- Sources list is empty after the user navigates between pages — the backend restarted, not the browser
- Railway logs show process restart (OOM kill) shortly after a `/lineage/paths` request
- Clicking Transform tab on any column with many upstream branches triggers the restart
- All uploaded ZIP data, parsed lineage graph, and source registry are gone after the restart

## What Didn't Work

- **Commit `07f9e13`** — reduced memory by replacing stored raw ZIP bytes with decoded `FileRecord`
  objects and added `--workers 1` Procfile to prevent multi-worker state divergence. This helped
  general memory pressure but did not address the DFS explosion triggered by `/lineage/paths`.

- **Commit `01f32a5`** — this was the regression: removed the existing `max_paths=50` guard
  entirely while redesigning the path selector UI. The assumption was that the mutable-backtracking
  rewrite would make unlimited paths safe, but that rewrite hadn't happened yet.

## Solution

**Commit `20ec998`** — two changes together:

1. **Mutable backtracking DFS** — replaced immutable list accumulation with a single shared
   `current_path` list (append on enter, pop on exit). Only one copy is made per completed path.

```python
# Before — O(depth²) memory: new list allocated at every recursion level
def dfs(node: str, steps_so_far: list[dict], visited: set[str]) -> None:
    ...
    dfs(pred, steps_so_far + [step], visited)   # ← allocates new list each call

# After — O(depth) memory: single list mutated in-place
current_path: list[dict] = []

def dfs(node: str, visited: set[str]) -> None:
    nonlocal truncated
    if truncated:
        return
    preds = get_preds(node, visited)
    if not preds:
        if current_path:
            all_paths.append(list(reversed(current_path)))  # ← one copy per completed path
            if len(all_paths) >= max_paths:
                truncated = True
        return
    for pred, tgt, edge_data in preds:
        if truncated:
            return
        current_path.append(step_dict(pred, tgt, edge_data))
        visited.add(pred)
        dfs(pred, visited)
        current_path.pop()      # ← backtrack
        visited.discard(pred)
```

2. **Reinstated `max_paths` cap** — default `max_paths=500` (10× the original 50). The
   `truncated` flag is returned to the frontend so it can surface "showing first 500."

```python
def trace_paths(raw_graph: nx.DiGraph, col_id: str, max_paths: int = 500) -> tuple[list[list[dict]], bool]:
```

## Why This Works

The original `steps_so_far + [step]` pattern creates a new list at every recursion level.
For a graph with depth D and branching factor B, this generates O(B^D) list allocations
in-flight simultaneously — each level holds all its ancestors' lists until the call returns.
On a COALESCE feeding a UNION with 5 branches at depth 10, this is millions of list objects.

The mutable backtracking pattern keeps exactly one `current_path` list alive at all times.
Memory at any point is O(depth) for the path plus O(paths_found) for completed results —
both bounded and predictable.

The `max_paths` cap is a safety valve on top: even with efficient memory use, returning
10,000 paths to the frontend is not useful. The cap converts an unbounded problem into a
bounded one at the source.

## Prevention

- **Any DFS that accumulates path steps must use mutable backtracking.** The `steps_so_far + [step]`
  pattern is safe for small graphs but silently explodes on production data with fan-out.

- **Always keep a `max_paths` cap on path enumeration.** Even after the mutable rewrite,
  removing the cap is a correctness risk. The cap should be a named parameter with a
  documented default, not hardcoded.

- **Surface truncation to the frontend.** When the cap fires, return `truncated=True` and
  show "showing first N paths" in the UI so users know they are seeing a subset.

- **The in-memory backend architecture means any OOM = total data loss.** Any unbounded
  computation that could exhaust Railway's memory should be treated as a data-loss risk,
  not just a performance concern.

## Related Issues

- `backend/lineage/engine.py` — `trace_paths` function
- Commit `01f32a5` — introduced the regression (removed cap)
- Commit `20ec998` — the fix (mutable DFS + reinstated cap)
- Commit `07f9e13` — earlier memory fix (ZIP bytes → FileRecords, Procfile workers=1)
