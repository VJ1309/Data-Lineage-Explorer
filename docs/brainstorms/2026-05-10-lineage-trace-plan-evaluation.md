---
title: "Evaluation of: feat: Lineage Trace plan"
type: evaluation
status: draft
date: 2026-05-10
target_plan: docs/plans/2026-05-10-007-feat-lineage-trace-plan.md
---

# Evaluation: Lineage Trace plan

Critical review of `docs/plans/2026-05-10-007-feat-lineage-trace-plan.md`. Strong on diagnosis and layering; over-scoped for the evidence currently available. Recommendation below shrinks v1 by two units while preserving the BA-facing value.

---

## What the plan gets right

- **Right diagnosis.** The data is already in `raw_graph` via the `__filter__` / `__joinkey__` / `__qualify__` / `__having__` synthetic-column edges. The inspector just doesn't pull them in. Surfacing existing parser output is a much smaller bet than building new lineage analysis.
- **Engine-vs-route discipline.** `lineage_trace` lives in `lineage/engine.py`, returns a typed dataclass, route reshapes via `_trace_step_to_dict`. Mirrors `column_metadata` / `trace_paths` exactly. No re-litigating the layering precedent.
- **The load-bearing decision is captured in an ADR.** Walking `raw_graph` (not `lineage_graph`) is the one call that, if wrong, makes the whole feature wrong. ADR-0001 codifies it. Good.
- **Backwards-compat is pinned.** `statement_id` is internal; existing endpoints stay byte-identical.
- **Phase-commit-friendly.** Five units, clear dependency chain, every unit ships with tests. Matches the project's `long-running-work.md` rule.

---

## Fixture-grounded findings

The plan asserts BA value but doesn't reckon with what the existing fixture actually covers. Walking `sample_data/` directly:

| Fixture | Statements | Filters present | What v1 needs to handle |
|---------|------------|-----------------|--------------------------|
| `orders_agg.sql` | 1 INSERT | `WHERE status='completed'` (in INSERT body) | Sibling-synthetic on target table |
| `finance_mart.sql` | 2 CTEs + 1 INSERT | `WHERE total_revenue > 0` (in CTE), JOIN on `customer_id` | **Temp-view / CTE rollup** |
| `transform_orders.py` | 3 writes, 1 JOIN | **None** | JOIN keys (already covered by SQL parser via `spark.sql`? — No, this is pure DataFrame API) |

Three implications:

1. **A naive "sibling synthetics on target table" v0.5 is demonstrably insufficient.** On `mart_finance.revenue`, the most informative filter (`total_revenue > 0`) lives inside the `customer_revenue` CTE. Its `__filter__` edge targets `customer_revenue.__filter__`, not `mart_finance.__filter__`. The simpler version would silently drop it.

2. **`statement_id` (U1) does no work the fixture exercises.** `statement_id`'s job is to disambiguate (a) multi-writer target tables — none in fixture; or (b) multi-writer temp views — none in fixture (CTEs are single-writer by definition). For every fixture case, grouping by source-table boundary in `raw_graph` produces the correct Trace Step grouping.

3. **PySpark predicate emission (U2) has zero fixture coverage.** `transform_orders.py` has no `.filter()` / `.where()` calls at all. U2 would ship to satisfy a speculative case with no validation harness.

---

## Recommendation: ship v0.7

Drop U1 and U2 from v1. Re-shape U3 to group by source-table boundary instead of `statement_id`. Keep U4 (endpoint), U5 (frontend), U6 (docs).

### What v0.7 looks like

- **No parser changes.** `LineageEdge` stays as-is. SQL parser keeps emitting `__filter__` / `__joinkey__` / `__qualify__` / `__having__` synthetics targeting the SELECT's target table or temp-view name (current behavior).
- **Engine: `lineage_trace(graph, raw_graph, table, column) -> list[TraceStep]`.** Walks `raw_graph` upstream from the column. For each immediate-writer source-table, gathers the `__filter__` / `__joinkey__` / `__qualify__` / `__having__` edges sharing that target-table. If a source is a temp view, recurses one hop and rolls up the temp view's filters with `via_temp_views` annotation.
- **Endpoint, frontend, docs: unchanged from the plan.**
- **`TraceStep` shape: unchanged except no `statement_id` field.** Steps are keyed by `(source_table, source_file)` rather than statement_id; this is the natural grouping the existing parser output supports.

### What v0.7 explicitly defers

- **Multi-INSERT-into-same-table scoping.** If a user uploads ETL with 30 daily-append INSERTs into `events`, all 30 WHEREs collapse into one Trace Step. This is wrong — but no fixture has it today. Add `statement_id` (current U1) and per-statement grouping the first time a real fixture reproduces the bug.
- **PySpark filter / join-key emission.** Current parser silently drops `.filter()` / `.where()` / `.join()` ON clauses. The Trace card will show "no filters/joins recorded for this PySpark write." Add U2 the first time someone uploads a real PySpark codebase where this is the gap. With actual fixtures, U2's predicate-extraction strategy can be designed against real predicate shapes instead of guessed ones.

### Concrete deferral triggers

- **U1 trigger:** A bug report (or user-uploaded fixture) where `lineage_trace(table, column)` returns a Trace Step that mixes filters from two distinct INSERT statements into the same target table. Until then, source-table-boundary grouping is correct on every fixture we have.
- **U2 trigger:** A bug report (or user-uploaded fixture) where a PySpark `.filter()` / `.where()` call is missing from the Trace Step its column was written through.

---

## Smaller items worth folding into the plan regardless

These apply whether you ship v0.7 or the full plan as drafted.

### 1. PySpark predicate extraction strategy (if/when U2 ships)

The drafted U2 unparses the predicate AST to a string and regex-extracts `F.col("name")` patterns. This silently misses `df["x"]`, `df.x`, `F.expr("x = 5")`, plain string predicates, wrapped expressions like `F.coalesce(F.col("x"), F.lit(0)) > 5`. The mitigation in §Risks ("emit a parser warning") doesn't reach the BA — they see a Trace Step with fewer upstream chips than they expected and don't know to distrust it.

Stronger approach: walk the predicate AST directly via `ast.NodeVisitor`. Captures `Subscript(Name)`, `Attribute(Name)`, `Call(F.col)` reliably. More upfront work, far fewer false negatives.

If extraction returns zero source columns, render the Trace Step element with the predicate text and an explicit "couldn't link source columns" badge. Honest beats silent.

### 2. Multi-writer Trace Step truncation (applies to U3 v0.7 or full)

The plan has no truncation discipline at the trace level. `trace_paths()` has one. `lineage_trace()` should too — at minimum a configurable `max_steps_per_column`, ideally with a "more results" affordance. Multi-writer columns (when they show up) need this; v0.7 won't have them but it's cheap to add the cap up front.

### 3. Snapshot test for byte-identicality is brittle

U4's full-response snapshot will trip on every NetworkX dict-ordering change, every unrelated field addition to other endpoints, every JSON serialization quirk. A targeted assertion is more durable:

```python
for endpoint in unchanged_endpoints:
    body = client.get(endpoint).json()
    assert "statement_id" not in json.dumps(body)
```

(For v0.7 there's nothing to leak, so this concern goes away entirely.)

### 4. The `file:line` chip's actual UX is unspecified

U5 says "no inline view in v1; just visually communicates jumpability." But the source file lives in `state.source_registry` (uploaded ZIP) — there's no editor to jump to. Either the chip is non-functional (decorative) or it opens a modal showing the file at that line. Decide explicitly before U5 implementation, not during.

### 5. `_make_edge` helper to bound parser carrying cost (deferred until U1 ships)

If/when U1 lands, every parser emitting an edge has to remember `statement_id`, `source_file`, `source_cell`, `source_line`. Consolidate these into a `_make_edge(node, source_col, target_col, transform_type, expression)` helper at the top of each parser file before adding `statement_id` as the seventh argument every emit site has to thread.

---

## Single sharpest probe (preserved from review)

Before locking the next move: **does the current `sample_lineage.zip` fixture contain anywhere a target table with two distinct writers that need to be separated?** If yes, U1 is load-bearing for v1 and v0.7 is wrong. If no — and based on reading the SQL files, the answer is no — U1 is speculative for v1 and v0.7 is the right ship.

---

## If proceeding with v0.7

Suggested follow-up:

1. Update `docs/plans/2026-05-10-007-feat-lineage-trace-plan.md` — strike U1 and U2; mark them as "deferred to follow-up plan, triggered by [observed gap]." Re-shape U3 description to use source-table-boundary grouping. R1 and R2 in Requirements Trace become deferred requirements.
2. Update ADR-0001 if the rationale shifts (it shouldn't — `raw_graph` walk is still load-bearing).
3. Implement U3 (slimmed) → U4 → U5 → U6.

Estimated scope reduction: from 5 units / 2-3 PRs to 4 units / 1-2 PRs. PySpark side is documented as "no `.filter()` capture today" rather than half-implemented.
