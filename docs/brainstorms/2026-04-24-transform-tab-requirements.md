---
date: 2026-04-24
topic: transform-tab-redesign
---

# Transform Tab: Unified End-to-End Transformation View

## Problem Frame

The Lineage section's Code and Path tabs address the same need — understanding how a column is transformed — but neither does it well. The Code tab shows one upstream hop at a time with no chain view. The Path tab shows the full chain but with minimal code detail. Users must flip between tabs to piece together a full picture, and it is unclear which tab to use for what purpose.

Note: this scope addresses chain navigation and structure, not code comprehension depth. Full surrounding file context (N lines around each expression) is a confirmed known gap — it is a follow-up task.

## Requirements

**Tab Restructuring**

- R1. Remove the Code tab and Path tab from the lineage tab bar; replace them with a single **Transform** tab.
- R2. The tab bar becomes: Graph · Tree · Transform (three tabs total).

**Chain View**

- R3. The Transform tab is **upstream-only**: it displays all hops from raw source columns to the selected target column. Downstream direction (what the selected column feeds into) is out of scope — the Impact tab covers that.
- R4. Each hop is rendered as a card containing: source column name with table prefix, transform type badge, the expression with syntax highlighting (SQL or Python, auto-detected by file extension), and the source file name + line reference. When expression, source_file, and source_line are all null (wildcard or temp-view synthesized hop), the code area is replaced with a dimmed label: "Structural hop — no expression recorded". The file/line row is omitted in this case.
- R5. When multiple transformation paths exist, a path selector appears at the top of the tab as labelled pills. Selecting a pill updates the chain view. Pills use source table name (e.g. "via events"). If two pills would be identically labelled, append a numeric suffix: "via events (1)", "via events (2)". Pills wrap via flex-wrap when they exceed available width.
- R6. Paths truncated by the backend (too many paths, or reaching the 10-hop depth limit) show a visible note indicating more paths may exist. Depth-truncated terminal nodes show a distinct indicator (e.g. "↑ may continue deeper") so they are not mistaken for true source columns.

**Summary Banner**

- R7. A summary sentence appears at the top of the Transform tab, above the chain and any path selector. It is derived deterministically from chain data using generic transform type labels — no AI, no expression parsing. Examples:
  - *"Aggregated from `events.revenue` across 3 hops · `etl_pipeline.sql`"*
  - *"Passed through unchanged from `raw.amount` (2 hops)"*
  - *"Derived by expression from `orders.status` · `transforms.py`"*
  For fan-in columns (JOIN-derived or multi-source), the first upstream source in path order fills the "from X" slot; the source file of the last non-passthrough hop fills the file slot.
- R8. For passthrough-only chains the summary reads "Passed through unchanged from …". For mixed chains, the **last non-passthrough step** in the chain determines the dominant transform type label. When a column has no upstream (R11), the summary is omitted.

**Code Display**

- R9. Expression blocks are expanded by default (no click to reveal). Each expression block is capped at approximately 6 lines (~120px) with vertical scroll inside the block — long expressions do not push subsequent chain cards off-screen.
- R10. Passthrough hops show "No transformation — passed through unchanged" in place of an expression block. No code block is rendered for passthrough steps.

**Empty, Edge, and Loading States**

- R11. If the column has no upstream, the Transform tab shows: "This is a source column — no upstream transformations." The tab is still visible and accessible. The summary banner is omitted.
- R12. During the `/lineage/paths` fetch, the Transform tab renders a loading state (skeleton or spinner) consistent with the app's existing loading patterns. If the fetch fails, an inline error message is shown.

**UX Quality**

- R13. The Transform tab UI is professional-grade, consistent with the app's dark theme. Minimum bar: hop cards have consistent spacing and typography with the rest of the lineage section; all states (loading, empty/source, error, chain with single path, chain with multiple paths) are rendered — none are left blank. A functional-but-rough implementation is not done.

## Visual Aid

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  ⬡ Graph  ≡ Tree  </> Transform                                              │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                              │
│  Aggregated from events.revenue across 3 hops · etl_pipeline.sql            │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Paths: [via events] [via raw_events]    (only when >1 path)                 │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────┐                │
│  │  events · revenue              [Source]                  │                │
│  └────────────────────────────┬─────────────────────────────┘                │
│                               │ [Passthrough]  etl/stage.sql · line 12       │
│                               │  No transformation — passed through           │
│                               ↓                                              │
│  ┌──────────────────────────────────────────────────────────┐                │
│  │  staged · revenue              [Intermediate]            │                │
│  └────────────────────────────┬─────────────────────────────┘                │
│                               │ [Aggregation]  etl/final.sql · line 47       │
│                               │  ┌─────────────────────────────────────┐     │
│                               │  │  SUM(revenue) OVER (PARTITION BY …) │ ↕   │
│                               │  └─────────────────────────────────────┘     │
│                               ↓    max ~6 lines, overflow scrolls in-block   │
│  ┌──────────────────────────────────────────────────────────┐                │
│  │  report · total_revenue       [Target / Selected]        │                │
│  └──────────────────────────────────────────────────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Success Criteria

- A user can open the Transform tab and trace the full transformation chain for any column — sources, transforms, expressions, and files — without switching tabs or clicking through hops one-by-one.
- The summary sentence gives at-a-glance orientation (what kind of transformation, which source, which file) before reading the chain.
- The Code and Path tabs are gone; users reach for Transform when they want to understand a column's origin.

## Scope Boundaries

- **Upstream-only.** Downstream direction (what the selected column feeds) belongs to the Impact tab.
- **No backend changes.** Expressions shown are the snippets currently stored per edge. Full surrounding file context is a follow-up task (Approach C).
- **No AI-generated summaries.** Summaries use generic transform type labels derived from chain data.
- **Graph and Tree tabs are unchanged.**
- **Impact tab is unchanged.**

## Key Decisions

- **Merge Code + Path → single Transform tab**: Two tabs addressed the same need and confused users. Merging delivers a richer unified experience without redundancy.
- **Generic labels in summary (not function names)**: `transform_type` stores category labels, not function names; parsing expressions for "SUM" would be fragile. "Aggregated from …" is accurate and always derivable.
- **Dominant type = last non-passthrough step**: Most relevant to the final shape of the column — the transformation closest to the output.
- **Passthrough = label only, no code block**: Nothing meaningful to show in a code block for a passthrough.
- **Expression cap at ~6 lines**: Keeps the chain scannable when individual hops have long expressions.
- **Null-data hops = "Structural hop" label**: Wildcard/temp-view synthesized hops always render a card — never empty or broken.
- **PathInspector as the base**: Extend `frontend/components/path-inspector.tsx` rather than building from scratch — it already implements multi-hop chain, path pills, and truncation notice.

## Dependencies / Assumptions

- `PathInspector` (`frontend/components/path-inspector.tsx`) already implements the multi-hop chain (R3), path selector pills (R5), and truncation notice (R6) — it is the base component to extend, not replace.
- `/lineage/paths` returns chains up to 10 hops deep (`max_depth=10` in `backend/api/routes.py`); paths reaching the depth limit are silently terminated without setting `truncated=True` — distinct from path-count truncation, covered by R6's depth indicator.
- The `expression` field is a snippet, not full surrounding query context. This is a known, accepted limitation within this scope.
- `react-syntax-highlighter` is already installed (used by current Code tab and PathInspector).

## Outstanding Questions

### Deferred to Planning

- [Affects R11][Technical] Whether R11's "no upstream" detection should use the `/lineage` response (`data.upstream.length === 0`) or the `/lineage/paths` response (`paths.length === 0`) as the authoritative signal — the two graphs (`lineage_graph` and `raw_graph`) can disagree. Planning should verify which better matches user intent for "source column."
- [Affects R5][Technical] If flex-wrap produces a cluttered pill row (many paths), confirm whether a "+N more" overflow button is preferable to wrapping indefinitely.

## Next Steps

-> `/ce:plan` for structured implementation planning
