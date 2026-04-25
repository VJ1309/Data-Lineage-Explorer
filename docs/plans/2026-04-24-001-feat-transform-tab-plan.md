---
title: "feat: Unified Transform Tab for Column Lineage"
type: feat
status: active
date: 2026-04-24
origin: docs/brainstorms/2026-04-24-transform-tab-requirements.md
---

# feat: Unified Transform Tab for Column Lineage

## Overview

Replace the Lineage section's Code tab and Path tab with a single **Transform** tab that shows the full upstream transformation chain from raw source columns to the selected target in one view. The new tab shows a summary banner, path selector pills, and a vertical chain of hop cards — each card showing the source column, transform badge, syntax-highlighted expression (capped at ~6 lines), and source file reference.

The existing `PathInspector` component is the foundation: it already renders multi-hop chains, path selector pills, and a truncation notice. The work is largely additive — summary banner, redesigned hop cards, passthrough/null-data treatments, and loading/error states.

## Problem Frame

Users can't understand a column's transformation story from the current UI. The Code tab shows one upstream hop at a time (no chain). The Path tab shows the chain but hides expressions behind a toggle, uses minimal card design, and has no summary. Users don't know which tab to use. Merging them into a single, richer Transform tab removes the confusion and delivers a clear picture in one place. (See origin document for full problem frame.)

## Requirements Trace

- R1. Remove Code and Path tabs from the lineage tab bar; replace with a single Transform tab
- R2. Tab bar becomes: Graph · Tree · Transform
- R3. Transform tab is upstream-only (raw sources → selected target); downstream covered by Impact tab
- R4. Each hop shows the source column as a card followed by a transform connector row (badge, expression capped ~6 lines, file/line ref). The final target column is rendered as a card at the chain bottom; intermediate targets are implicit as the next hop's source card. Null-data hops show "Structural hop — no expression recorded" in place of expression/file rows.
- R5. Multiple paths → path selector pills at top; colliding labels get numeric suffix ("via events (1)", "via events (2)"); pills flex-wrap
- R6. Truncation indicator when the path-count limit is reached. A global note ("chain may be deeper than shown") accompanies it to address the 10-hop depth-limit case within current frontend capability — per-node depth indicators require a backend flag and are deferred (see Deferred).
- R7. Summary sentence at top: generic type label, source origin, hop count, source file — derived deterministically from chain data, no AI
- R8. Dominant transform type = last non-passthrough step; passthrough-only chains → "Passed through unchanged from …"
- R9. Expressions expanded by default; capped at ~6 lines (~120px) with vertical scroll inside block
- R10. Passthrough hops show "No transformation — passed through unchanged" in place of a code block
- R11. Source columns (zero paths) show "This is a source column — no upstream transformations"
- R12. Loading indicator and inline error message during/after `/lineage/paths` fetch, consistent with the app-wide text-based pattern (`<p className="text-sm text-muted-foreground">Loading…</p>`)
- R13. UI matches existing card/pill/badge spacing and typography; expression blocks use vscDarkPlus; summary banner secondary info (hop count, file) uses `text-muted-foreground`; use only Tailwind classes already present in the codebase.

## Scope Boundaries

- Upstream-only; no backend changes
- No AI-generated summaries; summaries use generic type labels from `transform_type` field only
- Graph and Tree tabs unchanged; Impact tab unchanged
- `code-inspector.tsx` and `path-inspector.tsx` are deleted — not preserved or refactored

### Deferred to Separate Tasks

- Full file context (N lines around each expression) — Approach C, separate backend + frontend task
- Per-node depth-truncation indicator — requires backend to expose a `depth_truncated` flag on terminal nodes in `/lineage/paths`; frontend cannot distinguish depth-stopped nodes from true source columns without it

## Context & Research

### Relevant Code and Patterns

- **Base component**: `frontend/components/path-inspector.tsx` — already implements multi-hop `PathChain`, `StepArrow`, path selector pills, and truncation notice. Key behaviors to keep: `colLabel()` helper, language detection for SyntaxHighlighter, `selectedPath` state.
- **Page integration**: `frontend/app/lineage/page.tsx` — uses `usePaths(table, column)` at the top level (not deferred to tab click). Loading is `pathsLoading` guard; error from `usePaths` is currently not surfaced.
- **Transform badge**: `frontend/components/transform-badge.tsx` — `TransformBadge({ type })` with full COLOURS/LABELS/DESCRIPTIONS maps for all 7 types. No changes needed.
- **Hooks**: `frontend/lib/hooks.ts` — `usePaths` already returns `{ data, isLoading, error }`. No hook changes needed.
- **Loading/error pattern**: `<p className="text-sm text-muted-foreground">Loading…</p>` / `<p className="text-sm text-destructive">Error: …</p>` — established app-wide pattern.
- **SyntaxHighlighter usage**: `Prism` variant, `vscDarkPlus` style, `fontSize: 11`, `padding: "8px 12px"`. Cap at 6 lines via a wrapper `div` with `max-h-28 overflow-y-auto`.
- **Pill active/inactive pattern**: `text-xs px-2 py-1 rounded border transition-colors ${selected ? "bg-accent text-accent-foreground border-accent" : "text-muted-foreground border-transparent hover:text-foreground hover:border-border"}` — used in both `path-inspector.tsx` and `lineage-graph.tsx`.
- **Column ID splitting**: `id.lastIndexOf(".")` — never `split(".", 1)`. The `colLabel()` helper in `path-inspector.tsx` uses `parts.at(-1)` for col and `parts.at(-2)` for table, which is equivalent and reusable.
- **Expression field**: For join-key columns, `expression` may be a `\n`-separated multi-line string (per `docs/solutions/`). `whitespace-pre-wrap` or the SyntaxHighlighter will handle this naturally.

### Institutional Learnings

- `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md`: Join key columns can have `\n`-joined expressions. SyntaxHighlighter renders these correctly; no special handling needed.
- No frontend architecture solutions documented — this is the first significant frontend component work.

### External References

None needed. Local patterns are sufficient for all decisions.

## Key Technical Decisions

- **New file `transform-inspector.tsx`, delete old files**: The new component differs substantially from PathInspector (summary banner, redesigned cards, new state logic). Creating a new named file makes the change legible and avoids confusion. `code-inspector.tsx` and `path-inspector.tsx` are deleted at end.
- **Loading/error handled inside `TransformInspector` via props from the page**: `TransformInspector` receives `isLoading`, `isError`, and `errorMessage` as props and renders the appropriate state internally. The page passes these from `usePaths` with no conditional guard (no `pathsData &&` wrapper). Empty state (R11, `paths.length === 0`) is also handled inside the component. This differs slightly from the existing PathInspector pattern (which had a page-level `pathsData &&` guard) but consolidates all tab content rendering in one place.
- **Summary derived from `paths[selectedPath].steps`**: Source origin = `steps[0].source_col`; dominant type = last non-passthrough step's `transform_type`; hop count = `steps.length`; file = last non-passthrough step's `source_file` (filename only, no path prefix). **Edge cases**: (1) "1 hop" singular, "N hops" plural; (2) when `source_file` is null, omit the ` · {file}` segment entirely (no trailing separator); (3) for all-passthrough chains, `file` falls back to the last step's `source_file` (passthrough step is still the dominant step); (4) the banner recomputes whenever `selectedPath` changes — `deriveSummary` must be called inside the render body, not memoized across paths.
- **R11 signal = `paths.length === 0`**: Uses the same data source as the chain view (raw_graph via `/lineage/paths`). Avoids split-brain with `data.upstream.length`.
- **Null/wildcard hop detection**: `!step.expression || step.expression === "*"` (falsy covers null, undefined, and empty string). Show "Structural hop — no expression recorded" label; omit file/line row.
- **Depth truncation detection**: The backend sets `truncated = true` only for path-count overflow, not depth. A depth-truncated terminal node has no predecessors — it appears as a chain endpoint that isn't a known source table. The frontend cannot distinguish this from a true source without backend changes, so the R6 depth indicator is shown as a general note ("chain may be deeper than shown") in the truncation banner alongside the existing path-count note, not per-node.
- **Pill label uniqueness**: Compute all labels first; for any label that appears more than once, append ` (1)`, ` (2)` etc. in order of appearance.

## Open Questions

### Resolved During Planning

- **R11 signal (lineage_graph vs raw_graph)**: Resolved → use `paths.length === 0` from `/lineage/paths` response. Avoids data-source divergence.
- **R5 overflow (flex-wrap vs +N more)**: Resolved → flex-wrap (matches existing PathInspector behavior and is simpler).
- **Depth truncation per-node vs global note**: Resolved → global note in truncation banner. Frontend cannot detect depth-truncated nodes without backend changes (out of scope).

### Deferred to Implementation

- Exact Tailwind classes for card spacing and inter-card connector lines (vertical connector between cards) — follow PathInspector's `w-px h-3 bg-border` pattern; adjust as needed for visual quality.
- Whether to keep `StepArrow` as the local sub-component name or rename to something more descriptive — implementer's choice.

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```
TransformInspector
  props: { paths, truncated, isLoading, isError }

  ┌─ if isLoading → render loading text
  ├─ if isError   → render error text
  ├─ if paths.length === 0 → render source-column empty state (R11)
  └─ else:
       selectedPath: number (state, default 0)

       deriveSummary(steps[])
         → dominant = last step where transform_type !== "passthrough" (or first step if all passthrough)
         → source origin = colLabel(steps[0].source_col)
         → hop count = steps.length
         → file = dominant step's source_file?.split(/[\\/]/).at(-1)
         → returns { label, source, hops, file }

       computePillLabels(paths[])
         → for each path: "via {colLabel(steps[0].source_col).tbl}"
         → deduplicate: append " (N)" suffix for collisions

       render:
         <summary banner>
           {label} from `{source.tbl}.{source.col}` across {n} hops · {file}

         <path selector> (only when paths.length > 1)
           pills using computePillLabels()

         {truncated && <truncation note>}

         <chain>
           for each step in paths[selectedPath].steps:
             <HopCard step={step} isLast={...} />

HopCard
  → source column card (always shown)
  → connector with:
       TransformBadge
       file · cell N · line M (omit null fields + their separators)
       if passthrough: "No transformation — passed through unchanged" (no code block)
       elif null/wildcard: "Structural hop — no expression recorded" (no code block)
       else: <div relative max-h-28 overflow-y-auto>
               <SyntaxHighlighter> {expression} </SyntaxHighlighter>
               <CopyButton (hover-visible, top-right)>
             </div>
  → target column card only on the LAST hop (styled as selected/target)
     intermediate columns are not rendered as target cards —
     they appear as the source card of the next HopCard
```

## Implementation Units

---

- [ ] **Unit 1: Create `transform-inspector.tsx`**

**Goal:** Build the new `TransformInspector` component that satisfies R3–R13.

**Requirements:** R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13

**Dependencies:** None (standalone component)

**Files:**
- Create: `frontend/components/transform-inspector.tsx`

**Approach:**
- Props interface: `{ paths: LineagePath[]; truncated: boolean; isLoading: boolean; isError: boolean; errorMessage?: string }`. Loading and error are passed from the page (consistent with app pattern).
- Local state: `selectedPath: number` (default 0)
- `deriveSummary(steps: PathStep[])`: find last non-passthrough step; if none, treat all as passthrough. Return `{ verb, source, hops, file }`. Verb map: aggregation→"Aggregated", expression→"Derived by expression", window→"Computed (window)", cast→"Cast", join_key→"Used as join key", filter→"Filtered", passthrough→"Passed through unchanged".
- `computePillLabels(paths: LineagePath[])`: map each path to `"via {colLabel(steps[0].source_col).tbl}"`. If `tbl` is empty (rare), fall back to `"via (unknown)"` — do not fall back to column name (contradicts R5). Detect label collisions; append ` (1)`, ` (2)` in order of appearance.
- `HopCard` sub-component (local function): renders source card, connector with badge + expression treatment, target card (last step only). Uses `TransformBadge` from `./transform-badge`.
- Expression display: null/wildcard check first → structural hop label; passthrough check → passthrough label; else → SyntaxHighlighter in `max-h-28 overflow-y-auto` wrapper.
- Source/target card colors: source card uses `border-border bg-card`; target card uses `border-primary/40 bg-primary/5 text-primary` (matches PathInspector's terminal node style).
- Connector: vertical `w-px bg-border` lines above and below `↓` arrow; transform badge and file reference inline with arrow.
- **File/line display in connector**: render `{source_file} · cell {source_cell} · line {source_line}`, omitting any component and its separator when that field is null. Example: `"transform.sql · cell 3 · line 42"`. When all three are null (null-data hop), omit the entire row (already handled by the structural hop branch).
- **Expression copy button**: add a hover-visible copy-to-clipboard icon (Lucide `Copy`, `h-3 w-3`) absolutely positioned top-right inside each expression block wrapper. On click, call `navigator.clipboard.writeText(expression)`. This gives users the ability to copy expressions they see permanently on screen.
- **Pill ARIA**: wrap pills in a `role="radiogroup"` container with `aria-label="Transformation paths"`. Each pill is `role="radio"` with `aria-checked={selected}`. Implement roving tabindex: the active pill has `tabIndex={0}`, inactive pills have `tabIndex={-1}`; arrow keys (←/→) move focus and update selection.
- **Expression chain density**: add a "Collapse all / Expand all" toggle button (text button, top-right of the chain section) that collapses all expression blocks to 0 height simultaneously. Default state is expanded (R9). Individual blocks retain `max-h-28 overflow-y-auto` when expanded.
- **Responsive scope**: this component targets desktop/tablet (minimum 768px viewport). Below 768px, cards go full-width, the connector column collapses inline, and pill rows scroll horizontally rather than wrapping. Use only existing breakpoint classes (`sm:`, `md:`) — do not introduce new breakpoints.
- Truncation note: when `truncated`, show a muted note: "Showing {paths.length} path{s} — more may exist; chain may also be deeper than shown."
- Empty state (R11): when `paths.length === 0`, render: icon + "This is a source column — no upstream transformations" (simple muted paragraph, not the full centered card layout).

**Patterns to follow:**
- `frontend/components/path-inspector.tsx` — `colLabel()`, `StepArrow`, pill active/inactive classes, language detection
- `frontend/components/lineage-graph.tsx` — pill active/inactive pattern: `bg-accent text-accent-foreground border-accent` / `text-muted-foreground border-transparent hover:text-foreground hover:border-border`
- SyntaxHighlighter: `Prism` import, `vscDarkPlus`, `customStyle={{ margin: 0, borderRadius: 6, fontSize: 11, padding: "8px 12px" }}`

**Test scenarios:**
- Test expectation: none — no frontend test infrastructure exists. Verification is TypeScript compilation and visual review (see Verification below).

**Verification:**
- `npm run build` passes with no TypeScript errors
- Rendering a column with a multi-hop aggregation chain shows: summary banner with "Aggregated from …", all hops visible without clicking, expressions capped and scrollable, passthrough hops show label only
- Rendering a source column (no paths) shows the empty state message
- Rendering a column with multiple paths shows the path selector pills; selecting a different pill updates the chain
- Rendering a column whose path includes a wildcard/temp-view hop shows "Structural hop — no expression recorded" for that card
- Truncated response shows the truncation note
- Long expression (>6 lines) scrolls within the block without pushing chain cards off-screen

---

- [ ] **Unit 2: Wire Transform tab into the lineage page**

**Goal:** Replace Code and Path tabs with a single Transform tab using the new component. Surface the paths error state that was previously swallowed.

**Requirements:** R1, R2, R12

**Dependencies:** Unit 1 (TransformInspector must exist)

**Files:**
- Modify: `frontend/app/lineage/page.tsx`

**Approach:**
- Remove: `import { CodeInspector }` and `import { PathInspector }` lines
- Add: `import { TransformInspector } from "@/components/transform-inspector"`
- From `usePaths`, destructure `error` as `pathsError` alongside `data: pathsData` and `isLoading: pathsLoading`
- Remove `<TabsTrigger value="code">` and its `<TabsContent value="code">` block
- Remove `<TabsTrigger value="path">` and its `<TabsContent value="path">` block
- Add: `<TabsTrigger value="transform">⇢ Transform</TabsTrigger>` (position: after Tree, before any others)
- Add: `<TabsContent value="transform" className="pt-4">` containing `<TransformInspector key={`${table}.${column}`} paths={pathsData?.paths ?? []} truncated={pathsData?.truncated ?? false} isLoading={pathsLoading} isError={!!pathsError} errorMessage={(pathsError as Error)?.message} />` — the `key` prop forces a full remount on column change, resetting `selectedPath` to 0 and preventing out-of-bounds access
- Update `<Tabs defaultValue="graph">` — keep `"graph"` as default (Graph tab stays primary)
- The `pathsData` / `pathsLoading` variables are already declared at the top level — just add `pathsError` to the destructure

**Patterns to follow:**
- Existing tab wiring in `frontend/app/lineage/page.tsx` (Graph, Tree tabs as reference)
- Error pattern: `(pathsError as Error)?.message` matching how `error` is cast elsewhere in the file

**Test scenarios:**
- Test expectation: none — no frontend test infrastructure. See Verification.

**Verification:**
- `npm run build` passes
- Navigating to Lineage for any column shows Graph, Tree, Transform tabs (no Code or Path tabs)
- Transform tab renders correctly for a column with lineage data

---

- [ ] **Unit 3: Remove superseded components**

**Goal:** Delete `code-inspector.tsx` and `path-inspector.tsx` to prevent dead code.

**Requirements:** R1 (tab removal implies component removal)

**Dependencies:** Unit 2 (page must no longer import the old components)

**Files:**
- Delete: `frontend/components/code-inspector.tsx`
- Delete: `frontend/components/path-inspector.tsx`

**Approach:**
- Confirm neither file is imported anywhere after Unit 2 is complete (grep for `code-inspector` and `path-inspector` in the frontend directory)
- Delete both files
- Re-run `npm run build` to confirm no broken imports

**Test scenarios:**
- Test expectation: none — no frontend test infrastructure.

**Verification:**
- `npm run build` passes after deletion
- No import of `code-inspector` or `path-inspector` exists in the codebase

---

## System-Wide Impact

- **Interaction graph:** Only `frontend/app/lineage/page.tsx` imports the deleted components. No other page or component references them. The new `transform-inspector.tsx` is imported only from the lineage page.
- **Error propagation:** `pathsError` from `usePaths` was previously swallowed. After this change, it surfaces as an inline error message in the Transform tab — same pattern used by the main lineage query.
- **State lifecycle risks:** `selectedPath` does NOT reset automatically on re-render — React `useState` retains its value between renders. Navigating from a 5-path column (selectedPath=4) to a 1-path column leaves `paths[4]` undefined, causing a TypeError crash. Fix: add `key={\`${table}.${column}\`}` on `<TransformInspector>` in Unit 2 so the component fully remounts when the column changes, resetting all local state.
- **API surface parity:** No new API calls. The `/lineage/paths` endpoint is already being called on every lineage page load (not lazy). No change to backend.
- **Integration coverage:** The Transform tab's data flows entirely through the existing `usePaths` hook → `/lineage/paths` → `TransformInspector`. No new data flow introduced.
- **Unchanged invariants:** Graph tab, Tree tab, Impact tab behavior is unchanged. `/lineage` and `/lineage/paths` API contracts unchanged. Column ID format (`catalog.schema.table.column`) and splitting convention (`lastIndexOf(".")`) unchanged.

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| `@base-ui/react` Tabs API may differ from Radix — `data-active` not `aria-selected` for active state styling | Follow the existing `TabsTrigger`/`TabsContent` usage in `lineage/page.tsx` exactly; do not assume standard Radix behavior |
| Tailwind CSS 4.x `@theme` config — some utility classes may behave differently | Use only classes already in use in the codebase (no new utility experiments); verify visually |
| `react-syntax-highlighter` types may need explicit imports for Prism variant | Follow exact import pattern from current `path-inspector.tsx` and `code-inspector.tsx` |
| Long passthrough chains may make the Transform tab very tall (many label-only hops) | Passthrough hops are visually compact (no code block); mitigated by the label-only design. Future: collapsible passthrough runs |
| AGENTS.md warns Next.js 16 differs from training data | Check `node_modules/next/dist/docs/` for anything unusual before touching page or routing patterns |

## Sources & References

- **Origin document:** [docs/brainstorms/2026-04-24-transform-tab-requirements.md](docs/brainstorms/2026-04-24-transform-tab-requirements.md)
- Base component: `frontend/components/path-inspector.tsx`
- Page to modify: `frontend/app/lineage/page.tsx`
- Deleted components: `frontend/components/code-inspector.tsx`, `frontend/components/path-inspector.tsx`
- Institutional learning: `docs/solutions/logic-errors/joinkey-multi-predecessor-expression-drop-2026-04-19.md`
