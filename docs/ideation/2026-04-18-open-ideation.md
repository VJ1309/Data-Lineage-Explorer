---
date: 2026-04-18
topic: open-ideation
focus: open-ended
mode: repo-grounded
---

# Ideation: DataLineage Explorer — Open-Ended

## Grounding Context

**Project shape:** Full-stack data lineage tool — FastAPI backend (Railway, in-memory state) + Next.js/TypeScript frontend (Vercel). SQL parser built on SQLGlot (`dialect="databricks"`), PySpark parser via Python `ast`, Jupyter notebook parser. Lineage stored as a NetworkX DiGraph; all state lost on server restart.

**Notable patterns:**
- Column IDs are always 4-part `catalog.schema.table.column`; temp view resolution flattens through CTE/view chains
- Confidence system exists at the file level (high/medium/low) with per-source warning counts
- Frontend uses React Query for all data fetching; lineage visualized with `@xyflow/react`

**Pain points identified:**
- Zip upload is the primary onboarding friction point
- Confidence is visible per-file but not propagated downstream across the graph
- No history or diff between source refreshes; every refresh is a full reset
- Parser gaps (dynamic SQL, macros) leave edges unresolved with no escape hatch
- Lineage lives only in-browser; can't be exported, checked, or shared

**Leverage points:**
- NetworkX graph supports BFS traversal trivially — transitive analysis is cheap
- `/search` endpoint already supports column-level search but UI doesn't expose it prominently
- In-memory design means export/diff are pure computation with no storage concerns

---

## Ranked Ideas

### 1. Transitive Confidence Blast Radius
**Description:** When any upstream source is low-confidence (parse errors, ambiguous tables), propagate that uncertainty downstream and surface all affected columns. "This column is uncertain because its source `raw.events.user_id` has parse errors." Confidence becomes contagious, not local.
**Rationale:** The confidence system currently shows per-file badges but doesn't connect them to downstream impact. A single parse error in a base table can corrupt a dozen gold-layer columns silently. Closing this gap is the highest-value change relative to code cost — it's a BFS traversal on an already-built graph.
**Downsides:** UI needs a "columns affected by low-confidence sources" view that could get noisy for large graphs. Requires a decision on confidence aggregation strategy (minimum, average, product).
**Confidence:** 88%
**Complexity:** Low
**Status:** Unexplored

### 2. GitHub / Remote Source Connection
**Description:** Instead of uploading a zip, let users paste a GitHub repo URL (public or with token for private). Backend clones/fetches, classifies files, and parses. No local zip required. Also enables CI-driven refreshes.
**Rationale:** Zip upload is the highest-friction onboarding step — users must find the repo, zip it locally, upload it. Removing all three steps dramatically improves first-use experience. Pairs naturally with auto-refresh on webhook trigger.
**Downsides:** Requires git clone on the backend; private repos need token auth; Railway ephemeral disk limits retention. Adds a network dependency and a credential management concern.
**Confidence:** 85%
**Complexity:** Medium
**Status:** Unexplored

### 3. Lineage Diff Primitive
**Description:** After every source refresh, compute which edges were added, removed, or changed confidence tier vs. the previous snapshot. Surface a diff summary ("12 edges added, 3 removed, 5 confidence degraded") and store the snapshot for comparison.
**Rationale:** The current model throws away history on every refresh. A diff primitive unlocks PR lineage reviews, regression detection, and CI gates — it's the foundation for every change-tracking idea. Implementation requires persisting only the previous graph state.
**Downsides:** Adds state to a stateless server; needs a decision on snapshot storage (in-memory between refreshes, or serialized to disk). Snapshot may be large for complex graphs.
**Confidence:** 83%
**Complexity:** Medium
**Status:** Unexplored

### 4. Column-First Navigation
**Description:** A global "find a column" search bar that returns all tables producing a column named `X`, shows confidence per result, and jumps directly to the lineage view. "Where is `customer_id` defined?" becomes a one-step query.
**Rationale:** Analysts know the column they care about, not the table. The current table-first UX forces a mental mapping step that is often wrong. The `/search` endpoint already supports this — the change is primarily frontend UX.
**Downsides:** Risk of overwhelming results when column names are generic (`id`, `name`). Needs deduplication/grouping of results across sources.
**Confidence:** 82%
**Complexity:** Low
**Status:** Unexplored

### 5. Serializable Lineage Export
**Description:** Add a `/api/lineage/export` endpoint returning the full graph as JSON (nodes, edges, confidence, source file). Add a download button in the UI. Makes lineage a portable artifact: pipe into Slack, attach to PRs, ingest in Atlan/dbt docs.
**Rationale:** The lineage graph is currently trapped in the browser session. Export turns it into a shareable, diffable, checkable artifact that survives server restarts and enables every downstream integration. Trivial to implement; high future leverage.
**Downsides:** Once people start piping this into CI, breaking the export schema has a cost. Needs schema versioning from the start.
**Confidence:** 80%
**Complexity:** Low
**Status:** Unexplored

### 6. Editable Lineage (Manual Edge Override)
**Description:** When the parser can't resolve a column's origin (low confidence, ambiguous table), let the user manually specify the source. Overrides stored as a sidecar file (`lineage_overrides.json`) that survives refreshes and can be committed to the repo.
**Rationale:** Static parsing will always have gaps — dynamic SQL, macro-generated tables, external API writes. Editable edges make the tool practical for real-world messy pipelines. The sidecar pattern avoids polluting parse output and can be reviewed like code.
**Downsides:** Requires a persistence layer. UX for "click an edge to override" is moderately complex. Overrides can go stale if the schema changes without notice.
**Confidence:** 78%
**Complexity:** Medium
**Status:** Unexplored

### 7. CI / Pre-commit Integration
**Description:** A lightweight CLI command (`lineage-check`) or GitHub Action that fetches current lineage, compares to a committed baseline (`lineage-baseline.json`), and fails if confidence degrades or new unresolved columns appear. Pairs with export and diff primitives.
**Rationale:** Makes lineage a first-class CI artifact — data engineers get the same ratchet as test coverage. Directly addresses "confidence is visible but not enforced." Dependent on ideas 3 and 5 being shipped first.
**Downsides:** Requires export + diff primitives first. Needs a story for private deployment. Baseline workflow adds process overhead — teams must opt into maintaining it.
**Confidence:** 75%
**Complexity:** High
**Status:** Unexplored

---

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | Silent blind-spot inventory | Vague; already surfaced via table roles and warnings |
| 2 | Stale state notification | Users already know state resets on redeploy |
| 3 | Expression diff view on edges | Expression data incomplete in parser output; premature |
| 4 | Confidence heatmap | Duplicates existing confidence badges; incremental polish |
| 5 | PySpark variable tracker | Parser-internal improvement; not a product idea |
| 6 | Graph layout stability | Engineering housekeeping |
| 7 | Ambiguous table surfacing | Partially covered by existing warnings panel |
| 8/12 | Browser cache / localStorage state | Same idea twice; engineering hygiene, not a feature |
| 10 | Auto-refresh | Polling conflicts with in-memory architecture |
| 11 | Warnings on graph | Incremental; dominated by stronger ideas |
| 15 | Dialect auto-detect | Already handled via dialect parameter |
| 16 | LLM fallback parser | Too expensive; adds non-determinism and trust risk |
| 17 | Lineage as contract | Too abstract; needs a dedicated brainstorm to crystallize |
| 18 | Data product unit | Requires significant ontology design; out of scope |
| 19 | Lineage PR | Covered by diff primitive (idea 3) |
| 20 | Confidence distribution | Dashboard widget, not a product direction |
| 21/43 | Runtime capture / SDK | Completely different architecture; major scope expansion |
| 23 | Team annotations | Requires persistent storage; breaks in-memory model |
| 26 | OpenLineage emission | Niche at this stage; high integration cost |
| 27 | Fixture test format | Engineering internal; belongs in dev docs |
| 28 | Parser plugin interface | Premature abstraction |
| 30 | Confidence as queryable attribute | Covered by export survivor |
| 31 | Versioned source registry | Requires persistent storage redesign |
| 32 | Column ID index | Already implicit in NetworkX graph |
| 33 | Lineage blame | Requires git history correlation; complex, narrow |
| 34 | Coverage map | Same rejection as blind-spot inventory |
| 35 | Materiality thresholds | Too much domain-specific configuration cost |
| 37 | Probe points | Monitoring concern; out of scope |
| 38 | Citation quality markers | Already served by confidence levels |
| 39 | Type inference linting | Requires parser type tracking; major scope expansion |
| 40 | Chain of custody attestation | Enterprise feature; premature |
| 42 | Community registry | Requires network effects; premature |
| 46 | Multi-tenant cross-team maps | Too far from current single-user architecture |
| 47 | Databricks live sync | High value but complex auth; belongs in a dedicated brainstorm |
| 48 | Shareable public deployment | SaaS hosting concern, not a lineage feature |
