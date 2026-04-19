---
date: 2026-04-19
topic: open-ideation
focus: open-ended, grounded in zip upload test + warnings UX observation
mode: repo-grounded
---

# Ideation: DataLineage Explorer — Open-Ended (2026-04-19)

## Grounding Context

**Project shape:** Full-stack data lineage tool — FastAPI backend (Railway, in-memory state) + Next.js/TypeScript frontend (Vercel). SQL parser built on SQLGlot (`dialect="databricks"`), PySpark parser via Python `ast`, Jupyter notebook parser. Lineage stored as a NetworkX DiGraph; all state lost on server restart.

**Today's test observation:** User uploaded a zip file to the live app. Parse warnings surfaced in the WarningsPanel on the Sources page — a flat list with severity icons (✕/⚠/ⓘ) but no drill-down to affected tables/columns. No indication of parse completeness (what % of lineage was successfully resolved).

**Notable patterns:**
- Column IDs are always 4-part `catalog.schema.table.column`; temp view resolution flattens through CTE/view chains
- Confidence system exists at the file level (high/medium/low) with per-source warning counts
- Frontend uses React Query for all data fetching; lineage visualized with `@xyflow/react`
- `_normalize_edges()` performs suffix-match fallback for short table names — implicit resolution quality signal not yet surfaced
- `_edge_to_dict()` in `routes.py` is the sole serialization point for edges — every new edge field must be added there

**Pain points identified:**
- WarningsPanel is a dead-end: see warnings, can't act on them (no link to affected column/edge, no error type grouping)
- No parse completeness signal — graph looks identical whether 40% or 95% of references resolved
- Low-confidence base tables silently corrupt downstream gold columns with no visual indication
- Zip upload is high-friction onboarding — every Railway redeploy forces a full re-upload
- `/search` endpoint exists but is a corner widget, not a primary navigation surface
- Edges carry no provenance (which parser produced them, from which file/line)

**Leverage points:**
- `engine.downstream()` BFS already exists — transitive analysis is cheap
- `_normalize_edges()` already distinguishes exact vs. suffix-match resolution — implicit quality signal
- `/search` endpoint built, underused
- @xyflow/react supports custom node renderers — confidence visualization is straightforward
- In-memory design means export is pure computation — no storage decisions required

**External signals:**
- Market gap: no upload-tier lineage tool shows parse completeness % to users
- DataHub: column lineage silently degrades when schema stale — exact same failure mode
- Meta: two-tier confidence (exact vs. approximate) separated in the data model
- Datafold: "blast radius" reframes lineage as predictive impact, not documentation
- Codecov: quality signals annotated in-context (where the artifact is), not in a separate panel
- ESLint/Pyright: severity tiers + grouping by error type makes warnings actionable vs. ignored

---

## Ranked Ideas

### 1. Actionable Warning System
**Description:** Attribute every parse warning to the specific edge(s) and column(s) it affects at parse time. Group warnings by error type (unresolved table, ambiguous join, dialect failure, etc.). Render a small badge directly on affected graph nodes via @xyflow custom nodes. Clicking a warning in the panel jumps to the node; hovering a badge shows the warning inline. Replaces the flat list with a tiered, navigable, in-context system.
**Rationale:** Today's test revealed the core failure: warnings exist but are a dead-end — the user sees a problem but has no path to investigate or fix it. The data is already there (file, line, severity, error text); edge-level attribution is the missing step. Codecov's in-context annotation pattern validates the approach over a separate panel.
**Downsides:** Requires a backend change to attribute warnings to specific edge IDs at parse time. Warning panel grows more complex to render. Structured warning taxonomy (error type enum) must be defined and maintained.
**Confidence:** 91%
**Complexity:** Medium
**Status:** Unexplored

### 2. Parse Completeness Score
**Description:** After every parse, compute: total column references detected, fully resolved (4-part), heuristically matched (suffix fallback), and unresolved. Show a "Lineage Coverage: 87%" stat on the Sources page, with a per-table breakdown accessible on click. Expose as `/sources/{id}/report` for machine-readable access (future CI use).
**Rationale:** No upload-tier lineage tool currently shows users what percentage of their lineage was actually resolved — the graph always looks complete. DataHub's silent confidence degradation is the canonical failure of missing this signal. Pure computation from existing data in `_normalize_edges()` — no new parsing work required.
**Downsides:** "Coverage %" framing may be misinterpreted (87% of what, exactly?). Cross-repo references genuinely not in the zip will lower the score for non-bug reasons — needs clear labeling of what is and isn't resolvable from the upload.
**Confidence:** 88%
**Complexity:** Low
**Status:** Unexplored

### 3. Transitive Confidence + Visual Graph Layer
**Description:** Propagate low-confidence parse signals downstream via BFS (when a table has unresolved references, its downstream consumers are marked "caution"). Render in the graph as visual decay: fully-certain edges solid, heuristically-matched edges dashed, unresolved-chain edges dotted/faded. Add a "hide uncertain edges" filter toggle.
**Rationale:** Was the top-ranked idea from yesterday's session (88%) and independently re-validated by 3 sub-agents this session. A single parse error in a base table silently corrupts every downstream gold column — users can't tell. The BFS is trivial (`engine.downstream()` already exists); visual layer is straightforward in @xyflow custom nodes. Combines blast-radius thinking (SRE) with fog-of-war visual encoding (strategy games).
**Downsides:** Visual encoding (dashed/dotted edges) can clutter large graphs. Requires a confidence aggregation decision (minimum-hop vs. weighted decay). "Hide uncertain" toggle could hide lineage users actually need.
**Confidence:** 88%
**Complexity:** Low
**Status:** Unexplored

### 4. Serializable Lineage Export
**Description:** A `/api/lineage/export` endpoint returning the full resolved graph as JSON (nodes, edges, confidence, source file, line numbers). A "Download Lineage" button in the UI. Downloaded snapshots can be re-uploaded to restore state after a Railway redeploy. Unlocks future diff, CI integration, and Slack/PR annotations as natural follow-ons.
**Rationale:** The lineage graph is currently trapped in the browser session. Export is pure computation — no storage decisions required, no architectural constraint hit. Highest leverage-per-line-of-code ratio of any idea: once this exists, diff, sharing, CI gates, and state restore all become straightforward follow-ons.
**Downsides:** Schema versioning is a future maintenance cost — once external tools consume this, breaking the schema has a cost. Need to decide whether export includes `__filter__` / `__joinkey__` pseudo-columns or strips them.
**Confidence:** 85%
**Complexity:** Low
**Status:** Unexplored

### 5. GitHub Source Integration
**Description:** Accept a GitHub repo URL (public or with PAT for private repos) instead of a zip. The backend clones or fetches the target directory, classifies files, and runs the same parse pipeline. Optionally expose a webhook endpoint so GitHub can POST on push → auto re-parse. Eliminates the find-zip-upload cycle entirely and keeps lineage current with code changes.
**Rationale:** Zip upload is the highest-friction step in every use, and it compounds on every Railway redeploy (state lost = must re-upload). GitHub API is standard; git clone on backend is well-understood. External validation: DataHub ships this as a first-class feature. Webhook extension makes lineage a live artifact, not a one-time snapshot.
**Downsides:** Private repos need PAT storage (even if ephemeral per-session). Railway's ephemeral disk makes clone transient; large repos may approach timeout. Adds a network dependency to the parse flow. Webhook requires Railway's URL to remain stable.
**Confidence:** 84%
**Complexity:** Medium
**Status:** Unexplored

### 6. Edge Provenance Index
**Description:** Attach provenance metadata to every lineage edge at parse time: which parser produced it (`direct_ast` / `suffix_match` / `temp_view_chain` / `cte_inferred`), the source file path, and the line number. Expose via `/lineage` API. Render in the graph as a hover tooltip ("Defined in `transforms/revenue.sql:142`") linking to the existing code tab.
**Rationale:** Currently a lineage edge is indistinguishable whether it came from an unambiguous INSERT INTO or a heuristic suffix match — both look identical in the graph. Provenance makes this visible and auditable. It is also the foundational data structure that makes Idea #1 (warning attribution to edges) implementable without a separate system.
**Downsides:** Adds a field to `LineageEdge` — must propagate through both branches of `_normalize_edges()` AND `_edge_to_dict()` (an established footgun: every new field has silently defaulted before). Some edges (temp view chains) lose direct line attribution since they span multiple files.
**Confidence:** 82%
**Complexity:** Low
**Status:** Unexplored

### 7. Search Command Palette
**Description:** Promote the existing `/search` endpoint to a command-palette UI — keyboard shortcut (Cmd/Ctrl+K), fuzzy match, immediate jump to the lineage view for the selected column. The current nav search box already has the wiring; this makes it the primary navigation surface. A "go anywhere" pattern inspired by Linear/Raycast.
**Rationale:** Analysts know the column they want, not the table path to navigate to it. The backend `/search` is already built and underused. Once in place, this multiplies the value of every other feature by reducing navigation friction. Purely frontend, bounded scope, low risk.
**Downsides:** Keyboard shortcut conflicts possible on some browsers. Generic column names (`id`, `name`) return noisy results — needs result grouping by table. Perceived as polish rather than new capability; may not change retention.
**Confidence:** 80%
**Complexity:** Low
**Status:** Unexplored

---

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | Streaming Parse (SSE) | Real value but high complexity (SSE + incremental @xyflow render); not the highest priority now |
| 2 | Single-File Drop | Corpus-less normalization produces confusing dangling nodes — partial lineage misleads |
| 3 | Warning Taxonomy (standalone) | Infrastructure implied by idea #1; not a standalone user-facing feature |
| 4 | Downstream Analyst View | Impact page already covers this use case; not a distinct new capability |
| 5 | Warnings as Homepage | Too disruptive a reframe for uncertain gain; Sources page structure is reasonable |
| 6 | Interactive Warning Fix (dropdown) | Backend candidate exposure not yet available; premature — ship warning attribution first |
| 7 | Auto-Patch Mode | SQL generation for broken references adds trust risk; too speculative |
| 8 | Parser Plugin Interface | Premature abstraction — only 3 parsers, no community extension signal |
| 9 | /tmp Persistence | Railway may clear /tmp on restart; fragile false confidence |
| 10 | Webhook Trigger | Sequentially blocked on GitHub integration (idea #5) |
| 11 | Graph Diff Engine | Blocked on export (idea #4); natural follow-on once export exists, not standalone |
| 12 | Team Workspace / Ownership | Needs persistent storage + auth; explicitly out of scope for current architecture |
| 13 | Blast Radius Push (impact without graph nav) | Covered by idea #3 (transitive confidence) + existing Impact page |
| 14 | GPS Accuracy Halos | Subsumed by idea #3 (visual graph layer); not worth separate treatment |
| 15 | Journalist inline disclosure | Good framing; fully covered by idea #1 (actionable warning system) |
