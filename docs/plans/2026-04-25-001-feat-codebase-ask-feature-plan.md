---
title: "feat: Add Codebase Ask Page"
type: feat
status: active
date: 2026-04-25
origin: docs/brainstorms/2026-04-25-codebase-ask-feature-requirements.md
---

# feat: Add Codebase Ask Page

## Overview

Adds a new `/ask` page that maps natural-language question patterns to deterministic graph
queries over the existing lineage graph and a new named-construct index derived from the
parsers. No LLM calls are made at any point — build time or query time. All answers are
structured data (file lists, column lists, construct lists, role tables).

---

## Problem Frame

Users can browse lineage, catalog, and impact through the existing page UIs, but cannot
ask cross-file discovery questions without clicking through the graph. Questions like "which
notebooks touch `orders_final`?" or "find all places `customer_id` is filtered" require
either knowing the UI navigation path or manually searching the graph. The Ask page exposes
the lineage graph and a new named-construct index (CTE names, PySpark DataFrame variables)
through a bounded set of keyword-template query patterns. (see origin:
`docs/brainstorms/2026-04-25-codebase-ask-feature-requirements.md`)

---

## Requirements Trace

- R1. Extend SQL parser to retain CTE names as a construct index (parallel to edge resolution)
- R2. Extend PySpark parser to record DataFrame variable assignments as construct index entries
- R3. Store per-source construct index in state; maintain a global merged list
- R4. `GET /ask?q=` endpoint returning `{ intent, entities, results, result_type }`
- R5. Two-step NL parsing: entity extraction → keyword-rule intent classification; help fallback on no match
- R6. Eight supported intents: `files_for_table`, `upstream_of_column`, `downstream_of_column`, `filter_search`, `join_search`, `file_targets`, `constructs_search`, `table_role`
- R7. Partial table name resolution via suffix-match (reuse `_normalize_edges` logic)
- R8. New `/ask` frontend page with nav link
- R9. Quick-ask chips that pre-fill the search bar
- R10. Structured result cards with intent label and result count
- R11. Help fallback with real examples from the active source
- R12. Source selector (`?source_id=` query param on backend; dropdown in frontend)
- R13. Deep-link buttons to Lineage/Catalog/Impact pages

**Origin acceptance examples:** AE1 (covers R4, R6 `files_for_table`), AE2 (R6 `filter_search`), AE3 (R6 `file_targets`), AE4 (R5, R7), AE5 (R1, R3, R6 `constructs_search`), AE6 (R2, R3, R6 `constructs_search`), AE7 (R11), AE8 (R10, R13)

---

## Scope Boundaries

- Natural language generation (prose explanations, summaries) — out of scope
- True NLP / intent classification beyond keyword rules — out of scope
- Fuzzy or edit-distance entity matching — out of scope; exact substring + suffix-match only
- Semantic / vector similarity search — out of scope
- Ask history persistence — out of scope; page is stateless
- Graphify or any external graph library — dropped
- Lambda / higher-order function variable extraction in PySpark — out of scope (consistent with existing parser scope boundary)

---

## Context & Research

### Relevant Code and Patterns

- `backend/parsers/sql.py` — `parse_sql()` and `_parse_single_statement()`: CTE names available via `_resolve_ctes()` return values `(cte_map, multi_cte_bodies)`. Keys are CTE alias strings with `source_file` available in the surrounding scope. The `_raw_out: list[LineageEdge] | None = None` parameter pattern in `parse_sql()` is the established way to thread a side-channel output list through the parse stack — replicate this for `_constructs_out`.
- `backend/parsers/pyspark.py` — `_DataFrameTracker.visit_Assign()`: every branch that sets `self.df_sources[var]` represents a DataFrame variable assignment. The `var` is the construct name; `node.lineno` provides the line. The `spark.sql(...)` assignment path (early-return branch) creates a DataFrame without setting `df_sources`; capture it separately with `type="dataframe_sql"`.
- `backend/lineage/engine.py` — `_parse_file()` already accepts `raw_edges_out: list[LineageEdge] | None = None`; add a parallel `constructs_out: list[dict] | None = None` param and thread through `parse_sql()` / `parse_pyspark()`.
- `backend/lineage/engine.py` — `_normalize_edges()` suffix-match logic: `short_to_long` maps short names to full qualified names. Replicate this logic in the ask query engine to resolve partial entity names from user questions.
- `backend/state.py` — currently has `source_registry`, `lineage_graph`, `raw_graph`, `parse_warnings`. Add `construct_index: list[dict]`.
- `backend/api/routes.py` — `refresh_source()` already collects `_raw_out` side-channel via `build_graph_with_warnings`; extend pattern for constructs. `_remove_source_files()` handles graph cleanup on re-refresh/delete; add parallel construct cleanup there. `test_routes.py`'s `reset_state` fixture clears state — must add `state.construct_index.clear()`.
- `backend/api/routes.py` — `GET /search` is a direct structural query over graph nodes; the ask endpoint follows the same pattern, extended with intent parsing.
- `frontend/components/nav.tsx` — `links` array drives the nav. Add `{ href: "/ask", label: "Ask" }`.
- `frontend/lib/api.ts` — typed fetch wrappers. Add `AskResponse` type and `api.ask()` function.
- `frontend/lib/hooks.ts` — React Query hooks. Add `useAsk(q, sourceId)` hook.
- `frontend/app/catalog/page.tsx` — representative page pattern: `"use client"`, React Query hooks, Lucide icons, Tailwind classes, shadcn/ui primitives.

### Institutional Learnings

- `docs/solutions/` — no directly matching solutions found; proceed from codebase patterns.

### External References

- None required; all patterns are well-established in the local codebase.

---

## Key Technical Decisions

- **`_constructs_out` threading pattern**: Follow the existing `_raw_out: list | None = None` side-channel pattern in `parse_sql()` and `_parse_file()`. Avoids changing function return types; keeps the parser surface stable.
- **Flat list for `state.construct_index`**: Each entry carries `{ name, type, source_file, source_line, source_cell, source_id }`. Per-source filtering is `O(n)` list comprehension. A nested dict keyed by `source_id` would be marginally faster but adds indirection for the common "all sources" case. List wins on simplicity.
- **Separate `api/ask.py` module**: The query engine (entity extraction, intent classification, handlers) is substantial enough to live outside `routes.py`. The route handler in `routes.py` delegates to `api/ask.py` functions, keeping `routes.py` thin.
- **Source scoping via post-filter, not subgraph**: The merged `state.lineage_graph` has no per-source node partitioning. Scoping a query to `source_id` means collecting that source's `_parsed_files` from `state.source_registry` and post-filtering edges/results whose `source_file` is in that set. Simpler than building per-source subgraphs.
- **Help fallback generates real examples**: When intent classification fails, the response returns `result_type: "help"` with example questions built from the actual table/column/construct names in the active source — not generic templates — so users can copy-paste a working question.
- **`table_role` intent delegates to existing `/tables` logic**: The intent handler calls the same role-classification code already used in `GET /tables` rather than reimplementing it.

---

## Open Questions

### Resolved During Planning

- **CTE capture point**: In `_parse_single_statement()`, after `_resolve_ctes()` returns `(cte_map, multi_cte_bodies)`. Both dicts' keys are CTE alias strings; append to `_constructs_out` before returning edges. Source file/line/cell available from surrounding params.
- **Which PySpark assignments to include**: All DataFrame variable assignments that are tracked in `df_sources`. Include `spark.sql(...)` assignments as `type="dataframe_sql"`. This covers the full range from "show all DataFrames in pipeline.py" use cases.
- **`state.construct_index` shape**: Flat list with `source_id` per entry. Mirror the `state.parse_warnings` list (which also has `source_id` per entry).
- **Entity tokenization**: Whitespace split; each token is matched case-insensitively against the graph's known table names (via suffix-match), column names (exact column-part match), file names (substring match), and construct names (substring match).
- **Ask page layout**: Full page route `/ask`. Defer slide-over panel consideration to user feedback.

### Deferred to Implementation

- **`node_modules/next/dist/docs/` guidance**: Per `frontend/AGENTS.md`, the Next.js version in use may differ from training data. Read the docs in `node_modules/next/dist/docs/` before writing any frontend code to verify correct `useSearchParams`, `useRouter`, and App Router conventions.
- **`state.raw_graph` reset**: `test_routes.py`'s `reset_state` fixture resets `state.lineage_graph` but not `state.raw_graph`. Verify if this causes any latent test isolation issues when adding construct_index to the fixture — fix if needed.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

**Parse-time data flow (on source refresh):**

```
FileRecords
  → _parse_file(record, constructs_out=[...])
      → parse_sql(..., _constructs_out=[...])         # SQL: CTE names captured
      → parse_pyspark(..., _constructs_out=[...])     # PySpark: DataFrame vars captured
  → build_graph_with_warnings(records, _constructs_out=new_constructs)
  → state.lineage_graph  (existing — unchanged)
  → state.construct_index (new — flat list extended with new_constructs)
```

**Query-time data flow (on GET /ask):**

```
GET /ask?q="which notebooks touch orders"&source_id=abc

  api/ask.py::parse_question(q, lineage_graph, construct_index, source_id?)
    1. entity_extraction: tokenize q → match tokens against known tables/cols/files/constructs
    2. intent_classification: keyword-rule match → intent_name + entities
    3. intent_handler(intent_name, entities, lineage_graph, construct_index, source_id?)
       → post-filter by source's _parsed_files when source_id specified

  → { intent: "files_for_table",
      entities: ["catalog.schema.orders"],
      results: [{ file: "etl/orders.sql" }, ...],
      result_type: "file_list" }
```

**Intent → operation map:**

| Intent | Primary operation | Source scoping |
|---|---|---|
| `files_for_table` | Scan lineage_graph edges; collect `source_file` where table matches | Filter by `_parsed_files` |
| `upstream_of_column` | `engine.upstream(lineage_graph, col_id)` | Post-filter edges by `source_file` |
| `downstream_of_column` | `engine.downstream(lineage_graph, col_id)` | Post-filter edges by `source_file` |
| `filter_search` | Find nodes ending `.__filter__` from entity source columns | Filter by `source_file` |
| `join_search` | Find nodes ending `.__joinkey__` from entity source columns | Filter by `source_file` |
| `file_targets` | Scan edges where `source_file` matches file entity; collect targets | N/A (file IS the scope) |
| `constructs_search` | Filter `construct_index` by type keyword and optional name substring | Filter by `source_id` |
| `table_role` | Reuse role classification from `GET /tables` logic | Filter graph by `_parsed_files` |

---

## Implementation Units

- U1. **SQL construct index**

**Goal:** Extend `parse_sql()` and `_parse_single_statement()` to capture CTE names into an optional `_constructs_out` list, following the `_raw_out` side-channel pattern.

**Requirements:** R1

**Dependencies:** None

**Files:**
- Modify: `backend/parsers/sql.py`
- Test: `backend/tests/test_sql_parser.py`

**Approach:**
- Add `_constructs_out: list[dict] | None = None` to `_parse_single_statement()` and `parse_sql()`. Thread it through as `_raw_out` is threaded (pass-through in recursive Databricks notebook calls).
- In `_parse_single_statement()`, after `_resolve_ctes(statement)` returns `(cte_map, multi_cte_bodies)`, if `_constructs_out is not None`, append one entry per CTE alias (`type="cte"`, `name=alias`, `source_file=source_file`, `source_line=source_line`, `source_cell=source_cell`).
- No changes to CTE resolution, temp view resolution, or edge output — this is strictly additive.

**Patterns to follow:**
- `_raw_out: list[LineageEdge] | None = None` in `parse_sql()` and `_parse_file()` — exact same threading pattern.

**Test scenarios:**
- Happy path: SQL with a single CTE → `_constructs_out` contains one entry with correct `name`, `type="cte"`, `source_file`
- Happy path: SQL with two CTEs → two entries in output
- Multi-source CTE (has JOINs, lands in `multi_cte_bodies`) → captured with correct name
- CTE chain (`cte2 AS (SELECT * FROM cte1)`) → both `cte1` and `cte2` captured as separate entries
- SQL with no CTEs → `_constructs_out` unchanged (no entries added)
- Databricks notebook format (multiple cells, some with CTEs) → CTEs from each cell captured, `source_cell` field populated correctly
- `_constructs_out=None` (default) → no error; existing behavior unchanged

**Verification:**
- All new test scenarios pass
- Existing `test_sql_parser.py` tests continue to pass without modification

---

- U2. **PySpark construct index**

**Goal:** Extend `_DataFrameTracker` and `parse_pyspark()` to capture DataFrame variable names into an optional `_constructs_out` list.

**Requirements:** R2

**Dependencies:** None

**Files:**
- Modify: `backend/parsers/pyspark.py`
- Test: `backend/tests/test_pyspark_parser.py`

**Approach:**
- Add `_constructs_out: list[dict] | None = None` to `_DataFrameTracker.__init__()` and store as `self._constructs_out`.
- In `visit_Assign()`, at the point where `df_sources[var]` is set in each branch (`spark.read.table`, `.select`, `.withColumn`, `.agg`, `.join`, pass-through operations), append to `self._constructs_out` if not None: `{ name: var, type: "dataframe", source_file: ..., source_line: node.lineno, source_cell: None }`.
- For the `spark.sql(...)` early-return branch in `visit_Assign()` (which doesn't set `df_sources`), append with `type="dataframe_sql"`.
- Add `_constructs_out: list[dict] | None = None` param to `parse_pyspark()`, passing it to `_DataFrameTracker`.
- In `_parse_databricks_py()`, thread `_constructs_out` through `parse_pyspark()` calls for Python cells. After cell parsing, update `source_cell` on new construct entries to match `cell_idx`.

**Patterns to follow:**
- `_warnings: list[str] | None = None` in `_DataFrameTracker.__init__()` and in `parse_pyspark()` — exact same optional threading pattern to replicate for `_constructs_out`.

**Test scenarios:**
- Happy path: `df = spark.read.table("orders")` → one entry `{ name: "df", type: "dataframe", source_file: ... }`
- Happy path: `df2 = df.select("id", "amount")` → entry for `df2` captured
- `spark.sql(...)` assignment: `result = spark.sql("SELECT ...")` → entry with `type="dataframe_sql"`
- `.join()` producing merged DataFrame → entry captured
- `.filter()` / `.where()` → entry captured (pass-through)
- Databricks .py notebook format (multiple cells) → DataFrames across cells captured with correct `source_cell`
- `_constructs_out=None` (default) → no error; existing behavior unchanged
- Existing lineage edges unchanged by new parameter — spot check one PySpark file

**Verification:**
- All new test scenarios pass
- Existing `test_pyspark_parser.py` tests continue to pass

---

- U3. **State and engine wiring**

**Goal:** Add `state.construct_index`, thread `_constructs_out` through `engine.py`'s parse pipeline, and wire collect/clear into `routes.py` refresh and delete flows.

**Requirements:** R3

**Dependencies:** U1, U2

**Files:**
- Modify: `backend/state.py`
- Modify: `backend/lineage/engine.py`
- Modify: `backend/parsers/notebook.py`
- Modify: `backend/api/routes.py`
- Test: `backend/tests/test_routes.py`

**Approach:**
- `state.py`: add `construct_index: list[dict] = []`.
- `engine.py` — `_parse_file()`: add `constructs_out: list[dict] | None = None`. Pass to `parse_sql(..., _constructs_out=constructs_out)` and `parse_pyspark(..., _constructs_out=constructs_out)`.
- `parsers/notebook.py` — `parse_notebook()` currently accepts `_warnings` and `_raw_out` but not `_constructs_out`. Add `_constructs_out: list[dict] | None = None` parameter and thread it through calls to `parse_sql()` and `parse_pyspark()` within the notebook parser.
- `engine.py` — `build_graph_with_warnings()`: add `_constructs_out: list[dict] | None = None`. Pass to `_parse_file()` calls. No return type change.
- `routes.py` — `refresh_source()`: create `new_constructs: list[dict] = []`, pass to `build_graph_with_warnings(records, _constructs_out=new_constructs)`. After building the graph, stamp each construct with `source_id` (`c["source_id"] = source_id`). Remove stale constructs: `state.construct_index = [c for c in state.construct_index if c["source_id"] != source_id]`, then extend: `state.construct_index.extend(new_constructs)`.
- `routes.py` — `delete_source()`: add `state.construct_index = [c for c in state.construct_index if c["source_id"] != source_id]`.
- `test_routes.py` — `reset_state` fixture: add `state.construct_index.clear()`. Also add `state.raw_graph = nx.DiGraph()` if it is absent (verify during implementation).

**Patterns to follow:**
- `raw_edges_out: list[LineageEdge] | None = None` threading in `_parse_file()` → same pattern for `constructs_out`.
- `_remove_source_files()` pattern for cleanup.

**Test scenarios:**
- Happy path: register + refresh a source with SQL CTEs → `state.construct_index` has entries with correct `source_id`
- Re-refresh: old constructs replaced, no duplicates — `state.construct_index` has exactly the new set
- Delete source → constructs for that source removed from `state.construct_index`; other sources' constructs remain
- `reset_state` fixture: `state.construct_index` is empty before each test in `test_routes.py`

**Verification:**
- `GET /sources/{id}/refresh` populates `state.construct_index` for that source
- All existing `test_routes.py` tests pass after fixture update

---

- U4. **Ask query engine**

**Goal:** Implement `api/ask.py` with entity extraction, intent classification, and all 8 intent handlers. Returns `{ intent, entities, results, result_type }` for valid questions; `{ intent: "help", ... }` for unmatched.

**Requirements:** R4, R5, R6, R7

**Dependencies:** U3

**Files:**
- Create: `backend/api/ask.py`
- Test: `backend/tests/test_ask.py`

**Approach:**

*Entity extraction* (`extract_entities(question, graph, construct_index)`):
1. Tokenize question: lowercase, split on whitespace, strip punctuation.
2. Collect known names from the graph: table names (all `node.rsplit(".", 1)[0]` values), column names (all `node.rsplit(".", 1)[1]` values), file names (all `edge.source_file` values from graph edges).
3. Collect construct names from `construct_index`.
4. For each token: (a) exact match against known names; (b) suffix-match against table names (check if any table name ends with `"." + token` or equals token); (c) substring-match for file names; (d) substring-match for construct names.
5. Return a list of `{ kind: "table"|"column"|"file"|"construct", value: <resolved_name> }` dicts — one per match, deduped.

*Intent classification* (`classify_intent(question_lower, entities)`):
- Ordered keyword checks. First match wins.
- `constructs_search`: question contains "cte", "temp view", "dataframe", "define", "named"
- `files_for_table`: question contains ("which"|"what") + ("file"|"notebook") OR ("touch"|"reference"|"use") + table entity present
- `upstream_of_column`: "upstream"|"comes from"|"source of" + column or table entity present
- `downstream_of_column`: "downstream"|"flows to"|"impacts" + column or table entity present
- `filter_search`: "filter"|"filtered"|"where clause"|"condition" + column or table entity present
- `join_search`: "join"|"joined"|"join key" + column or table entity present
- `file_targets`: ("write"|"produce"|"output") + file entity present
- `table_role`: "source table"|"target table"|"intermediate"|"show all" + role keyword present
- No match → `"help"` intent

*Intent handlers* (one function each, all deterministic graph operations):
- `files_for_table(entities, graph, source_files?)`: iterate `graph.edges(data=True)`, collect distinct `d["data"].source_file` where edge source or target table matches entity. If `source_files` provided, intersect.
- `upstream_of_column(entities, graph, source_files?)`: call `engine.upstream(graph, col_id)` for each matched column entity; post-filter by `source_files` if provided.
- `downstream_of_column(...)`: `engine.downstream(...)` similarly.
- `filter_search(entities, graph, source_files?)`: find all nodes matching `*.__filter__` whose predecessors match entity; collect with file/line info.
- `join_search(...)`: same for `*.__joinkey__`.
- `file_targets(entities, graph)`: iterate edges where `source_file` matches file entity; collect distinct target table names.
- `constructs_search(entities, construct_index, source_id?)`: filter `construct_index` by type keyword (e.g., "cte"→`type="cte"`, "dataframe"→`type in ["dataframe", "dataframe_sql"]`) and optional name substring match.
- `table_role(question, graph, source_files?)`: classify nodes by role (reuse logic from `GET /tables`); filter by requested role keyword.
- `help_result(graph, construct_index, source_id?)`: build examples using first few real table names, file names, and construct names from the active source.

Public entry point: `parse_question(q, graph, construct_index, source_registry, source_id?) → dict`. The `source_registry` param is used to resolve `source_id → _parsed_files` inside the handler calls; passing `None` means no source scoping.

**Patterns to follow:**
- `engine.upstream()` / `engine.downstream()` for traversal.
- `routes.py` `list_tables()` role-classification logic for `table_role` intent.
- Edge iteration: `for u, v, d in state.lineage_graph.edges(data=True): edge = d.get("data")`.

**Test scenarios:**

*`files_for_table`:*
- Covers AE1. Exact table name "orders_final" → returns list of files with edges to/from that table
- Covers AE4. Partial name "orders" suffix-matches "catalog.schema.orders" → same result
- Table exists but has no edges → empty results, `result_type: "file_list"`

*`filter_search`:*
- Covers AE2. Column "customer_id" → returns edges where source matches `*.customer_id` and target is `*.__filter__`
- Column not in graph → empty results

*`file_targets`:*
- Covers AE3. File "orders.sql" → returns distinct target tables from edges with `source_file="orders.sql"`
- File name partial match (substring) → resolved correctly

*`constructs_search`:*
- Covers AE5. "find CTEs named dedup" → `type="cte"`, name contains "dedup"
- Covers AE6. "show dataframes in pipeline.py" → `type in ["dataframe","dataframe_sql"]`, `source_file` contains "pipeline.py"
- No type keyword, only name → searches all construct types
- Empty `construct_index` → empty results, no error

*`upstream_of_column` and `downstream_of_column`:*
- Happy path: valid column ID → returns edges from `engine.upstream()` / `engine.downstream()`
- Column not in graph → empty results

*`join_search`:*
- Table entity with join keys → returns `__joinkey__` edges for that table

*`table_role`:*
- "source tables" → returns tables with role="source"
- "show all target tables" → returns tables with role="target"

*`help` intent:*
- Covers AE7. Unrecognized question → `intent="help"`, `result_type="help"`, examples contain real names from graph
- Empty question → `intent="help"`
- Empty graph → help result with empty examples (no error)

*Source scoping:*
- `source_id` specified → results include only files in that source's `_parsed_files`
- `source_id` not in registry → returns empty results, no 500 error

**Verification:**
- All `test_ask.py` tests pass
- `parse_question("", graph, [], None)` returns `intent="help"` without raising

---

- U5. **Ask API route**

**Goal:** Add `GET /ask?q=&source_id=` endpoint to `api/routes.py` that delegates to `api/ask.py`.

**Requirements:** R4, R12

**Dependencies:** U3, U4

**Files:**
- Modify: `backend/api/routes.py`
- Test: `backend/tests/test_ask.py` (integration scenarios)

**Approach:**
- Add `from api.ask import parse_question` import.
- Add endpoint after the `# ── Search ──` section:
  ```
  GET /ask?q=<str>&source_id=<optional str>
  ```
  Calls `parse_question(q, state.lineage_graph, state.construct_index, state.source_registry, source_id or None)`.
  Returns the dict directly. No `HTTPException` needed for no-match — that returns `intent="help"`.

**Patterns to follow:**
- `GET /search` in `routes.py` — same simple GET handler calling a pure function on `state.lineage_graph`.

**Test scenarios:**
- Integration: upload + refresh a source with SQL; `GET /ask?q=which+files+touch+orders` returns `intent="files_for_table"` with real results
- `source_id` param scopes to that source's files only
- `?q=` (empty) → `intent="help"` with 200 OK
- `source_id` that does not exist → 200 OK with scoped-empty results (not 404)

**Verification:**
- `GET /ask?q=...` returns 200 with `{ intent, entities, results, result_type }` shape
- No regression in existing route tests

---

- U6. **Frontend Ask page**

**Goal:** Add `/ask` page with search bar, quick-ask chips, structured result cards, source selector, and deep-link buttons. Add "Ask" nav link, `AskResponse` type, `api.ask()` function, and `useAsk()` hook.

**Requirements:** R8, R9, R10, R11, R12, R13

**Dependencies:** U5

**Files:**
- Create: `frontend/app/ask/page.tsx`
- Modify: `frontend/components/nav.tsx`
- Modify: `frontend/lib/api.ts`
- Modify: `frontend/lib/hooks.ts`

**Approach:**

*`lib/api.ts`*: Add types and API function:
```
AskResult: { file?: string; column?: string; table?: string; name?: string; type?: string; source_file?: string; role?: string }
AskResponse: { intent: string; entities: string[]; results: AskResult[]; result_type: string }
api.ask(q, sourceId?): apiFetch<AskResponse>(...)
```

*`lib/hooks.ts`*: Add `useAsk(q: string, sourceId: string | null)` — `useQuery` with `enabled: q.length >= 2`, `queryKey: ["ask", q, sourceId]`, calling `api.ask(q, sourceId)`.

*`components/nav.tsx`*: Add `{ href: "/ask", label: "Ask" }` to the `links` array.

*`app/ask/page.tsx`*: `"use client"` page with:
- Top: source selector `<select>` populated from `useSources()`. Stores selected `sourceId` in state.
- Search bar: controlled `<input>` with a magnifying glass icon (Lucide `Search`). Calls `useAsk(q, sourceId)` when `q.length >= 2`.
- Quick-ask chips row: four `<button>` elements for "Which files touch…", "Where is … filtered?", "What's upstream of…", "Find CTEs named…". Click sets the input value.
- Result card: shown when `data` is present. Displays intent label (human-readable mapping from intent key), result count badge, and a result list appropriate to `result_type`:
  - `"file_list"` → list of file paths with "View in Lineage →" link when a matching column node exists
  - `"edge_list"` → table of `source_col → target_col` with `source_file` and `source_line`
  - `"construct_list"` → table of `name`, `type`, `source_file`, `source_line`
  - `"table_list"` → table of `table`, `role` with "View in Catalog →" link
  - `"help"` → "Try asking:" section with example chips (clickable)
- Loading state: spinner while query is in-flight.
- Empty state (no query yet): brief description of what the page does.

Per `frontend/AGENTS.md`: read `node_modules/next/dist/docs/` before implementing to confirm `useSearchParams` / `useRouter` conventions for this Next.js version.

**Patterns to follow:**
- `frontend/app/catalog/page.tsx` — page structure, hook usage, Tailwind classes, router navigation.
- `frontend/components/nav.tsx` `links` array pattern.
- `frontend/lib/hooks.ts` `useSearch` hook — same `useQuery` pattern with `enabled` guard.

**Test scenarios (verified via dev server):**

*Happy path — nav and page load (R8):*
- Navigate to `/ask` → page loads without error; "Ask" link is active/highlighted in the nav
- "Ask" link is visible in the nav on all other pages (Lineage, Catalog, Impact, Sources)

*Happy path — search bar and chips (R9):*
- Type at least 2 characters into the search bar → `useAsk` query fires; no error thrown
- Click each quick-ask chip → search bar pre-fills with the chip's template text; cursor positioned at end

*Happy path — structured result cards (R10):*
- With a source that has SQL CTEs uploaded, type a `files_for_table` question → result card appears showing intent label "Files referencing …", file list, and result count badge matching the list length
- With a `constructs_search` question → result card shows a `construct_list` table with `name`, `type`, `source_file`, `source_line` columns

*Help fallback (R11, covers AE7):*
- Type an unrecognized question (e.g., "what is the meaning of life") → result card shows `intent="help"`, "Try asking:" section renders with chips containing real table/file names from the active source

*Source selector (R12):*
- Source dropdown is populated from `GET /sources`; selecting a specific source and re-querying → results reflect only that source's files
- Selecting "All sources" → results span the merged graph

*Deep-link buttons (R13, covers AE8):*
- A `files_for_table` result renders a "View in Lineage →" button; clicking it navigates to the Lineage page with the relevant node pre-selected
- A `table_list` result renders "View in Catalog →" buttons

*Edge cases:*
- Empty query (no text) → empty state description visible; no in-flight request made
- Query with only whitespace → no request made; empty state shown
- Source selector with no sources loaded → dropdown shows a placeholder; page does not crash

**Verification:**
- `npm run build` in `frontend/` passes (TypeScript compilation, no type errors)
- `npm run lint` passes
- All dev-server test scenarios above pass manual walk-through

---

## System-Wide Impact

- **Interaction graph:** `routes.py` `refresh_source()` now also populates `state.construct_index`. The `delete_source()` handler clears construct entries for the deleted source. Both are additive changes to existing handlers.
- **Error propagation:** `api/ask.py` functions do not raise. All error states (empty graph, unknown entity, unmatched intent) return a structured response with `intent="help"` or empty `results`. No new HTTP error codes introduced.
- **State lifecycle risks:** `state.construct_index` is cleared by the `reset_state` fixture in `test_routes.py`. The flat list is rebuilt from scratch on each refresh (remove-then-extend), so stale entries cannot accumulate.
- **API surface parity:** `GET /ask` returns `source_id`-scoped results when the parameter is provided. The frontend's source selector must match the `source_id` values from `GET /sources`.
- **Integration coverage:** `test_ask.py` must include at least one end-to-end scenario that registers a source, refreshes it, and then calls `GET /ask` to verify the full parse→index→query chain works together.
- **Unchanged invariants:** `lineage_graph`, `raw_graph`, all existing routes, and all existing parser return types are unchanged. The construct index is strictly additive. `engine.upstream()` / `engine.downstream()` are called, not modified.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| `parsers/notebook.py` calls `parse_sql()` / `parse_pyspark()` without `_constructs_out` | Confirmed during planning; `notebook.py` is in U3's Modify list with explicit threading instructions |
| `test_routes.py` `reset_state` fixture does not reset `state.raw_graph` — may cause latent isolation issues when adding `construct_index` | Verify and fix in U3; a one-line addition if needed |
| Entity extraction returns multiple matches for common column names (e.g., `customer_id` in 10 tables) | Intent handlers accept lists of matched entities and union results; documented in `help` response |
| Next.js version in `frontend/` may differ from training data patterns | Per `frontend/AGENTS.md`, read `node_modules/next/dist/docs/` before writing any frontend code in U6 |
| Large corpora (100+ files) produce large `construct_index` lists → `constructs_search` becomes slower | O(n) scan is acceptable for expected corpus sizes; note for future optimization if needed |

---

## Documentation / Operational Notes

- No new environment variables, deployment steps, or Railway/Vercel configuration changes required.
- The `construct_index` lives in the same in-memory state as `lineage_graph` — it is lost on server restart, same as all other state. Users must re-upload after deployment.

---

## Sources & References

- **Origin document:** [docs/brainstorms/2026-04-25-codebase-ask-feature-requirements.md](docs/brainstorms/2026-04-25-codebase-ask-feature-requirements.md)
- Related code: `backend/parsers/sql.py`, `backend/parsers/pyspark.py`, `backend/lineage/engine.py`, `backend/api/routes.py`, `backend/state.py`
- Related code: `frontend/components/nav.tsx`, `frontend/lib/api.ts`, `frontend/lib/hooks.ts`
