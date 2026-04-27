---
date: 2026-04-25
topic: codebase-ask-feature
---

# Codebase Ask Feature

## Problem Frame

Users upload Databricks SQL and PySpark codebases into the DataLineage Explorer. The existing UI answers navigation questions well (lineage graph, catalog, impact analysis), but offers no way to ask cross-file discovery questions — for example, "which notebooks touch the `orders` table?" or "where is `customer_id` filtered?". A new **Ask** page fills this gap by mapping natural-language question patterns to deterministic graph queries with no LLM dependency.

The two primary question types in scope:

- **Codebase-wide search**: "which notebooks/files reference X?", "find all places Y is filtered/joined"
- **Construct discovery**: "what CTEs exist across all notebooks?", "which files define a DataFrame named X?"

Questions requiring natural language *generation* (e.g., "explain this CTE") are explicitly out of scope — those require an LLM. All answers are returned as structured data (lists, tables, counts). Question matching uses keyword-rule templates, not true NLP — supported patterns are bounded and enumerated.

No new Python dependencies are introduced. The feature extends the existing parsers and lineage graph rather than adding an external library.

---

## Requirements

**Named construct index (parser extensions)**

- R1. Extend `parsers/sql.py` to retain CTE names as queryable nodes in the lineage graph instead of resolving them away. Each CTE node must carry a `node_type = "cte"` attribute and a `source_file` attribute. This does not change the column-level lineage edges — temp view resolution still runs as today — but adds a parallel index of named constructs.
- R2. Extend `parsers/pyspark.py` to record DataFrame variable names (the left-hand side of `df = spark.read...`, `df = df.select(...)`, etc.) as construct nodes with `node_type = "dataframe"` and `source_file`. Column-level lineage edges are unchanged.
- R3. Store the construct index per-source in `state.source_registry[source_id]["_construct_index"]`: a list of `{ name, type, source_file, source_line }` dicts. A global merged `state.construct_index` (flat list) is rebuilt on each refresh and used at query time.

**Query engine**

- R4. Implement a `GET /api/backend/ask?q=` endpoint that accepts a free-text question string and returns `{ intent, entities, results, result_type }`. All computation is deterministic — no LLM calls at any point.
- R5. Parse questions using a two-step pipeline: (1) entity extraction — match tokens in the question against known table names, column names, and file names from the lineage graph; (2) intent classification — keyword-rule matching against a defined intent set (see R6). If no intent is matched, return a `help` result listing supported question patterns with real examples drawn from the current source's table and file names.
- R6. Support the following named intents, each mapping to a graph or index operation:

  | Intent | Trigger keywords | Operation |
  |---|---|---|
  | `files_for_table` | "which files", "which notebooks", "touch", "reference", "use" + table entity | Filter lineage edges by table; collect distinct `source_file` values |
  | `upstream_of_column` | "upstream", "comes from", "source of" + column entity | `engine_upstream()` on lineage graph |
  | `downstream_of_column` | "downstream", "flows to", "impacts" + column entity | `engine_downstream()` on lineage graph |
  | `filter_search` | "filter", "where clause", "filtered", "condition" + column/table entity | Find `__filter__` pseudo-column edges whose source column matches entity |
  | `join_search` | "join", "joined on", "join key" + column/table entity | Find `__joinkey__` pseudo-column edges matching entity |
  | `file_targets` | "write", "produce", "output" + file entity | Filter edges by `source_file`, collect distinct target tables |
  | `constructs_search` | "CTE", "temp view", "dataframe", "function", "define", "named" + optional name entity | Query `state.construct_index` for matching entries |
  | `table_role` | "source tables", "target tables", "intermediate", "show all" + role keyword | Query lineage graph node roles (existing `/tables` logic) |

- R7. Entity extraction resolves partial names: a token matching a table name suffix (e.g., `orders` matching `catalog.schema.orders`) uses the same suffix-match logic already in `lineage/engine.py` (`_normalize_edges`). File name matching is case-insensitive substring match against `source_file` values in the graph.

**Frontend**

- R8. Add a new `/ask` page accessible from the top nav, labelled "Ask".
- R9. The page shows a single prominent search bar at the top. Below it, a row of quick-ask chips for the most common question patterns: "Which files touch…", "Where is … filtered?", "What's upstream of…", "Find CTEs named…". Clicking a chip pre-fills the search bar with the template text.
- R10. Results display as structured cards below the search bar. Each card shows: matched intent label (e.g., "Files referencing `orders`"), a result list (table of files, columns, or constructs), and a result count badge.
- R11. If no intent is matched, the result card shows a "Try asking:" help section listing all supported patterns with concrete examples using the current source's actual table and file names.
- R12. A source selector dropdown at the top-right scopes queries to a single uploaded source or "All sources" (merged global graph/index).
- R13. Results that map to existing pages include deep-link buttons — "View in Lineage →", "View in Catalog →", "View in Impact →" — so users can navigate directly to the relevant node.

---

## Acceptance Examples

- AE1. **Covers R4, R6 (files_for_table).** User types "which notebooks touch orders_final". Response: `{ intent: "files_for_table", entities: ["catalog.schema.orders_final"], results: ["etl/orders.sql", "notebooks/finalize.sql"], result_type: "file_list" }`.

- AE2. **Covers R6 (filter_search).** User types "find all places customer_id is filtered". Response: list of edges where source column matches `customer_id` and target is a `__filter__` pseudo-column, grouped by file and line number.

- AE3. **Covers R6 (file_targets).** User types "what does orders.sql write to". Response: all distinct target tables produced by edges with `source_file = "orders.sql"`.

- AE4. **Covers R5, R7 (partial match).** User types "which files reference orders". The partial name `orders` suffix-resolves to `catalog.schema.orders` and returns the file list.

- AE5. **Covers R1, R3, R6 (constructs_search).** User types "find CTEs named dedup". The construct index returns all entries with `type = "cte"` and name containing "dedup", with file and line.

- AE6. **Covers R2, R3, R6 (constructs_search, pyspark).** User types "show dataframes in pipeline.py". The construct index returns all `type = "dataframe"` entries with `source_file = "pipeline.py"`.

- AE7. **Covers R11 (help fallback).** User types "what is the meaning of life". Response card shows "Try asking:" with real examples like "Which files touch `catalog.schema.orders`?" and "Find CTEs named dedup".

- AE8. **Covers R10, R13 (deep links).** A `files_for_table` result for a column node includes a "View in Lineage →" button that opens the Lineage page pre-selected to that column.

---

## Success Criteria

- All named intents return results in under 200ms for a codebase of 100 files (pure in-memory operations, no I/O at query time).
- Source refresh time does not increase by more than 500ms after the parser construct-index additions for a 50-file upload.
- No network calls or LLM API calls are made at any point in the ask flow.
- The help fallback fires for any unmatched question and shows real examples from the active source.
- No new Python packages are introduced.
- All existing tests continue to pass; each new intent and the construct index are covered by tests in `backend/tests/test_ask.py` and `backend/tests/test_sql_parser.py` / `test_pyspark_parser.py`.

---

## Scope Boundaries

- Natural language generation (prose explanations, summaries) — out of scope. All answers are structured data.
- True NLP / intent classification beyond keyword rules — out of scope. The question parser supports a bounded set of named patterns.
- Fuzzy or edit-distance entity matching — out of scope. Entity extraction uses exact substring match and suffix-match.
- Semantic / vector similarity search — out of scope.
- Ask history persistence — out of scope. The page is stateless.
- Graphify or any external graph library — dropped. Named constructs come from extending the existing parsers.
- Lambda / higher-order function variable extraction in PySpark (`transform(arr, x -> x.field)`) — out of scope for the construct index; consistent with the existing parser's scope boundary.

---

## Key Decisions

- **No new dependencies:** Named constructs are retained directly in the existing parsers (SQLGlot for SQL, Python `ast` for PySpark) rather than re-parsing with tree-sitter via Graphify. This keeps the system self-contained.
- **No LLM anywhere:** Both the construct index (parser-derived) and the query engine (keyword rules + graph traversal) are fully deterministic.
- **Construct index as a flat list per source, not a graph:** A separate NetworkX graph for constructs would add overhead without benefit for the bounded query types supported. A flat list with metadata is simpler and sufficient.
- **Template-based intent parsing:** The system explicitly does not attempt true NLP. Supported question patterns are enumerated and documented. The help fallback surfaces them clearly so users learn the vocabulary quickly.
- **Construct nodes parallel lineage edges — they don't replace them:** CTE resolution and temp view short-circuiting continue to run as today. Construct nodes are an additive index layer, not a change to the lineage model.

---

## Dependencies / Assumptions

- The `source_file` field on `LineageEdge` is already populated for all parsers — confirmed in `routes.py`. No parser changes required for `files_for_table`, `file_targets`, `filter_search`, or `join_search` intents.
- Existing `engine_upstream()` / `engine_downstream()` are reusable as-is for `upstream_of_column` and `downstream_of_column`.
- The suffix-match logic for partial table-name resolution is already in `lineage/engine.py` (`_normalize_edges`). It must be extracted or duplicated for use in entity extraction at query time.
- CTE names are accessible in the SQL parser's AST traversal (`exp.CTE` nodes in SQLGlot) before resolution runs — confirm the extraction point during planning.
- DataFrame assignments are already tracked in `parsers/pyspark.py` (the variable-tracking dict) — confirm which assignments should be surfaced as construct nodes vs. kept internal.

---

## Outstanding Questions

### Resolve Before Planning

*(none)*

### Deferred to Planning

- [Affects R1][Technical] Identify the correct point in `parsers/sql.py` to capture CTE names — before or alongside `_resolve_temp_views` — without disrupting existing resolution.
- [Affects R2][Technical] Define which PySpark DataFrame assignments qualify for the construct index: only `spark.read.*` / `spark.sql()` entry points, or all intermediate DataFrame reassignments too.
- [Affects R3][Technical] Decide whether `state.construct_index` is a flat list or a dict keyed by source_id for efficient per-source filtering.
- [Affects R5][Technical] Decide how entity extraction tokenizes the question: whitespace split is simple but misses multi-word table names with spaces (unlikely in practice but worth confirming).
- [Affects R9][Frontend] Decide whether the Ask page is a full page route or a slide-over panel — a panel keeps the lineage graph visible while querying. Default to full page; revisit if user feedback prefers context-aware access.

---

## Next Steps

-> `/ce-plan` for structured implementation planning
