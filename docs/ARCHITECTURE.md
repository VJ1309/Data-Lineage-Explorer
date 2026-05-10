# Architecture

DataLineage Explorer parses SQL and PySpark code into a column-level lineage DAG and serves it over a small REST API. This document captures the data flow, the load-bearing invariants, and the state model.

## Data flow

```
ZIP upload
   │
   ▼
ingestion/upload.py        ingest_zip(bytes, source_ref) → list[FileRecord]
   │                       Classifies by extension: .sql → sql, .py → python, .ipynb → notebook.
   │                       Invalid ZIP → raises ValueError → routes.py returns 400.
   ▼
lineage/engine.py          _parse_file(record) dispatches per type:
   │                         sql      → parsers/sql.py     (SQLGlot AST, dialect="databricks")
   │                         python   → parsers/pyspark.py (Python ast, DataFrame variable tracking)
   │                         notebook → parsers/notebook.py (nbformat → cell-by-cell dispatch)
   │                       Returns (edges, raw_edges, warnings).
   ▼
build_graph_with_warnings  Aggregates per-file results, runs _normalize_edges()
   │                       (lowercase + suffix-match short table names → catalog.schema.table),
   │                       builds two NetworkX DiGraphs.
   ▼
state.py                   lineage_graph (post temp-view resolution, used by /lineage)
                           raw_graph     (pre  temp-view resolution, used by /lineage/paths)
                           parse_warnings
                           source_registry
   ▲
   │ queries
   │
api/routes.py              REST endpoints traverse the DAG via engine_upstream / engine_downstream (BFS).
```

## Two graphs, not one

| Graph | Built from | Used by | Why |
|---|---|---|---|
| `lineage_graph` | edges with temp views resolved | `/lineage`, `/impact`, `/tables`, `/search` | Temp views are implementation detail, not lineage. Consumers want "this column flows from that source column," skipping the intermediate `CREATE TEMP VIEW`. |
| `raw_graph` | edges as parsed, before resolution | `/lineage/paths`, `/lineage/trace` | Path tracing and Lineage Trace need to show the *actual* hops a value took, including through temp views, so predicates and transformations stay attributable to the right SQL statement. The Trace explicitly *requires* `raw_graph` — temp-view filters live there only. |

Keep them in sync. Any code that mutates one (refresh, delete) must mutate the other the same way.

## Key invariants

1. **Column IDs are 4-part: `catalog.schema.table.column`.** Always split with `lineage.ids.split_column_id()` (uses `rfind`/`rsplit`). Never `.split(".", 1)` — that splits at the catalog dot. `ColumnNode.__post_init__` enforces `id == f"{table}.{column}"`.

2. **`_normalize_edges()` runs before graph construction, not during query.** It lowercases identifiers and suffix-matches short table references against fully-qualified ones. Querying assumes everything is already normalized.

3. **Temp view resolution is iterative.** Chains (`v1` → `v2` → `v3`) are resolved in a single pass in `parsers/sql.py::resolve_temp_views`. The `_resolve_views=False` kwarg on `parse_sql()` exists so notebook parsers can collect all cells' temp views before resolution — don't call it with `True` from inside a notebook flow.

4. **Notebook detection is by separator string, not metadata.** Databricks `.sql` notebooks contain `-- COMMAND ----------`; Databricks `.py` notebooks contain `# COMMAND ----------` and `# MAGIC %sql`. Files without these are treated as plain SQL or plain Python.

5. **`ParseResult` is iterable.** It yields edges (not raw_edges) so legacy callers that did `for e in parse_sql(...)` still work. New code should use `result.edges` / `result.raw_edges` explicitly.

## State model

All state lives in module-level globals in `backend/state.py`:

- `source_registry: dict[str, SourceEntry]` — registered sources keyed by 8-char UUID prefix.
- `lineage_graph: nx.DiGraph` — resolved DAG.
- `raw_graph: nx.DiGraph` — unresolved DAG.
- `parse_warnings: list[dict]` — warnings from the last refresh of each source.

**Lifecycle:**
- **Server start** — all empty. No persistence layer.
- **`POST /sources`** — registers a source, parses files, adds edges to both graphs.
- **`POST /sources/{id}/refresh`** — removes that source's edges from both graphs and re-parses.
- **`DELETE /sources/{id}`** — removes that source's edges from both graphs, drops orphan nodes, drops its warnings, removes the registry entry. Never touches edges contributed by other sources.
- **Server restart / Railway redeploy** — everything is gone. Users must re-upload.

**Test discipline:** `tests/test_routes.py` resets `source_registry`, `lineage_graph`, `raw_graph`, and `parse_warnings` before each test. Any new global must be reset there too.

## REST surface

Defined in `backend/api/routes.py`. All routes return JSON; errors use the shape `{"error": str, "detail": str}` via the handlers in `main.py`.

| Method | Path | Purpose |
|---|---|---|
| `GET`    | `/sources` | List registered sources. |
| `POST`   | `/sources` | Register a source (form-data: `source_type`, `url`, `token`, `file`). |
| `DELETE` | `/sources/{id}` | Remove a source and its lineage contributions. |
| `GET`    | `/sources/{id}/files` | List parsed files for a source. |
| `POST`   | `/sources/{id}/refresh` | Re-parse a source. |
| `GET`    | `/tables` | All tables grouped by role (source/intermediate/target/result). |
| `GET`    | `/tables/{table}/columns` | Columns of a table. |
| `GET`    | `/lineage` | Upstream + downstream edges of a column (resolved graph). |
| `GET`    | `/lineage/paths` | All raw paths to a column (raw graph). |
| `GET`    | `/lineage/trace` | Per-column **Lineage Trace** Steps for the column inspector. Walks `raw_graph` upstream from the column's writer edges, groups by source-table boundary, and rolls up predicates from collapsed temp views (named in `via_temp_views`). Powers the column-inspector tree. |
| `GET`    | `/impact` | Downstream impact of a column. |
| `GET`    | `/search` | Substring search across columns/tables. |
| `GET`    | `/warnings` | Parse warnings from the last refresh. |

## Frontend coupling points

The frontend talks to the backend exclusively through `frontend/app/api/backend/[...path]/route.ts`, which proxies to `API_URL` server-side. Two conventions to preserve:

- Never expose the backend URL via `NEXT_PUBLIC_*` (it bakes at build time).
- Source-mutating mutations in `frontend/lib/hooks.ts` must call `invalidateLineageData()`. Adding a new derived endpoint? Add it to that invalidator, or stale data will leak across navigations.

## Things that are intentionally not here

- **Auth.** Single-tenant MVP; `CORSMiddleware` allows all origins. If you add auth, also revisit CORS.
- **Persistence.** State is in-memory by design — see ADRs (when added) for the why.
- **A queue.** Parsing is synchronous on the request thread. Big uploads will block. If this becomes a problem, parse off-thread before caching results — don't add Celery.
