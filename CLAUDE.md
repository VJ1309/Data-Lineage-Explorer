# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Layout

```
backend/         FastAPI backend (deployed to Railway)
frontend/        Next.js frontend (deployed to Vercel)
docs/solutions/  documented solutions to past problems (bugs, best practices), organized by category with YAML frontmatter (module, tags, problem_type)
```

Both are committed to the same git repo. The working directory for most backend work is `backend/`; for frontend it is `frontend/`.

---

## Backend

### Commands

```bash
# From backend/
python -m pytest tests/                        # run all tests
python -m pytest tests/test_sql_parser.py -v   # run a single test file
python -m pytest tests/ -k "test_name" -v      # run a single test by name
uvicorn main:app --reload --port 8000          # run dev server
```

Dependencies are managed with `uv` (lockfile at `uv.lock`). The venv is at `../.venv` (repo root level).

### Architecture

**Entry point:** `main.py` — FastAPI app with CORS, mounts `api/routes.py`.

**State:** `state.py` — module-level in-memory globals. `source_registry` (dict of source configs) and `lineage_graph` (NetworkX DiGraph) are rebuilt on each source refresh. All state is lost on server restart.

**Data flow for a parse:**
1. `ingestion/upload.py` — unzips uploaded file, classifies files by extension (`.sql` → sql, `.py` → python, `.ipynb` → notebook), returns `list[FileRecord]`
2. `lineage/engine.py` — calls `_parse_file()` per record (dispatches to SQL/PySpark/notebook parser), collects all `LineageEdge` objects, runs `_normalize_edges()` (lowercase + suffix-match short table names to full `catalog.schema.table` form), builds NetworkX DAG
3. `api/routes.py` — REST endpoints query the DAG using `engine.upstream()` / `engine.downstream()` (BFS)

**Parsers:**
- `parsers/sql.py` — SQLGlot AST, `dialect="databricks"`. Handles multi-statement SQL, schema/catalog-qualified names, CTEs, window functions, temp view resolution. Detects Databricks `.sql` notebook format (`-- COMMAND ----------` separators). `parse_sql()` accepts `_resolve_views=False` for sub-calls from notebook parsers (resolution happens at notebook level).
- `parsers/pyspark.py` — Python `ast` module. Tracks DataFrame variable assignments through `.select()`, `.withColumn()`, `.join()`, `.agg()`, etc. Handles `spark.sql()` calls and Databricks `.py` notebook format (`# COMMAND ----------` / `# MAGIC %sql`).
- `parsers/notebook.py` — Jupyter `.ipynb` notebooks via `nbformat`.

**Key invariant — naming convention:** All column IDs are `catalog.schema.table.column` (4-part). Always use `.rsplit(".", 1)` to split table from column — never `.split(".", 1)`, which would split at the catalog dot instead.

**Temp view resolution:** `_resolve_temp_views()` in `sql.py` short-circuits edges through `CREATE TEMP VIEW` — consumers of a temp view get edges directly from the temp view's sources. Chains of temp views are resolved in a single iterative pass.

**Table roles** (returned by `/tables`): `source` (only read), `target` (only written), `intermediate` (both), `result` (standalone SELECT with no INSERT INTO target).

---

## Frontend

### Commands

```bash
# From frontend/
npm run dev      # dev server on :3000
npm run build    # production build (run to verify TypeScript + compilation)
npm run lint     # ESLint
```

### Architecture

**API proxy:** `app/api/backend/[...path]/route.ts` — all `/api/backend/*` requests are forwarded server-side to the backend using the `API_URL` env var (set in Vercel; defaults to `http://localhost:8000`). Never use `NEXT_PUBLIC_` for the backend URL — it bakes at build time.

**API client:** `lib/api.ts` — typed fetch wrappers. `lib/hooks.ts` — React Query hooks (`useQuery` / `useMutation`) for all data fetching. Mutations invalidate relevant queries after success.

**Pages:** `sources` (upload/manage), `catalog` (browse tables grouped by role), `lineage` (graph + tree + code tabs), `impact` (downstream analysis).

**Lineage graph** (`components/lineage-graph.tsx`): Uses `@xyflow/react`. Nodes are positioned using topological depth (left-to-right: sources → intermediates → targets). Color coding: green = source node, cyan = selected column, purple = final target.

**Lineage tree** (`components/lineage-tree.tsx`): Upstream (←) and downstream (→) sections around the selected column. Uses `id.lastIndexOf(".")` to split table from column to handle multi-part names correctly.

---

## Deployment

- **Backend (Railway):** Auto-deploys from `master`. In-memory state — all uploaded data is lost on redeploy. Users must re-upload after each deployment.
- **Frontend (Vercel):** Auto-deploys from `master`. Set `API_URL=https://<railway-url>` in Vercel environment variables (without trailing slash).
