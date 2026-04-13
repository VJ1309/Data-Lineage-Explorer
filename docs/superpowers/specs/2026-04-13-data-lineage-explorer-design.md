# DataLineage Explorer — Design Spec

**Date:** 2026-04-13  
**Status:** Approved  

---

## Overview

DataLineage Explorer is a hosted web application that ingests Databricks notebooks, Python/PySpark scripts, and SQL files from multiple source types, parses them to extract expression-level column lineage, and presents that lineage through an interactive multi-view UI. It serves three personas — Data Engineers, Data Analysts, and Data Governance teams — from a single shared workspace with no authentication required.

---

## Goals

- Parse `.ipynb`, `.py`, and `.sql` files from Git repos, Databricks Workspace API, and local file uploads
- Extract column-level lineage at the expression level (aggregations, CTEs, window functions, CASE, JOINs, casts)
- Present lineage in three switchable views: graph, expandable tree, and code inspector
- Identify downstream impact when a source column changes
- Provide a searchable table/column catalog
- Deploy as a hosted web app (Vercel + Railway/Fly.io), accessible by the full team

---

## Non-Goals

- Authentication and per-user access control (single shared workspace)
- Persistent storage — source registrations and lineage graphs are held in a module-level in-memory store; all state is lost on server restart (re-adding sources and re-parsing is the recovery path)
- LLM-assisted transformation explanation
- Support for non-Databricks compute (Spark on EMR, Flink, etc.)
- dbt-native lineage (SQL files from dbt repos are supported as plain SQL, not via dbt manifest)

---

## Users

| Persona | Primary Need |
|---|---|
| Data Engineer | Trace how a column is derived through PySpark/SQL transformations; identify the exact notebook cell and line |
| Data Analyst | Understand what a column means, where it came from, and whether the source is trustworthy |
| Data Governance | Audit column usage, assess impact of schema changes, document ownership |

---

## Architecture

```
[Sources]                    [Backend]                        [Frontend]
Git Repo (GitHub/GitLab) ──► Ingestion Layer ──► Parser ──► FastAPI REST API ──► Next.js (Vercel)
Databricks Workspace API ──►   (normalise to      Layer       /sources
Local ZIP/folder upload ──►    FileRecord)        │           /tables
                                                  ▼           /lineage
                                             Lineage Engine   /impact
                                             (NetworkX DAG)   /search
```

**Backend:** FastAPI (Python), deployed on Railway or Fly.io as a Docker container.  
**Frontend:** Next.js 15 App Router, deployed on Vercel.  
**Communication:** JSON over HTTPS.

---

## Backend Design

### Ingestion Layer

Normalises all three source types into a flat list of `FileRecord` objects:

```python
@dataclass
class FileRecord:
    path: str          # relative path within the source
    content: str       # raw file content
    type: Literal["notebook", "python", "sql"]
    source_ref: str    # e.g. "github.com/org/repo" or "workspace.azuredatabricks.net"
```

- **Git:** Clone via GitPython to a temp directory; walk for `.ipynb`, `.py`, `.sql` files
- **Databricks API:** Use `databricks-sdk` to export notebooks as source; detect language per notebook
- **File upload:** Accept `.zip` via FastAPI `UploadFile`; extract to temp directory; walk files

### Parser Layer

Routed by `FileRecord.type`:

**Notebook parser (`nbformat`)**
1. Load notebook with `nbformat.read`
2. Iterate code cells; detect language via cell metadata or `%%sql` / `%sql` magic prefix
3. Route SQL cells to the SQL parser, Python cells to the PySpark parser
4. Attach `source_cell` (cell index) to all emitted nodes/edges

**PySpark parser (Python `ast`)**
1. Parse file to AST with `ast.parse`
2. Walk for DataFrame operations: `spark.read`, `.select()`, `.withColumn()`, `.groupBy().agg()`, `.join()`, `.filter()`, `.alias()`
3. Track variable reassignments to follow DataFrame lineage through chained operations
4. Resolve column expressions inside `.agg()`, `.withColumn()` to source columns
5. Attach `source_line` (AST node `lineno`) to all emitted nodes/edges

**SQL parser (SQLGlot)**
1. Parse with `sqlglot.parse_one(sql, dialect="databricks")`
2. Walk the AST: resolve CTEs and subquery aliases before column resolution
3. For each output column expression, identify all contributing input columns
4. Classify transform type: passthrough, aggregation (SUM/COUNT/AVG/etc.), expression (arithmetic/CASE), join_key, window (OVER), cast, filter (WHERE/HAVING)
5. Attach `source_line` where available

### Data Model

```python
@dataclass
class ColumnNode:
    id: str            # "{table}.{column}"
    table: str
    column: str
    dtype: str | None
    source_file: str
    source_cell: int | None   # notebook cell index
    source_line: int | None   # line number

@dataclass
class LineageEdge:
    source_col: str    # ColumnNode.id
    target_col: str    # ColumnNode.id
    transform_type: Literal[
        "passthrough", "aggregation", "expression",
        "join_key", "window", "cast", "filter"
    ]
    expression: str    # raw expression string e.g. "SUM(amount)"
    source_file: str
    source_cell: int | None
    source_line: int | None
```

### In-Memory State

The backend holds two module-level stores:
- **`source_registry: dict[str, SourceConfig]`** — registered sources (URL, token, type). Populated by `POST /sources`, cleared by `DELETE /sources/{id}`. Lost on server restart.
- **`lineage_graph: nx.DiGraph`** — the merged lineage DAG for all sources. Rebuilt from scratch on each `POST /sources/{id}/refresh`. Initially empty; populated on first refresh.

### Lineage Engine

Built on **NetworkX** `DiGraph`. Nodes are `ColumnNode.id` strings; edges carry `LineageEdge` data as attributes.

- **`build_graph(records: list[FileRecord]) -> nx.DiGraph`** — runs all parsers, merges output into a single DAG
- **`upstream(graph, col_id) -> list[LineageEdge]`** — BFS backwards; returns all ancestor edges
- **`downstream(graph, col_id) -> list[LineageEdge]`** — BFS forwards; returns all descendant edges
- **Cycle detection:** NetworkX `is_directed_acyclic_graph()` check; cycles are flagged as parse warnings, not errors

### REST API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/sources` | Register a new source (Git URL + token, Databricks host + token, or file upload) |
| `GET` | `/sources` | List registered sources |
| `DELETE` | `/sources/{id}` | Remove a source |
| `POST` | `/sources/{id}/refresh` | Re-parse a source |
| `GET` | `/tables` | List all discovered tables with column counts |
| `GET` | `/tables/{table}/columns` | List all columns for a table |
| `GET` | `/lineage` | `?table=&column=` — upstream lineage for a column |
| `GET` | `/impact` | `?table=&column=` — downstream impact for a column |
| `GET` | `/search` | `?q=` — search table and column names |
| `GET` | `/warnings` | List parse warnings from the last refresh |

Each lineage response includes:
```json
{
  "target": "agg_revenue.total_revenue",
  "upstream": [ ...LineageEdge ],
  "downstream": [ ...LineageEdge ],
  "graph": { "nodes": [...], "edges": [...] }
}
```

### Error Handling

- File parse failures are **non-fatal**: emit a `ParseWarning(file, error)` and continue
- Auth failures (Git token, Databricks token) surface immediately on source registration with a clear message
- Circular lineage is detected via NetworkX cycle check; involved nodes are flagged in warnings, not included in traversal
- All endpoints return structured error responses: `{ "error": "...", "detail": "..." }`

---

## Frontend Design

Built with **Next.js 15 App Router**, styled with **Tailwind CSS** and **shadcn/ui** components.

### Pages

**`/sources` — Source Manager**
- List connected sources (Git, Databricks, upload) with status indicator and last-parsed timestamp
- Refresh button per source triggers `POST /sources/{id}/refresh`
- "Add Source" panel with three tabs: Git repo URL + token, Databricks host + token, ZIP upload
- Parse warnings banner (count + link to `/warnings` detail page)

**`/catalog` — Table Catalog**
- Left sidebar: searchable list of all tables (calls `GET /tables`, filters client-side)
- Main panel: selected table's column list as a table with columns: name, type, source column, transform badge, "View Lineage →" link
- Transform badges colour-coded by type (aggregation=amber, passthrough=green, expression=purple, etc.)
- Clicking "View Lineage →" navigates to `/lineage?table=&column=`

**`/lineage` — Lineage Explorer**
Three views, switchable via tabs, all showing the same underlying lineage data:

1. **Graph view** — React Flow (`@xyflow/react`): tables as grouped nodes, columns as child nodes, edges labelled with transform type and expression. Pan/zoom. Clicking a node highlights its direct edges.
2. **Tree view** — collapsible tree component: target column at root, upstream columns as children, recursively expandable. Each node shows file + cell/line reference.
3. **Code view** — split panel: left shows column list; right shows the raw source expression with syntax highlighting (`react-syntax-highlighter`) and file/cell/line metadata.

**`/impact` — Impact Analyzer**
- Input: select table + column (or arrive via "View Impact" link from catalog)
- Output: cascading indented list of all downstream dependents, with transform type, expression, and file + line reference
- Warning banner for the number of affected columns

### Data Fetching

- **TanStack Query** for all API calls: caching, loading states, error states
- Lineage graph data fetched on demand per column (not pre-loaded for all columns)
- Search is client-side over the cached table list for instant results

---

## Deployment

| Component | Platform | Notes |
|---|---|---|
| Frontend | Vercel | Auto-deploy from `main` branch |
| Backend | Railway | Dockerised FastAPI; `BACKEND_URL` env var set in Vercel |
| Secrets | Vercel env vars + Railway env vars | Git tokens, Databricks tokens stored server-side, never in frontend |

`.gitignore` additions: `.superpowers/`, `tmp/`, `.env`

---

## Project Structure

```
/
├── backend/
│   ├── main.py                  # FastAPI app entry point
│   ├── ingestion/
│   │   ├── git.py               # GitPython ingestion
│   │   ├── databricks.py        # databricks-sdk ingestion
│   │   └── upload.py            # ZIP upload handler
│   ├── parsers/
│   │   ├── notebook.py          # nbformat parser
│   │   ├── pyspark.py           # ast-based PySpark parser
│   │   └── sql.py               # SQLGlot parser
│   ├── lineage/
│   │   ├── engine.py            # NetworkX graph builder + traversal
│   │   └── models.py            # ColumnNode, LineageEdge dataclasses
│   └── api/
│       └── routes.py            # All FastAPI route handlers
├── frontend/
│   ├── app/
│   │   ├── sources/page.tsx
│   │   ├── catalog/page.tsx
│   │   ├── lineage/page.tsx
│   │   └── impact/page.tsx
│   ├── components/
│   │   ├── lineage-graph.tsx    # React Flow wrapper
│   │   ├── lineage-tree.tsx     # Collapsible tree
│   │   └── code-inspector.tsx  # Split panel + syntax highlight
│   └── lib/
│       └── api.ts               # TanStack Query hooks
├── Dockerfile                   # Backend container
└── docs/
    └── superpowers/specs/
        └── 2026-04-13-data-lineage-explorer-design.md
```

---

## Tech Stack Summary

| Layer | Technology |
|---|---|
| Backend framework | FastAPI + Uvicorn |
| SQL parsing | SQLGlot (`dialect="databricks"`) |
| PySpark parsing | Python `ast` stdlib |
| Notebook parsing | `nbformat` |
| Graph traversal | NetworkX |
| Git ingestion | GitPython |
| Databricks ingestion | `databricks-sdk` |
| Frontend framework | Next.js 15 (App Router) |
| Graph visualisation | React Flow (`@xyflow/react`) |
| Styling | Tailwind CSS + shadcn/ui |
| Data fetching | TanStack Query |
| Code display | `react-syntax-highlighter` |
| Backend deployment | Railway (Docker) |
| Frontend deployment | Vercel |
