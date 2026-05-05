# Databricks SQL Scripting Coverage — Requirements

**Status:** Draft, ready for `/ce-plan`
**Date:** 2026-05-04
**Driver:** Real customer files combined with a forward-looking audit. The current parser is precise on the imperative DML surface (SELECT/CTE/INSERT/CTAS/MERGE/COPY/CLONE/temp views/PIVOT/LATERAL VIEW) but blind to the entire procedural surface that Databricks Runtime 16.3+/17.0+ shipped (SQL scripting GA, stored procedures preview, recursive CTEs GA).
**Source spec:** `sql-scripting.md` at repo root.

---

## Problem

When a Databricks `.sql` file (or notebook cell) contains any of the following, lineage extraction produces **zero edges** and emits a parse warning:

- `BEGIN … END` compound blocks
- `BEGIN ATOMIC … END` blocks
- `CREATE [OR REPLACE] PROCEDURE … AS BEGIN … END`
- Any `IF` / `WHILE` / `FOR` / `LOOP` / `REPEAT` / `CASE`-statement
- `DECLARE` / `SET` (local variables)
- `EXECUTE IMMEDIATE` (dynamic SQL — the dominant Databricks ETL idiom for parameterised pipelines)
- `SIGNAL` / `RESIGNAL` / `HANDLER` declarations
- `CALL proc(...)` (parses to `exp.Command`; `_parse_command_fallback` only recognises `DEEP CLONE`)

`WITH RECURSIVE` parses successfully but the recursive branch's reference to the CTE name is treated as a literal table, producing a **phantom self-edge** (`org_tree.id → org_tree.id`-style noise) and missing the correct anchor → source column lineage.

A non-empty body of customer SQL exists today that hits these patterns and shows up as gaps in the graph. New customer ETL written against Runtime 17.0+ will increasingly use procedures and recursive CTEs.

## Goal

Make `parsers/sql.py` complete and accurate over the procedural Databricks SQL surface defined in `sql-scripting.md`, at a **flat data-flow projection** fidelity level: every embedded DML statement inside a procedural wrapper produces the same lineage edges it would produce as a top-level statement, and `WITH RECURSIVE` produces correct anchor → source lineage with no phantom self-edges.

## Non-goals

- **Branch-aware lineage.** Edges produced inside `IF` / `CASE` / loop bodies are emitted unconditionally. No `confidence="conditional"` flag, no per-branch sub-graphs.
- **Full SQL/PSM interpreter semantics.** No variable type inference, no loop fixed-point analysis, no static reachability / dead-code detection, no exception flow modelling.
- **Cross-file `CALL` resolution.** `CALL proc(args)` emits an approximate wildcard edge in v1. Linking the call site to the procedure body's edges (when both live in the same upload bundle) is deferred.
- **Multi-statement transactions outside SQL files.** The Python connector `autocommit = False` API is a runtime concern, not a parser one. Out of scope.
- **Procedure metadata extraction.** `DROP / DESCRIBE / SHOW PROCEDURE` are recognised and silently skipped — they carry no lineage.

## Users / consumers

- Backend lineage engine (`lineage/engine.py:_parse_file`) — invokes `parsers.sql.parse_sql` on every `.sql` file and per cell of `.sql` Databricks notebooks.
- Frontend `lineage` / `impact` / `catalog` pages — consume edges via existing API; no frontend changes required.
- The `/warnings` endpoint — should see a meaningful drop in "Invalid expression / Unexpected token" warnings on procedural files.

## Scope — patterns that must produce correct lineage

Mapped 1:1 against `sql-scripting.md`. Each row is the precision/completeness contract for that pattern.

### Compound blocks and control flow

| Pattern | Behaviour |
|---|---|
| `BEGIN … END` | Walk the body. Every nested DML / CTAS / temp-view / EXECUTE IMMEDIATE produces edges identical to the same statement at top level. |
| `BEGIN ATOMIC … END` | Same as `BEGIN … END`. The `ATOMIC` keyword is recognised and skipped. |
| Nested `BEGIN … END` | Walked recursively. |
| Labels (`label: BEGIN`, `label: WHILE`, etc.) | Recognised and skipped. |
| `IF / ELSEIF / ELSE / END IF` | Walk every branch unconditionally; emit DML from all branches. |
| `CASE` (statement form) / `END CASE` | Walk every `WHEN` and `ELSE` branch unconditionally. |
| `WHILE … DO / END WHILE` | Walk the loop body once. |
| `FOR variable_name AS query DO / END FOR` | The cursor query becomes a synthetic virtual source named `__for_<label-or-var>__`. References to `variable_name.col` inside the body resolve to `__for_*.col`. DML inside the body produces edges using that virtual source. |
| `FOR query DO` (no variable name) | Cursor query parsed; body DML walked, but no variable rewrite (cursor only contributes if the body uses an unqualified column matching a cursor output). |
| `LOOP / END LOOP` | Walk body once. |
| `REPEAT / UNTIL cond / END REPEAT` | Walk body once. |
| `LEAVE label`, `ITERATE label` | Recognised and skipped (no data flow). |
| `DECLARE name [, ...] type [DEFAULT expr]` | Recognised. Bind `name → literal` when DEFAULT is a constant expression (string literal, number, `NULL`, simple `||` chain) — only used by EXECUTE IMMEDIATE folding. Otherwise skipped. |
| `DECLARE name CONDITION [FOR SQLSTATE 'xxxxx']` | Recognised and skipped. |
| `DECLARE handler_type HANDLER FOR … action` | Walk the handler action body for embedded DML, then skip. |
| `SET name = expr` / `SET VAR name = expr` / `SET (a,b) = (e1,e2)` | If `expr` is a constant, update the variable binding. Otherwise skipped. |
| `SIGNAL` / `RESIGNAL` | Recognised and skipped (no data flow). |

### Stored procedures

| Pattern | Behaviour |
|---|---|
| `CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] name(params) characteristics AS BEGIN … END` | Strip the wrapper (parameter list + characteristics). Parse the body as if it were a top-level script. The procedure name is recorded in a module-level registry keyed by qualified name (catalog.schema.proc) for v2 cross-file resolution. |
| Parameter modes `IN` / `OUT` / `INOUT` | Recognised in the parameter list and skipped (no lineage in v1). |
| `CALL name(args)` / `CALL name(named => arg)` | Emits a single approximate-confidence wildcard edge `__call_<proc-name>__.* → __call_<proc-name>__.*` plus a parse-warning-style note that the call site couldn't resolve a body. (Wildcard placeholder so the call appears in the graph without contaminating real tables.) |
| `DROP / DESCRIBE / SHOW PROCEDURE` | Recognised and skipped silently. |

### EXECUTE IMMEDIATE

| Pattern | Behaviour |
|---|---|
| `EXECUTE IMMEDIATE 'literal SQL'` | Inner SQL re-parsed via the normal pipeline. |
| `EXECUTE IMMEDIATE 'pre ' \|\| var \|\| ' post'` | Constant-fold the `\|\|` chain using the in-scope variable bindings (literal DEFAULTs from `DECLARE`, constant `SET` updates). If fully foldable, re-parse the resulting SQL string. |
| `EXECUTE IMMEDIATE … USING arg1 [, ...]` / `… INTO var [, ...]` | Bindings are dropped in v1 (placeholders not substituted). The static SQL skeleton is parsed; the parameter values do not affect lineage. |
| Non-foldable `sql_string` | Emit one approximate wildcard edge `__dynamic_sql__.* → __dynamic_sql__.*` and a per-statement warning so the user knows lineage is incomplete at this site. |

### Recursive CTEs

| Pattern | Behaviour |
|---|---|
| `WITH RECURSIVE cte (cols) [MAX RECURSION LEVEL n] AS (anchor UNION ALL recursive)` | The anchor is parsed normally and contributes its source-column → cte-column edges. The recursive branch's references to the CTE name are resolved to the anchor's source columns (not the CTE alias itself). No phantom `cte.col → cte.col` self-edges. |
| `MAX RECURSION LEVEL n` | Recognised and skipped (no lineage impact). |
| `LIMIT ALL` (Runtime 17.2+) | Recognised; passes through to SQLGlot. |

## Success criteria

1. **Behavioural parity for embedded DML.** For any test fixture containing a `BEGIN … END` or `CREATE PROCEDURE … AS BEGIN … END` wrapper around top-level DML, the produced edges (after temp-view resolution) equal the edges produced for the same DML at top level. Verified by a parameterised test pair: `<sql>_top_level.sql` vs. `<sql>_in_block.sql`.
2. **Recursive CTE precision.** A representative fixture (org-tree traversal, BOM explosion, number series, graph cycle detection from `sql-scripting.md`) produces edges only between the recursive CTE's downstream consumer and the anchor's source columns. Zero edges have `source_col` and `target_col` pointing at the same CTE alias.
3. **Dynamic SQL recovery.** A fixture replicating the doc's `EXECUTE IMMEDIATE 'TRUNCATE TABLE ' \|\| table_name` plus the matching `INSERT INTO ' \|\| table_name \|\| ' SELECT * FROM source` produces real edges to/from the resolved `table_name` value when DECLARE provides a literal default.
4. **Warning reduction, not just suppression.** Files that previously failed wholesale with a single "Unexpected token" warning now produce edges, and the warning is downgraded to a per-statement warning *only* for the statements the parser genuinely couldn't handle (e.g., non-foldable EXECUTE IMMEDIATE).
5. **No regression.** All existing tests in `backend/tests/test_sql_parser.py`, `test_engine.py`, and `test_routes.py` pass unchanged.
6. **No phantom nodes.** Synthetic source/target table names introduced by this change (`__for_*__`, `__call_*__`, `__dynamic_sql__`) are visible in `/tables` only when they actually carry an edge, and are tagged so the frontend can render them distinctly if desired (deferred to a frontend follow-up).

## Out-of-scope (deferred for later)

- Cross-file `CALL` resolution (linking a `CALL` site to its `CREATE PROCEDURE` body within the same upload bundle).
- Branch-aware confidence flags or per-branch sub-graphs.
- Variable tracking beyond literal `DECLARE` defaults / constant `SET` updates (no expression evaluation, no `INTO var` value capture).
- Modelling the difference between `WriteSerializable` and `Serializable` isolation, or any concurrency / commit semantics.
- A `/procedures` API endpoint or UI surface for inspecting parsed procedure bodies independently of their file.
- Frontend rendering treatment for synthetic `__for_*__` / `__call_*__` / `__dynamic_sql__` nodes.

## Outside this product's identity

- This is a **lineage parser**, not a **SQL/PSM static analyser**. Reachability, type checking, exception flow, and dead-code detection belong in a different product if they're ever wanted.
- We do not aspire to faithfully evaluate procedural semantics. A `WHILE i < N DO INSERT …` produces the same edges whether the loop runs once or a thousand times — that's by design.

## Dependencies / assumptions

- SQLGlot version pinned in `backend/uv.lock` will continue to fail-fast on procedural syntax. The pre-processor approach insulates us from this — it parses the *body's DML*, which SQLGlot supports.
- `WITH RECURSIVE` is parsed correctly by SQLGlot today (verified). The fix lives in `_resolve_ctes` in `parsers/sql.py`, not in SQLGlot.
- Customer files do not nest stored procedures inside other procedures (Databricks itself prohibits this in the current preview). If they do, v1 parses the outermost body only.
- `EXECUTE IMMEDIATE` strings, when foldable, are syntactically valid Databricks SQL after folding. Non-foldable strings degrade gracefully to an approximate edge.

## Risks

- **Block-delimiter tokenisation bugs.** The pre-processor must correctly pair `BEGIN`/`END`, `IF`/`END IF`, etc. across nested blocks, comments, and string literals. A bug here silently drops or duplicates statements. Mitigation: drive the tokeniser off `sqlglot.tokens.Tokenizer` (already used by `_split_top_level_statements`) so string/comment handling is shared, plus aggressive fixture coverage.
- **`FOR row AS SELECT … DO` cursor aliasing.** Subtle: nested DML may reference `row.col` (qualified) or just `col` (unqualified, falling through to lateral view-style resolution). v1 handles only qualified references; unqualified column drops the cursor link. Documented limitation; revisit if customers hit it.
- **EXECUTE IMMEDIATE folding scope creep.** Resist the urge to evaluate non-trivial expressions. Constant-fold means: string literals, numeric literals, `NULL`, identifier-bound constants. Anything else degrades to wildcard.
- **Existing `_parse_command_fallback` regex hooks.** The DEEP CLONE regex is the only existing case. Adding scripting-aware fallbacks here would hide the new pre-processor behind a fragile regex layer. Keep the new logic in a dedicated module; do not extend `_parse_command_fallback`.

## Implementation outline (for `/ce-plan` to deepen)

The detailed implementation plan belongs in a follow-up `/ce-plan` document. The shape is:

1. New module `backend/parsers/sql_script.py` exposing a single function `normalize_script(sql: str) -> tuple[str, list[VirtualSource], dict[str, str]]` returning (a) flattened SQL safe to feed `_split_top_level_statements`, (b) virtual-source declarations for `FOR` cursors, (c) a variable-binding map for EXECUTE IMMEDIATE folding.
2. `parsers/sql.py:parse_sql` calls `normalize_script` first when the input contains procedural keywords (cheap pre-check); otherwise unchanged.
3. `_resolve_ctes` gains a recursive-CTE branch: when a CTE body is a UNION over (anchor, recursive) and the recursive side joins the CTE alias, the CTE alias is resolved to the anchor's source tables before parsing the recursive branch.
4. New test files: `tests/test_sql_script_normalize.py` (block tokeniser, cursor rewriting, EXECUTE IMMEDIATE folding) and additional cases in `tests/test_sql_parser.py` keyed to each row of the **Scope** table above.
5. Existing tests must pass unchanged.

---

## Open questions

None blocking. The fidelity decision (flat data-flow projection + `EXECUTE IMMEDIATE` constant folding) is settled. Implementation strategy (pre-processor over local SQLGlot fork) is settled. `/ce-plan` should produce the file-level diff plan and a phased implementation order — likely (recursive CTE fix → compound blocks + control flow → procedures → EXECUTE IMMEDIATE → CALL placeholder).
