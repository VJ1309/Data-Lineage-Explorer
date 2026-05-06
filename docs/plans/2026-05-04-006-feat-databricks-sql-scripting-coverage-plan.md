---
title: "feat: Databricks SQL scripting coverage in parsers/sql.py"
type: feat
status: completed
date: 2026-05-04
origin: docs/brainstorms/2026-05-04-databricks-sql-scripting-coverage-requirements.md
---

# feat: Databricks SQL scripting coverage in parsers/sql.py

## Overview

Extend the Databricks SQL parser so it produces correct column-level lineage for the procedural surface that Databricks Runtime 16.3+/17.0+ shipped: compound blocks (`BEGIN…END`, `BEGIN ATOMIC`), control flow (`IF`/`CASE`-statement/`WHILE`/`FOR`/`LOOP`/`REPEAT`), stored procedures (`CREATE PROCEDURE`/`CALL`), `EXECUTE IMMEDIATE` dynamic SQL, and recursive CTEs.

The fidelity target is **flat data-flow projection**: every embedded DML statement produces the same edges it would as a top-level statement. Branch-aware lineage and full SQL/PSM semantics are explicit non-goals (see origin: `docs/brainstorms/2026-05-04-databricks-sql-scripting-coverage-requirements.md`).

The strategy is a **pre-processor module** (`parsers/sql_script.py`) that strips procedural wrappers and rewrites cursor references *before* SQLGlot sees the SQL. This insulates us from SQLGlot's lack of SQL/PSM grammar and lets us continue parsing only DML — which SQLGlot already supports robustly. A small surgical fix in `_resolve_ctes` handles `WITH RECURSIVE` precision separately.

---

## Problem Frame

Today, when a `.sql` file (or notebook cell) contains `BEGIN`, `IF`, `CREATE PROCEDURE`, `EXECUTE IMMEDIATE`, `CALL`, or any other procedural construct, `sqlglot.parse_one(..., dialect="databricks")` raises and the entire file produces **zero edges** plus a single "Unexpected token" warning. `WITH RECURSIVE` parses but the recursive branch's reference to the CTE name is treated as a literal table, producing phantom self-edges (`org_tree.id → org_tree.id`) and missing the correct anchor → source column lineage.

Real customer ETL is starting to land on Runtime 17.x and uses these features (procedures driving parameterised pipelines, dynamic `TRUNCATE`/`INSERT` chains, recursive org-tree traversal). The graph silently misses them today.

---

## Requirements Trace

Mapped from the origin doc's `Scope` tables and `Success criteria`. R1–R6 are end-to-end behavioural contracts (success criteria); R7–R13 are pattern-coverage requirements derived from the scope tables.

- R1. **Behavioural parity for embedded DML.** Edges from any DML inside a `BEGIN…END` / procedure body / control-flow body equal the edges the same DML produces at top level.
- R2. **Recursive CTE precision.** A `WITH RECURSIVE` produces edges only between the consumer and the anchor's source columns. Zero `cte.col → cte.col` self-edges.
- R3. **Dynamic SQL recovery.** Foldable `EXECUTE IMMEDIATE 'op ' || var || ' …'` (where `var` has a literal `DECLARE` default or constant `SET`) re-parses to real edges against the resolved table.
- R4. **Warning fidelity.** Procedural files produce per-statement warnings only for statements the parser genuinely cannot handle, not a wholesale "Unexpected token" failure for the whole file.
- R5. **No regression.** All existing tests in `backend/tests/test_sql_parser.py`, `test_engine.py`, and `test_routes.py` pass unchanged.
- R6. **No phantom nodes leak as real tables.** Synthetic names (`__for_*__`, `__call_*__`, `__dynamic_sql__`) only appear in `/tables` when they carry an edge, and are flagged so the frontend can render them distinctly later (frontend rendering is deferred — see Scope Boundaries).
- R7. **Compound blocks.** `BEGIN`/`BEGIN ATOMIC`/labelled `BEGIN`/nested `BEGIN…END` walked recursively; embedded DML hoisted unchanged.
- R8. **Statement-form control flow.** `IF`/`ELSEIF`/`ELSE`/`END IF`, statement-form `CASE`/`END CASE`, `WHILE`/`END WHILE`, `LOOP`/`END LOOP`, `REPEAT`/`UNTIL`/`END REPEAT`: every branch / loop body walked unconditionally.
- R9. **FOR cursor lineage.** `FOR variable_name AS query DO … END FOR` — cursor query becomes a synthetic virtual source `__for_<label-or-var>__`; qualified `variable_name.col` references inside the body resolve to that virtual source.
- R10. **Variable bindings for fold.** `DECLARE name [type] DEFAULT literal` and constant-RHS `SET`/`SET VAR`/`SET (a,b)=(e1,e2)` populate a binding map used only by EXECUTE IMMEDIATE folding. Non-constant RHS skipped.
- R11. **Stored procedures.** `CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] name(params) characteristics AS BEGIN … END`: parameter list + characteristics stripped, body parsed as a top-level script, qualified procedure name registered in a module-level registry for v2 cross-file resolution. `IN`/`OUT`/`INOUT` modes recognised and skipped. `DROP`/`DESCRIBE`/`SHOW PROCEDURE` recognised and silently skipped.
- R12. **EXECUTE IMMEDIATE.** Constant string literal → re-parse. `||` chain over bound literals → constant-fold then re-parse. `USING`/`INTO` clauses dropped in v1. Non-foldable expression → emit one approximate `__dynamic_sql__.* → __dynamic_sql__.*` wildcard edge plus per-statement warning.
- R13. **CALL placeholder.** `CALL name(args)` / `CALL name(named => arg)` emits one approximate wildcard edge `__call_<proc-name>__.* → __call_<proc-name>__.*` plus a per-statement note. (Cross-file resolution deferred.)

**Origin actors:** *(not formally enumerated in the requirements doc — origin uses "Users / consumers")*
- Backend lineage engine (`backend/lineage/engine.py:_parse_file`)
- Frontend `lineage` / `impact` / `catalog` pages (no frontend changes required)
- `/warnings` endpoint (sees a meaningful drop in "Unexpected token" warnings on procedural files)

---

## Scope Boundaries

### Deferred for later
*(Carried verbatim from origin — product/version sequencing.)*

- Cross-file `CALL` resolution (linking a `CALL` site to its `CREATE PROCEDURE` body within the same upload bundle).
- Branch-aware confidence flags or per-branch sub-graphs.
- Variable tracking beyond literal `DECLARE` defaults / constant `SET` updates (no expression evaluation, no `INTO var` value capture).
- Modelling `WriteSerializable` vs. `Serializable` isolation, or any concurrency / commit semantics.
- A `/procedures` API endpoint or UI surface for inspecting parsed procedure bodies independently of their file.
- Frontend rendering treatment for synthetic `__for_*__` / `__call_*__` / `__dynamic_sql__` nodes (backend tagging lands in this plan, frontend rendering follows separately).

### Outside this product's identity
*(Carried verbatim from origin — positioning rejection.)*

- This is a **lineage parser**, not a **SQL/PSM static analyser**. Reachability, type checking, exception flow, and dead-code detection belong in a different product if they're ever wanted.
- We do not aspire to faithfully evaluate procedural semantics. A `WHILE i < N DO INSERT …` produces the same edges whether the loop runs once or a thousand times — that is by design.

### Deferred to Follow-Up Work
*(Plan-local — implementation work intentionally split across other PRs.)*

- Frontend distinct rendering for synthetic `__for_*__` / `__call_*__` / `__dynamic_sql__` nodes — separate frontend PR once this backend plan ships and the synthetic tag is observable in `/tables`.

---

## Context & Research

### Relevant Code and Patterns

- `backend/parsers/sql.py:_split_top_level_statements` — already drives off `sqlglot.tokens.Tokenizer`, sharing string/comment handling. The new block tokeniser must follow the same pattern (origin risk: "drive the tokeniser off `sqlglot.tokens.Tokenizer`").
- `backend/parsers/sql.py:_resolve_ctes` — current CTE classification (simple_map vs. multi_map). The recursive-CTE fix lives here.
- `backend/parsers/sql.py:_wildcard_edge` — factory for approximate edges. Reused by CALL placeholder and dynamic-SQL placeholder.
- `backend/parsers/sql.py:_parse_command_fallback` — explicitly off-limits per origin risk: "do not extend `_parse_command_fallback`". The new logic lives in a dedicated module.
- `backend/parsers/sql.py:parse_sql` — public entry point. Calls `normalize_script` first when procedural keywords are present, otherwise unchanged.
- `backend/lineage/engine.py:_parse_file` — owns format dispatch (Databricks notebook split, `_resolve_views=False`). Does **not** change in this plan; the new pre-processor lives below `parse_sql`.
- `backend/lineage/models.py:LineageEdge`, `ParseResult` — return types are unchanged. New synthetic edges fit the existing schema.
- `backend/lineage/ids.py:split_column_id` — used by route-layer table classification. Synthetic-node tagging surfaces through the table response.

### Institutional Learnings

- `docs/solutions/best-practices/databricks-sql-parser-extensions-2026-04-25.md` — coverage patterns and SQLGlot pitfalls. Key reusable: "silently-dropped edges are the worst failure mode" (motivates the per-statement warning + approximate-edge fallback for non-foldable EXECUTE IMMEDIATE and unresolved CALL).
- `docs/solutions/best-practices/sql-parser-dry-refactoring-patterns-2026-04-27.md` — three placement levels (module helper, inner closure, factory). The block tokeniser is module-level; per-scope variable-binding rewrites are inner closures.
- `docs/solutions/architecture-patterns/backend-parser-state-refactor-patterns-2026-05-01.md` — `parse_sql` must continue to return `ParseResult`; engine owns format dispatch. Do not detect notebook separators or recurse from inside `parse_sql`.
- `backend/AGENTS.md` — SQLGlot ≥ 25 with `dialect="databricks"` mandatory; expression classes from `sqlglot.expressions` only; no new state globals (procedure registry is module-level in `sql_script.py`, not in `state.py`).

### External References

External research skipped: the origin doc settled the implementation strategy (pre-processor over local SQLGlot fork) and the codebase has strong local patterns for predicate edges, wildcard edges, and CTE resolution.

---

## Key Technical Decisions

- **Pre-processor over SQLGlot fork.** A separate normalisation pass strips procedural wrappers before SQLGlot parses. *Rationale:* SQLGlot's Databricks dialect doesn't model SQL/PSM; forking would create maintenance debt and lock us to a specific SQLGlot version. The pre-processor only depends on `sqlglot.tokens.Tokenizer` (a stable surface).
- **Reuse `sqlglot.tokens.Tokenizer` for block tokenisation.** Strings, comments, and identifier escaping are already correctly handled. *Rationale:* writing a Databricks-grade tokeniser from scratch is the dominant risk vector for silent edge drops (origin risk).
- **Recursive CTE fix lives in `_resolve_ctes`, not in the normaliser.** `WITH RECURSIVE` is structurally a CTE — it parses successfully today; the bug is in alias resolution. Pulling it into the normaliser would mix concerns. *Rationale:* surgical fix in the right module.
- **Synthetic prefix convention: `__for_<id>__`, `__call_<proc>__`, `__dynamic_sql__`.** All synthetic table names use the existing `__name__` convention already used by `__sub_N__` and pseudo-columns (`__filter__`, `__joinkey__`). *Rationale:* consistent with the codebase, easy for the route layer to detect with a single prefix check.
- **Drop EXECUTE IMMEDIATE `USING`/`INTO` in v1.** Placeholders are not substituted; the static skeleton is parsed. *Rationale:* origin defers parameter substitution; static skeleton catches the table-name folding case that matters for ETL.
- **Procedure registry is module-level in `sql_script.py`, not `state.py`.** Single-process in-memory registry keyed by qualified name. *Rationale:* `state.py` is reserved for source-registry / graph state per `backend/AGENTS.md`; the procedure registry is a pre-processor concern that survives across `parse_sql` calls within one upload-refresh cycle.
- **Per-statement warnings, not file-level.** Each unparseable embedded statement contributes its own warning string to `ParseResult.warnings`. *Rationale:* the engine already wraps each warning with the file path; the granularity matches what `/warnings` consumers expect.

---

## Open Questions

### Resolved During Planning

- *Should the block tokeniser preserve original line numbers for each hoisted statement?* **Yes** — preserve to the extent the origin doc's structure allows. The normaliser emits each hoisted DML statement with its starting offset in the original SQL; `parse_sql` can use that for `source_line`. When the offset is unknown (e.g., a folded EXECUTE IMMEDIATE), pass through the wrapper's outermost line.
- *Should `CREATE PROCEDURE` register the procedure body in a registry even though v1 has no cross-file resolution?* **Yes** — register by qualified name. *Rationale:* origin defers cross-file resolution to v2 but explicitly asks the registry to be in place. Cheap to populate and unblocks v2 with no migration.
- *Where does the module-level procedure registry live?* In `parsers/sql_script.py` as a module global, mirroring how `parsers/sql.py` already keeps `_subquery_counter` at module level. *Rationale:* it is normaliser state, not engine state; lives next to the code that mutates it.
- *Should `BEGIN ATOMIC` produce a different edge than `BEGIN`?* **No**. The `ATOMIC` keyword is a transactional contract, not a lineage one. Recognise and skip.

### Deferred to Implementation

- Exact SQLGlot token names for procedural keywords (`BEGIN`, `END`, `IF`, etc.). Resolve when implementing U2 by introspecting `sqlglot.tokens.TokenType` against the installed version per `backend/AGENTS.md`'s "verify against the installed version" rule.
- Whether `WITH RECURSIVE` produces `exp.Union` or `exp.UnionAll` for the anchor/recursive branches in the installed SQLGlot version. Verify with a probe in U1.
- Final test fixture corpus for "before/after" parity (the `<sql>_top_level.sql` vs. `<sql>_in_block.sql` pairs from origin SC1). Decide which constructs warrant fixture pairs vs. inline-string tests when writing U2/U5 tests.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

### Pipeline shape

```
parse_sql(sql)
   │
   ├─ if procedural-keyword pre-check:
   │     normalize_script(sql)
   │       returns: (flattened_sql,
   │                 list[VirtualSource],
   │                 dict[var_name → literal])
   │
   ├─ _split_top_level_statements(flattened_sql)
   │
   ├─ for each statement:
   │     sqlglot.parse_one(stmt, dialect="databricks")
   │     _parse_single_statement(...)        # existing path
   │       └─ for synthetic CALL / dynamic placeholder:
   │             _wildcard_edge(...)
   │
   └─ ParseResult(edges, raw_edges, warnings)
```

### Block-tokeniser sketch

```
tokens = sqlglot.tokens.Tokenizer().tokenize(sql)
walk(tokens):
   on CREATE PROCEDURE name(params) characteristics AS BEGIN…END:
     register(name, body); emit body  (recursive walk)
   on BEGIN [ATOMIC] [label]:
     enter scope; walk body; on END: exit scope
   on DECLARE name [type] DEFAULT literal:
     bind(scope, name, literal)
   on DECLARE name CONDITION ...:           skip
   on DECLARE handler HANDLER FOR ... action:
     walk action  (it can contain DML)
     mark skipped
   on SET name = literal:                    bind(scope, name, literal)
   on SET name = non-literal:                clear binding (defensive)
   on IF cond / ELSEIF / ELSE:               walk every branch
   on CASE …WHEN…ELSE…END CASE (statement):  walk every branch
   on WHILE cond DO / END WHILE:             walk body once
   on FOR var AS query DO / END FOR:
     emit virtual source __for_<label_or_var>__
     map var.col → __for_<label_or_var>__.col inside body
     hoist cursor query
     walk body once
   on LOOP / REPEAT:                         walk body once
   on LEAVE / ITERATE / SIGNAL / RESIGNAL:   skip (no data flow)
   on EXECUTE IMMEDIATE expr [USING…] [INTO…]:
     if expr is constant string OR foldable ‖ chain: re-parse inner SQL inline
     else: emit placeholder DML  ( __dynamic_sql__.* → __dynamic_sql__.* )
   on CALL proc(args):
     emit placeholder DML  ( __call_<proc>__.* → __call_<proc>__.* )
   on DROP/DESCRIBE/SHOW PROCEDURE:          skip silently
```

### Recursive CTE resolution

For `WITH RECURSIVE cte AS (anchor UNION ALL recursive)`:
1. Detect: a CTE whose body is a `Union`/`UnionAll` and whose recursive branch references the CTE alias as a table.
2. Resolve the anchor branch normally — it does not reference the CTE; produces real source-column → CTE-column edges.
3. When parsing the recursive branch, populate `simple_map[cte_alias] → anchor_qualified_source` (or, when the anchor has a JOIN, register the anchor as a virtual subquery so resolve_temp_views chains it).
4. The recursive branch's `JOIN cte ON …` join keys and any column references resolve to the anchor's source — never to `cte_alias` itself.

---

## Output Structure

    backend/
    ├─ parsers/
    │  ├─ sql.py                       (modified: _resolve_ctes recursive branch; parse_sql pre-check)
    │  └─ sql_script.py                (NEW)
    └─ tests/
       ├─ test_sql_parser.py           (modified: scripting / recursive CTE / EXECUTE IMMEDIATE / CALL cases)
       └─ test_sql_script_normalize.py (NEW)

---

## Implementation Units

- U1. **Recursive CTE precision in `_resolve_ctes`**

**Goal:** Eliminate phantom `cte.col → cte.col` self-edges on `WITH RECURSIVE` and produce correct anchor → consumer edges.

**Requirements:** R2, R5

**Dependencies:** None — independent of the normaliser; can land first.

**Files:**
- Modify: `backend/parsers/sql.py` (extend `_resolve_ctes`)
- Modify: `backend/tests/test_sql_parser.py` (add recursive CTE cases)

**Approach:**
- In `_resolve_ctes`, detect a recursive CTE: `with_clause.args.get("recursive")` is true, OR (defensive) the CTE body is a Union/UnionAll and one of its Select branches has a `Table` reference whose `.name` equals the CTE alias.
- Split anchor (does not reference the CTE alias) from recursive branch (does).
- When the anchor is single-FROM single-table: populate `simple_map[cte_alias] → anchor_qualified_source` so the existing chain-resolution loop (lines 113–122) collapses recursive references to the anchor's underlying table.
- When the anchor is multi-source (JOIN, UNION over multiple anchors): collect anchor `Select` nodes into `multi_map[cte_alias]` exactly like a non-recursive multi-source CTE. The recursive branch's references to `cte_alias` then resolve to the anchor subquery alias rather than themselves.
- Skip `MAX RECURSION LEVEL n` and `LIMIT ALL` — pass through to SQLGlot, which already parses them.
- Do not parse the recursive branch as a fresh CTE Select; its column outputs are already represented by the anchor branch (UNION ALL projects the same column shape). Emitting edges from the recursive body would double-count source columns.

**Patterns to follow:**
- `_resolve_ctes` chain-resolution loop (lines 113–122 of current `parsers/sql.py`).
- `_collect_union_selects` for fanning out the Union body.

**Test scenarios:**
- *Happy path — anchor with literal seed (number-series fixture from origin SC2).* `WITH RECURSIVE numbers(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM numbers WHERE n < 100) SELECT * FROM numbers` — VALUES is sourceless. This case asserts zero phantom `numbers.n → numbers.n` self-edges; downstream consumer traces to the literal seed (no real source table).
- *Happy path — anchor is single-table (org-tree fixture from origin SC2).* From `sql-scripting.md`: anchor `SELECT employee_id, name, manager_id, name AS root_name, 0 AS depth FROM employees WHERE manager_id IS NULL` and recursive `JOIN org_tree t ON e.manager_id = t.employee_id`. Asserts: `org_tree.root_name`'s downstream consumer traces to `employees.name`, **never** to `org_tree.name`.
- *Happy path — anchor with JOIN (BOM fixture from origin SC2).* Anchor selects from `bill_of_materials`; recursive joins `bill_of_materials` with `bom`. Asserts the recursive `bom` reference resolves through the anchor's subquery alias (no phantom `bom.col → bom.col`).
- *Edge case — `MAX RECURSION LEVEL`.* The clause is recognised; lineage matches the same query without it.
- *Edge case — `LIMIT ALL` (Runtime 17.2+).* Passes through; lineage unchanged.
- *Edge case — graph-cycle-detection fixture from origin SC2.* `WITH RECURSIVE search_graph(f, t, …) AS (SELECT *, … FROM graph g UNION ALL …)` produces edges from `graph` columns to consumers, no `search_graph.col → search_graph.col`.
- *Integration — recursive CTE consumer in INSERT.* `INSERT INTO target WITH RECURSIVE …` produces the same edges as a top-level SELECT consumer.
- *Regression — non-recursive CTEs unchanged.* Existing `test_cte_resolution` and the multi-source CTE tests still pass with no edge changes.

**Verification:**
- `python -m pytest tests/test_sql_parser.py -k "recursive or cte" -v` passes.
- For each fixture, the count of edges with `source_col.startswith(cte_alias + ".")` AND `target_col.startswith(cte_alias + ".")` is **zero**.

---

- U2. **`parsers/sql_script.py` foundation: block tokeniser + control-flow walker + variable bindings**

**Goal:** Expose `normalize_script(sql) -> tuple[str, list[VirtualSource], dict[str, str]]` returning (a) a flattened SQL safe for `_split_top_level_statements`, (b) virtual-source declarations (empty for this unit; populated by U4), (c) a variable-binding map for U6.

**Requirements:** R7, R8, R10

**Dependencies:** None.

**Files:**
- Create: `backend/parsers/sql_script.py`
- Create: `backend/tests/test_sql_script_normalize.py`

**Approach:**
- Drive the walker off `sqlglot.tokens.Tokenizer().tokenize(sql)` so string/comment handling is shared with `_split_top_level_statements` (origin risk mitigation).
- Define `VirtualSource` as a small dataclass (`name: str`, `body_sql: str`); list is empty in this unit, populated in U4.
- Implement a recursive-descent block walker keyed off keyword tokens. State per scope: `bindings: dict[str, str]` (name → literal SQL form), `parent_bindings` for lookups.
- Recognised top-level wrappers: `BEGIN [ATOMIC] [label] … END [label]`, `IF…END IF`, `CASE…END CASE` (statement form only — distinguish from `CASE` *expression* by terminator), `WHILE…END WHILE`, `LOOP…END LOOP`, `REPEAT…END REPEAT`. For each: walk the body unconditionally, hoist embedded DML to top-level statements joined by `;`.
- `DECLARE name [, ...] type [DEFAULT literal_expr]` — when `DEFAULT` is a string literal, numeric literal, `NULL`, boolean, or a `||` chain over those: store the *resolved literal* in the binding map. Otherwise skip.
- `DECLARE name CONDITION ...` — recognise and skip.
- `DECLARE handler_type HANDLER FOR … action` — walk the handler action body for DML (it may contain `INSERT INTO error_log …`); skip the SQLSTATE/condition wrapper itself.
- `SET name = expr` / `SET VAR name = expr` / `SET (a, b) = (e1, e2)` — when RHS is a literal or foldable `||`, update the binding; otherwise clear that variable's binding (defensive — never let a stale literal survive a runtime assignment).
- `SIGNAL` / `RESIGNAL` / `LEAVE` / `ITERATE` — recognise and skip (no data flow).
- Emit hoisted statements joined with `;` and trailing newline. Preserve `source_line` offsets when feasible (e.g., emit a `-- LINE: N` marker comment that `parse_sql` can read; if line tracking proves brittle, fall back to wrapper line and document the limitation).

**Execution note:** Test-first. The block tokeniser is the dominant silent-drop risk in this plan; lock down compound, nested, and labelled-block cases before wiring U3.

**Technical design:**

```
def normalize_script(sql: str) -> tuple[str, list[VirtualSource], dict[str, str]]:
    if not _has_procedural_keyword(sql):
        return sql, [], {}
    tokens = _tokens_with_offsets(sql)
    walker = _BlockWalker(tokens, sql)
    walker.walk_top_level()
    return walker.emit(), walker.virtual_sources, walker.top_scope_bindings
```

`_BlockWalker` maintains a scope stack; each scope has its own bindings; `_BlockWalker.emit()` joins all hoisted DML chunks.

**Patterns to follow:**
- `_split_top_level_statements` (already uses `sqlglot.tokens.Tokenizer`).
- `parsers/pyspark.py` for an existing recursive-walker pattern in this repo.

**Test scenarios:**
- *Happy path — minimal `BEGIN…END`.* `BEGIN INSERT INTO t SELECT a FROM s; END` flattens to `INSERT INTO t SELECT a FROM s;`.
- *Happy path — `BEGIN ATOMIC`.* Same result; `ATOMIC` recognised and skipped.
- *Happy path — labelled `BEGIN`.* `proc: BEGIN … END proc` flattens; label dropped.
- *Happy path — nested `BEGIN…END`.* Two levels of nesting flattens to a flat statement list.
- *Edge case — comments inside body.* `BEGIN /* note */ INSERT … -- trailing\nEND` — comments preserved; `END` inside a comment or string is **not** treated as a block terminator (drives the tokenizer-shared-handling test).
- *Edge case — string literal containing `END`.* `BEGIN INSERT INTO t SELECT 'END' AS lit FROM s; END` — the inner `END` literal does not close the block.
- *Edge case — multi-variable DECLARE.* `DECLARE x, y, z STRING DEFAULT 'a'` binds all three to `'a'`.
- *Edge case — DECLARE without DEFAULT.* No binding stored; variable is unbound (treated as non-foldable downstream).
- *Edge case — `SET` with non-literal RHS.* `SET total = (SELECT SUM(x) FROM t)` clears the binding for `total` if it had one.
- *Edge case — `||` chain over bound vars.* `DECLARE p STRING DEFAULT 'foo'; DECLARE q STRING DEFAULT p || '_bar'` → `q` resolves to `'foo_bar'`.
- *Edge case — handler body with embedded DML.* `DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN INSERT INTO err_log …; END;` — the `INSERT` is hoisted; the `DECLARE … HANDLER` wrapper is stripped.
- *Edge case — IF/ELSEIF/ELSE with DML in every branch.* All three branches' DML is hoisted unconditionally; resulting flattened statement list contains all three.
- *Edge case — CASE statement-form.* Three `WHEN` branches plus `ELSE`, each containing an INSERT. All four INSERTs in the flattened output.
- *Edge case — CASE expression form (DML scalar context).* Should **not** be flattened — distinguished from statement-form CASE by absence of `END CASE` terminator (`CASE WHEN … THEN … END` inside SELECT). Verify the walker passes this through unchanged.
- *Edge case — WHILE / LOOP / REPEAT.* Each loop body walked once; embedded INSERT hoisted exactly once.
- *Error path — malformed block (missing `END`).* Walker detects unclosed block; returns the input unchanged (graceful degradation) and records a synthetic warning. A wholesale unparseable wrapper must not silently drop edges — `parse_sql` falls through to its existing parse-error path.
- *Error path — `END` without matching `BEGIN`.* Walker reports unbalanced block; returns input unchanged.
- *No procedural keywords present.* `normalize_script("SELECT * FROM t")` returns the input unchanged with empty virtual-source list and empty bindings (cheap pre-check exit).

**Verification:**
- All test cases above pass.
- Bundle a real customer fixture (or synthetic one matching the org-tree procedure body in `sql-scripting.md`) and assert the flattened output round-trips through `_split_top_level_statements` to the expected DML statements.

---

- U3. **Wire `normalize_script` into `parse_sql`**

**Goal:** Make `parse_sql` invoke the normaliser when procedural keywords are present, before `_split_top_level_statements`. Surface per-statement warnings instead of file-level "Unexpected token".

**Requirements:** R1, R4, R5, R7, R8

**Dependencies:** U2.

**Files:**
- Modify: `backend/parsers/sql.py` (extend `parse_sql`)
- Modify: `backend/tests/test_sql_parser.py` (parity tests)

**Approach:**
- Add a cheap procedural pre-check (case-insensitive substring scan for `BEGIN`, `IF `, `CASE`, `WHILE`, `FOR `, `LOOP`, `REPEAT`, `DECLARE`, `EXECUTE IMMEDIATE`, `CREATE PROCEDURE`, `CALL `). The scan must skip strings/comments — reuse the same tokenizer call already needed by `_split_top_level_statements` so the work isn't duplicated.
- When the pre-check matches, call `normalize_script(sql)` and use its first return value as the SQL fed to `_split_top_level_statements`. Otherwise the existing path runs unchanged (no perf impact for non-procedural files).
- Variable bindings (third return value) are stored on a scoped object accessible to the per-statement parse loop; U6 uses them. For U3, populate but do not consume.
- Virtual sources (second return value) are empty until U4; U3 plumbs the list through but does not act on it.
- When `normalize_script` returns the input unchanged due to a tokenisation error, `parse_sql` falls through to its existing parse-error path — a single per-statement warning carrying the original SQL preview, not "Unexpected token" silently shed.

**Patterns to follow:**
- `parse_sql`'s existing per-statement loop (lines 1108–1116 of current `parsers/sql.py`).

**Test scenarios:**
- *Happy path — parity (SC1).* Parameterised fixture pair: `<sql>_top_level.sql` (e.g., `INSERT INTO t SELECT a FROM s`) vs. `<sql>_in_block.sql` (the same wrapped in `BEGIN … END;`). Edge sets are equal after temp-view resolution.
- *Happy path — procedure body with multiple INSERTs.* `BEGIN INSERT INTO a SELECT 1 FROM s; INSERT INTO b SELECT 2 FROM s; END` produces both edges.
- *Happy path — IF/ELSE with different INSERTs in each branch.* Both target tables get edges; no branching is encoded.
- *Edge case — pre-check false positive.* SQL containing the literal string `'BEGIN'` inside a column value passes through the tokenizer-aware pre-check and is **not** routed to `normalize_script`.
- *Error path — malformed body.* `BEGIN INSERT INTO t SELECT FROM` (no source) — `normalize_script` returns the body; `_split_top_level_statements` runs; the malformed `INSERT` produces a per-statement parse-warning; other valid statements in the same block still produce edges.
- *Regression — non-procedural files.* All existing `test_sql_parser.py` cases pass unchanged; no measurable perf change (tokens are scanned at most once per file).
- *Integration — `_resolve_views=False` from the engine.* When `engine._parse_file` calls `parse_sql(..., _resolve_views=False)` for a Databricks notebook cell containing a `BEGIN…END` block, the per-cell edges produced equal the same DML at top level once the engine's per-file `resolve_temp_views` finishes.

**Verification:**
- `python -m pytest tests/test_sql_parser.py -v` passes.
- Manual smoke on a customer file with `BEGIN…END`: previously zero edges + 1 file-level warning; now non-zero edges + per-statement warnings only for genuinely unparseable statements.

---

- U4. **FOR cursor virtual sources**

**Goal:** Implement the `FOR variable_name AS query DO … END FOR` pattern: cursor query becomes a synthetic virtual source `__for_<label_or_var>__`; qualified `variable_name.col` references inside the body resolve to that virtual source.

**Requirements:** R6, R9

**Dependencies:** U2 (block walker scaffolding), U3 (parser wiring).

**Files:**
- Modify: `backend/parsers/sql_script.py` (FOR-loop case)
- Modify: `backend/tests/test_sql_script_normalize.py` (cursor cases)
- Modify: `backend/tests/test_sql_parser.py` (end-to-end FOR fixture)

**Approach:**
- When the walker sees `FOR variable_name AS query DO … END FOR`:
  1. Generate a synthetic virtual-source name from the label (or var name): `name = f"__for_{label_or_var}__"`.
  2. Hoist `INSERT INTO {name} {query}` (or `CREATE TEMP VIEW {name} AS {query}` — pick the form that flows naturally through existing `_resolve_temp_views` so consumers chain to real sources). Decide form when implementing: temp view is preferred so resolve_temp_views collapses it.
  3. Within the FOR body's scope, register a rewrite: `variable_name.col` → `__for_<id>__.col`. Apply the rewrite by walking the body's tokens and substituting qualified references before hoisting body DML.
  4. After `END FOR`, drop the rewrite from scope.
- For `FOR query DO` (no variable name): hoist the cursor query as a temp view but do not register a body rewrite; body DML uses unqualified columns and lateral resolution, same as today.
- Synthetic name uniqueness: increment a per-walker counter on collision (`__for_row__`, `__for_row_2__`).

**Patterns to follow:**
- Temp-view hoisting form mirrors how `CREATE TEMP VIEW` is handled by `_is_temp_view` and `resolve_temp_views`.
- Synthetic prefix mirrors `__sub_N__` from `_next_sub_alias`.

**Test scenarios:**
- *Happy path — qualified cursor reference.* `FOR row AS SELECT order_id, amount FROM orders DO INSERT INTO summary VALUES (row.order_id, row.amount); END FOR` produces edges `orders.order_id → summary.…`, `orders.amount → summary.…`. No `__for_row__` survives in the final graph (resolve_temp_views collapses it).
- *Happy path — labelled FOR.* `process_orders: FOR row AS … DO … END FOR process_orders` uses `__for_process_orders__` as the synthetic name (label takes precedence over var name).
- *Edge case — FOR with no variable name.* `FOR SELECT … DO INSERT INTO target VALUES (col); END FOR` — body uses unqualified column; cursor lineage best-effort, documented limitation. Test asserts no crash and produces best-effort edges.
- *Edge case — nested FOR loops.* Two nested FOR-AS loops produce two synthetic temp views; inner cursor references resolve only inside the inner scope.
- *Edge case — LEAVE inside FOR.* `LEAVE process_orders` is recognised and skipped; loop body is still walked once and its DML hoisted.
- *Edge case — `FOR` keyword inside `INSERT … VALUES` (non-loop FOR).* False-positive guard: `INSERT INTO t VALUES (1) FOR x` — only block-level `FOR var AS query DO` triggers the cursor path.
- *Integration — synthetic node visibility.* After the engine processes a file with a FOR loop, the resolved graph contains zero edges with `source_col.startswith("__for_")` or `target_col.startswith("__for_")` (all collapsed by resolve_temp_views).

**Verification:**
- All cursor tests pass.
- For the `process_orders` fixture: edges trace `orders.order_id → summary.…` end-to-end; `__for_process_orders__` does not appear in `/tables`.

---

- U5. **Stored procedures: `CREATE PROCEDURE` body parsing + `CALL` placeholder**

**Goal:** Strip the `CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] name(params) characteristics AS BEGIN … END` wrapper and parse the body as a top-level script. Register the qualified procedure name in a module-level registry. `CALL name(args)` emits a `__call_<proc>__` placeholder edge with a per-statement warning. `DROP`/`DESCRIBE`/`SHOW PROCEDURE` recognised and silently skipped.

**Requirements:** R6, R11, R13

**Dependencies:** U2 (block walker), U3 (parser wiring).

**Files:**
- Modify: `backend/parsers/sql_script.py` (procedure / CALL / DROP-DESCRIBE-SHOW handling, registry)
- Modify: `backend/parsers/sql.py` (CALL → wildcard edge emitter; reuse `_wildcard_edge`)
- Modify: `backend/tests/test_sql_script_normalize.py` (procedure body extraction, registry)
- Modify: `backend/tests/test_sql_parser.py` (end-to-end procedure + CALL)

**Approach:**
- In `sql_script.py` walker: on `CREATE [OR REPLACE] PROCEDURE [IF NOT EXISTS] qname(param_list) characteristic+ AS BEGIN…END`:
  1. Parse the parameter list to consume `IN`/`OUT`/`INOUT` modes; discard parameter names and types (no lineage v1).
  2. Skip characteristic clauses (`LANGUAGE SQL`, `SQL SECURITY INVOKER`, `NOT DETERMINISTIC`, `MODIFIES SQL DATA`, `COMMENT '…'`, `DEFAULT COLLATION …`).
  3. Extract the inner `BEGIN…END` body and walk it as a nested compound block (reusing the U2 walker).
  4. Register `(qname, body_sql)` in the module-level `_PROCEDURE_REGISTRY` dict for v2 cross-file resolution. Idempotent on `OR REPLACE`.
- On `CALL qname(args)`: hoist a synthetic top-level statement that the parser can recognise — emit `INSERT INTO __call_<qname>__ SELECT * FROM __call_<qname>__` (or use a marker the parser then rewrites into a `_wildcard_edge`). Pick the form that flows through the existing `_parse_single_statement` pipeline; the cleanest is a small new branch in `_parse_single_statement` that detects an `exp.Command` whose name is `CALL` and emits one approximate wildcard edge directly. Add per-statement warning text: `"Unresolved CALL to <qname>; cross-file body resolution deferred to v2"`.
- On `DROP [PROCEDURE] [IF EXISTS] qname` / `DESCRIBE PROCEDURE [EXTENDED] qname` / `SHOW PROCEDURES [FROM|IN schema]`: walker recognises and emits nothing (silent skip).
- Synthetic `__call_<proc>__` name: sanitise `qname` with `_TVF_SANITIZE_RE` (already in `parsers/sql.py`) so dots become underscores. Reuse the existing pattern.

**Patterns to follow:**
- `_wildcard_edge` factory.
- `_TVF_SANITIZE_RE` for synthetic name sanitisation.
- Module-level state pattern from `_subquery_counter` (mutable list container at module scope).

**Test scenarios:**
- *Happy path — full procedure body.* The `run_daily_etl` example from `sql-scripting.md`: a procedure containing two `EXECUTE IMMEDIATE 'TRUNCATE TABLE …'` and `'INSERT INTO …'` with `||` folding. After U6 lands, this fixture should produce real edges; for U5 alone, it produces a placeholder dynamic-SQL edge if EXECUTE IMMEDIATE isn't yet folded — adjust assertion in U6.
- *Happy path — procedure body with plain DML.* `CREATE PROCEDURE p() LANGUAGE SQL SQL SECURITY INVOKER AS BEGIN INSERT INTO t SELECT * FROM s; END` produces edge `s.* → t.*` (or column-level if SELECT names columns); the `CREATE PROCEDURE` wrapper does not appear in lineage.
- *Happy path — `OR REPLACE` and `IF NOT EXISTS`.* Both syntactic forms strip identically.
- *Happy path — registry populated.* `_PROCEDURE_REGISTRY["my_catalog.my_schema.run_etl"]` contains the body SQL after parsing.
- *Edge case — parameter list with `IN`/`OUT`/`INOUT` modes and DEFAULTs.* All modes recognised; defaults discarded; no edges from parameters.
- *Edge case — multiple characteristic clauses in any order.* `LANGUAGE SQL SQL SECURITY INVOKER COMMENT '…' NOT DETERMINISTIC` all skipped.
- *Happy path — CALL placeholder.* `CALL my_catalog.my_schema.run_etl('raw', 'silver')` produces one approximate edge `__call_my_catalog_my_schema_run_etl__.* → __call_my_catalog_my_schema_run_etl__.*` and one per-statement warning.
- *Happy path — CALL with named parameters.* `CALL run_etl(target_schema => 'silver', source_schema => 'raw')` produces the same shape of placeholder edge.
- *Happy path — DROP/DESCRIBE/SHOW PROCEDURE silent skip.* Each produces zero edges and zero warnings (silently skipped).
- *Integration — procedure body referencing local variables.* Variables declared inside the procedure body are scoped to the body; the binding map is reset on procedure exit.
- *Edge case — nested procedure not allowed.* Per origin assumption, customers don't nest procedures. Test that v1 parses the outermost body only and emits a warning if `CREATE PROCEDURE` appears inside another procedure body.

**Verification:**
- Procedure-body fixtures produce edges identical to the same DML at top level (excepting EXECUTE IMMEDIATE, covered in U6).
- `__call_*__` synthetic names appear only in the resolved graph when a `CALL` was actually present.

---

- U6. **EXECUTE IMMEDIATE constant folding + dynamic-SQL placeholder**

**Goal:** Foldable `EXECUTE IMMEDIATE 'literal SQL'` and `EXECUTE IMMEDIATE 'pre ' || var || ' post'` (where `var` has a literal binding from `DECLARE` / constant `SET`) re-parse via the normal pipeline. Non-foldable strings degrade to `__dynamic_sql__.* → __dynamic_sql__.*` plus a per-statement warning. `USING`/`INTO` clauses are dropped in v1.

**Requirements:** R3, R6, R10, R12

**Dependencies:** U2 (variable-binding map), U3 (parser wiring).

**Files:**
- Modify: `backend/parsers/sql_script.py` (EXECUTE IMMEDIATE case + folding)
- Modify: `backend/parsers/sql.py` (dynamic-SQL placeholder edge emission, mirrors CALL)
- Modify: `backend/tests/test_sql_script_normalize.py` (folding, placeholder)
- Modify: `backend/tests/test_sql_parser.py` (end-to-end EXECUTE IMMEDIATE)

**Approach:**
- In the walker, on `EXECUTE IMMEDIATE expr [INTO …] [USING …]`:
  1. Discard the `INTO` and `USING` clauses (origin: parameter substitution deferred).
  2. Attempt to fold `expr` using the in-scope binding map:
     - `'literal'` → literal string.
     - `'pre' || var` → resolve `var` from bindings; concatenate.
     - `var || 'post'` → same, reversed.
     - Chains of the above.
     - Recursively bound vars (`q := p || '_bar'` where `p := 'foo'`) — fold in two passes, capped at a small depth (e.g., 4) to bound work.
  3. If fully folded to a string literal: hoist the inner SQL as a top-level statement (it gets parsed by the existing pipeline along with the rest).
  4. If not fully foldable: hoist a placeholder. Cleanest route: emit a marker the parser recognises (e.g., a comment-marked statement) that produces one `_wildcard_edge("__dynamic_sql__.*", "__dynamic_sql__.*", …)` plus a per-statement warning `"Non-foldable EXECUTE IMMEDIATE; lineage incomplete"`. Symmetrical to CALL placeholder.
- Folding is **literal-only**. No expression evaluation, no function calls, no arithmetic. Resist scope creep (origin risk).
- Re-parsed inner SQL flows through the same `_split_top_level_statements` → `sqlglot.parse_one` pipeline; if the folded string is itself malformed, it surfaces as a normal per-statement warning.

**Patterns to follow:**
- `_wildcard_edge` factory.
- Per-statement warning collection in `parse_sql` (`local_warnings.append(...)`).

**Test scenarios:**
- *Happy path — constant string.* `EXECUTE IMMEDIATE 'INSERT INTO t SELECT a FROM s'` produces edge `s.a → t.a`.
- *Happy path — `||` chain folds (origin SC3).* `DECLARE table_name STRING DEFAULT 'my_catalog.my_schema.staging'; EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || table_name; EXECUTE IMMEDIATE 'INSERT INTO ' || table_name || ' SELECT * FROM source'` produces real edges to/from `my_catalog.my_schema.staging` resolved from `table_name`. The `TRUNCATE` statement produces no edges (it's a delete, not data-flow).
- *Happy path — recursively folded var.* `DECLARE p STRING DEFAULT 'sch'; DECLARE t STRING DEFAULT p || '.tbl'; EXECUTE IMMEDIATE 'INSERT INTO ' || t || ' SELECT a FROM s'` resolves `t` to `'sch.tbl'` and produces `s.a → sch.tbl.a`.
- *Happy path — `INTO`/`USING` clauses dropped.* `EXECUTE IMMEDIATE 'SELECT SUM(c1) FROM VALUES(?), (?) AS t(c1)' USING 5, 6` parses the static SQL skeleton; `?` placeholders survive in the parsed body but produce no spurious edges; `USING` does not contribute.
- *Edge case — non-foldable expression.* `EXECUTE IMMEDIATE current_query()` (function call, not foldable) → emits one `__dynamic_sql__.* → __dynamic_sql__.*` edge + warning `"Non-foldable EXECUTE IMMEDIATE; lineage incomplete"`.
- *Edge case — non-foldable due to unbound variable.* `DECLARE x STRING; EXECUTE IMMEDIATE 'INSERT INTO ' || x || ' SELECT * FROM s'` (no DEFAULT for `x`) → placeholder edge + warning.
- *Edge case — `SET` clears prior binding.* `DECLARE p STRING DEFAULT 'foo'; SET p = (SELECT name FROM t LIMIT 1); EXECUTE IMMEDIATE 'INSERT INTO ' || p || ' SELECT * FROM s'` → placeholder edge (binding cleared).
- *Edge case — folded SQL is itself malformed.* `DECLARE x STRING DEFAULT 'NOT VALID SQL'; EXECUTE IMMEDIATE x` → folds, then per-statement parse warning surfaces; no edges.
- *Edge case — `EXECUTE IMMEDIATE` inside a FOR loop body.* Bindings from the enclosing scope are visible; folding works as expected.
- *Integration — procedure body with EXECUTE IMMEDIATE.* The `run_daily_etl` example produces real edges to `target_schema.orders_daily` and from `source_schema.orders` once `target_schema` and `source_schema` are bound. (When invoked via `CALL`, parameters are not bound in v1 — covered by Deferred for later.)

**Verification:**
- The origin SC3 fixture (`'TRUNCATE TABLE ' || table_name` + `'INSERT INTO ' || table_name || ' SELECT * FROM source'`) produces real `source → my_catalog.my_schema.staging` edges.
- For non-foldable cases, exactly one `__dynamic_sql__` edge per statement, and exactly one per-statement warning.

---

- U7. **Synthetic-node tagging in route layer + warning fidelity verification**

**Goal:** Surface a `synthetic: true` flag on `__for_*__` / `__call_*__` / `__dynamic_sql__` table entries returned by `/tables`, so the frontend (deferred PR) can render them distinctly. Verify R4 (warning fidelity) end-to-end.

**Requirements:** R4, R6

**Dependencies:** U2, U4, U5, U6 (synthetic prefixes must be in place).

**Files:**
- Modify: `backend/api/routes.py` (or wherever `/tables` is shaped — locate during implementation; `engine.py` has the column-metadata helper)
- Modify: `backend/tests/test_routes.py` (synthetic flag visibility)
- Modify: `backend/tests/test_engine.py` (per-file warning counts on procedural files)

**Approach:**
- In the route/engine helper that produces the `/tables` payload, detect synthetic table names with a single helper: `def _is_synthetic_table(name: str) -> bool: return name.startswith("__") and name.endswith("__")`. Add a `synthetic` boolean to each table entry.
- Tag the corresponding warning class so the frontend can later filter (origin defers full frontend treatment).
- Manually exercise on a customer fixture: previously `warning_count == 1` (file-level "Unexpected token"); now `warning_count` reflects only genuine per-statement issues, and the count is non-zero only when the file actually has unparseable embedded statements.

**Patterns to follow:**
- Existing `to_public_dict()` pattern on `SourceEntry` for explicit serialization (do not use `dataclasses.asdict()` — see `docs/solutions/architecture-patterns/backend-parser-state-refactor-patterns-2026-05-01.md`).

**Test scenarios:**
- *Happy path — `/tables` lists `__call_*__` with `synthetic: true`.* After parsing a file with a `CALL` to an unknown procedure, `/tables` includes the synthetic placeholder with the flag set.
- *Happy path — `/tables` excludes synthetic tables that carry no edges.* If `__for_<id>__` is collapsed by `resolve_temp_views`, it does not appear in `/tables`.
- *Integration — warning fidelity (R4).* A file that previously produced one file-level "Unexpected token" warning now produces zero or more per-statement warnings, and the file's `error_files` flag in `GraphResult.file_stats` reflects only files where every statement failed.
- *Regression — non-synthetic tables unchanged.* Existing `test_routes.py` tables-list tests pass with no shape changes (they should not look at `synthetic`).

**Verification:**
- `python -m pytest tests/test_routes.py -v` passes.
- Manually upload `sample_data/sample_lineage.zip` with a procedural fixture appended; verify `/tables` and `/warnings` reflect the new shape.

---

## System-Wide Impact

- **Interaction graph:** `parsers/sql.py:parse_sql` ← `parsers/sql_script.py:normalize_script`. `lineage/engine.py:_parse_file` is unchanged in shape — it continues to call `parse_sql` on each cell with `_resolve_views=False` and run `resolve_temp_views` once per file. The new synthetic temp views (`__for_<id>__`) ride the existing temp-view resolution path.
- **Error propagation:** `normalize_script` failures (unbalanced blocks, malformed input) return the input unchanged with a synthetic warning string in the bindings/virtual-source returns; `parse_sql` then runs its existing per-statement parse-error path, surfacing one warning per failed statement. No file-level wholesale failures.
- **State lifecycle risks:** The procedure registry in `sql_script.py` is module-level and process-lifetime. Two upload-refresh cycles in the same process accumulate procedure entries — fine for v1 (no eviction needed; `OR REPLACE` handles overwrites), but document this in the module docstring. No tests depend on a fresh registry, but `tests/test_routes.py:reset_state` is unaffected because the registry is parser state, not engine/route state.
- **API surface parity:** `parse_sql` signature and `ParseResult` shape are unchanged. `LineageEdge` schema is unchanged (synthetic edges use existing `confidence="approximate"` and `qualified=False`). `/warnings` payload shape unchanged; warning *count and content* changes (per-statement granularity). `/tables` gains a `synthetic` boolean — additive only.
- **Integration coverage:** Two cross-layer scenarios warrant explicit tests beyond unit tests on the normaliser:
  1. *Engine + per-cell notebook:* a Databricks notebook with a procedural cell. The engine collects per-cell edges with `_resolve_views=False`, then runs `resolve_temp_views` once. Synthetic FOR temp views must collapse the same way real temp views do.
  2. *Routes + tables endpoint:* synthetic flag visibility plus warning-count fidelity on a real upload.
- **Unchanged invariants:**
  - `parse_sql` continues to return `ParseResult` (per `backend/AGENTS.md`).
  - Engine owns format dispatch; the parser does not detect notebook separators (per `backend/AGENTS.md`).
  - Two graphs (`lineage_graph`, `raw_graph`) stay in sync because `parse_sql` returns both `edges` and `raw_edges` as before.
  - Column IDs are 4-part `catalog.schema.table.column`; synthetic tables use `__name__` as their bare table form, which `split_column_id` handles correctly via `rsplit(".", 1)`.
  - `_parse_command_fallback` is **not** extended (per origin risk).

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| **Block-delimiter tokenisation bugs silently drop or duplicate edges** (origin's primary risk). The pre-processor must correctly pair `BEGIN`/`END`, `IF`/`END IF`, etc. across nested blocks, comments, and string literals. | Drive the tokeniser off `sqlglot.tokens.Tokenizer` (shared with `_split_top_level_statements`); aggressive U2 fixture coverage including comments-containing-`END`, strings-containing-`END`, and nested labelled blocks; graceful unchanged-input fallback on any unbalanced state plus a synthetic warning. Test-first execution posture for U2 (see Execution note). |
| **`FOR row AS SELECT … DO` cursor aliasing — unqualified column references inside the body fall through.** v1 only handles qualified `row.col`. | Documented limitation in U4 and in the `sql_script.py` module docstring. Track via a follow-up issue if customers hit it. |
| **EXECUTE IMMEDIATE folding scope creep.** Tempting to evaluate function calls, arithmetic, or further DECLARE chains. | U6 explicitly limits folding to literals + `||` chains over bound literals + bounded recursion. Anything else degrades to wildcard. Code review checks for any `eval`-shaped logic. |
| **Procedure registry growth.** Module-level dict accumulates across upload-refresh cycles. | Acceptable for v1 (no per-process pressure expected); `OR REPLACE` overwrites; documented as a known property in the module docstring. Eviction is part of the deferred v2 cross-file resolution work. |
| **SQLGlot version drift.** Procedural keyword token names (`BEGIN`, `END`, etc.) might rename in future versions. | U2 verifies token names against the installed version on import (a one-time assertion in module init), failing fast with a clear message. |
| **Unparseable statement misclassification.** The `_parse_command_fallback` path could swallow what should be a per-statement warning. | Origin explicitly forbids extending `_parse_command_fallback`. New synthetic-edge emission lives in U5/U6 paths and goes through `_parse_single_statement`'s normal flow. |
| **Frontend rendering for synthetic nodes is deferred.** Until the frontend ships rendering, synthetic nodes appear as plain tables in `/lineage` and `/catalog`. | Acceptable per origin; backend tagging in U7 is the prerequisite. Coordinate the frontend follow-up PR after this plan ships. |

---

## Phased Delivery

### Phase 1 — Recursive CTE precision (U1)

Lands first; isolated; high confidence; immediate user-visible win on any file using `WITH RECURSIVE` today. Zero coupling to the new module.

### Phase 2 — Normaliser foundation + parser wiring (U2, U3)

Lands the pre-processor module and wires it in. After this phase, plain-DML procedural wrappers (`BEGIN…END`, IF/CASE/WHILE/LOOP/REPEAT, plain `CREATE PROCEDURE` bodies without EXECUTE IMMEDIATE) produce correct edges. Most of the customer-facing lift.

### Phase 3 — FOR cursors (U4)

Adds virtual-source support for FOR loops; mostly orthogonal to U5/U6 but shares scaffolding from U2.

### Phase 4 — Procedures + CALL placeholder (U5)

Strips `CREATE PROCEDURE` wrappers, registers procedures, and emits CALL placeholders. Covers the `run_daily_etl`-shaped fixtures end-to-end (excepting EXECUTE IMMEDIATE folding, covered in Phase 5).

### Phase 5 — EXECUTE IMMEDIATE folding + dynamic-SQL placeholder (U6)

Closes the dominant Databricks ETL idiom (parameterised `TRUNCATE`/`INSERT` chains).

### Phase 6 — Tagging + warning fidelity verification (U7)

Surfaces synthetic-node tagging and verifies R4 end-to-end. Small; lands last.

---

## Documentation Plan

- New module `backend/parsers/sql_script.py` carries a top-of-file docstring covering: scope (flat data-flow projection), non-goals (no SQL/PSM semantics), folding rules (literals + `||` only), procedure registry lifecycle, synthetic prefix conventions, and pointers to origin + this plan.
- Update `docs/solutions/best-practices/databricks-sql-parser-extensions-2026-04-25.md` after this plan ships, adding a section R9 ("SQL scripting and procedural surface") describing the pre-processor pattern, recursive CTE fix, and synthetic prefix conventions. Cross-reference the new test file.
- No `docs/ARCHITECTURE.md` change required — engine/parser separation invariants are unchanged.
- No `backend/AGENTS.md` change required — pinned floors and conventions all hold. Optional: add one line under "Conventions specific to this repo" about procedural wrappers if it would help future agents discover the new module.

---

## Operational / Rollout Notes

- **No database migrations** — backend state is in-memory only (per CLAUDE.md). After deploy, the next upload-refresh cycle picks up the new parser automatically.
- **Backward compatibility** — `parse_sql` signature, `ParseResult` shape, `LineageEdge` schema, `/lineage` / `/impact` / `/catalog` API shapes are all unchanged. `/tables` gains an additive `synthetic` boolean.
- **Performance** — pre-check is a single tokenizer pass on the SQL string; no measurable overhead for non-procedural files. Procedural files do one extra walk over already-tokenised input, dominated by the existing `_split_top_level_statements` cost.
- **Monitoring** — `/warnings` endpoint becomes a proxy for the success criterion R4: a meaningful drop in "Unexpected token" warnings on procedural files after this lands. Worth eyeballing post-deploy on the staging dataset.
- **Frontend follow-up** — coordinate a small frontend PR after this lands to render `synthetic: true` tables distinctly in `/catalog` and `/lineage` (e.g., dashed border or icon). Tracked under "Deferred to Follow-Up Work" in this plan's Scope Boundaries.

---

## Sources & References

- **Origin document:** [docs/brainstorms/2026-05-04-databricks-sql-scripting-coverage-requirements.md](../brainstorms/2026-05-04-databricks-sql-scripting-coverage-requirements.md)
- **Source spec:** [sql-scripting.md](../../sql-scripting.md) — Databricks SQL/PSM, stored procedures, recursive CTEs, multi-statement transactions
- Related code: `backend/parsers/sql.py` (`parse_sql`, `_resolve_ctes`, `_wildcard_edge`, `_split_top_level_statements`); `backend/lineage/engine.py:_parse_file`; `backend/lineage/models.py` (`ParseResult`, `LineageEdge`)
- Related institutional learnings:
  - `docs/solutions/best-practices/databricks-sql-parser-extensions-2026-04-25.md`
  - `docs/solutions/best-practices/sql-parser-dry-refactoring-patterns-2026-04-27.md`
  - `docs/solutions/architecture-patterns/backend-parser-state-refactor-patterns-2026-05-01.md`
- Related repo guidance: `backend/AGENTS.md`, `CLAUDE.md`
