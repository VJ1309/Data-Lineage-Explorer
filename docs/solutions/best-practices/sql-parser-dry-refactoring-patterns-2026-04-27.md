---
title: SQL Parser Code Organisation — DRY Patterns for parsers/sql.py
date: 2026-04-27
category: docs/solutions/best-practices
module: parsers/sql.py
problem_type: best_practice
component: tooling
severity: low
applies_when:
  - Adding a new SQL construct handler that shares logic with an existing one
  - Noticing that a bug fix in one branch (FROM, JOIN, WHERE, etc.) didn't propagate to identical sibling branches
  - Reviewing parsers/sql.py for dead guards or redundant checks after a coverage expansion pass
tags: sql-parser, refactoring, dry, python, sqlglot, inner-closure, code-organisation
---

# SQL Parser Code Organisation — DRY Patterns for parsers/sql.py

## Context

After a coverage expansion pass (April 2026, see `databricks-sql-parser-extensions-2026-04-25.md`), `parsers/sql.py` had three classes of duplication:

1. **TVF synthetic-name logic** duplicated verbatim across the FROM branch and the JOIN branch of `_parse_select_node`.
2. **Predicate edge emission** (WHERE → `__filter__`, QUALIFY → `__qualify__`, HAVING → `__having__`) repeated 30+ lines of identical column-resolution + edge-construction code, differing only in the clause argument and pseudo-column name.
3. **Wildcard edge construction** in `_parse_copy`, `_parse_clone`, and `_parse_command_fallback` each inlined the same 10-field `LineageEdge(...)` call.

Additionally, two dead-code patterns accumulated:
- `if subquery_aliases:` guards before every call to `_resolve_temp_views` (which already early-returns on empty set).
- `if statement is None: continue` inside a loop whose list was already filtered to non-`None` values during construction.

## Guidance

### 1. Module-level helper for shared syntax patterns

When two branches of `_parse_select_node` (FROM and JOIN) need the same AST traversal to produce a derived value, extract a module-level function. Place the pre-compiled regex at module level too — inline `re.sub(r'...', ...)` in a loop means recompiling on every call.

```python
# module level — compiled once
_TVF_SANITIZE_RE = re.compile(r'[^a-zA-Z0-9_]')

def _tvf_synthetic_name(table_node: exp.Table) -> str:
    fn_expr = table_node.this
    path_str = next((a.this for a in fn_expr.expressions if isinstance(a, exp.Literal)), None)
    raw_name = path_str or fn_expr.name or "tvf"
    return _TVF_SANITIZE_RE.sub('_', raw_name).strip('_') or "tvf"

# inside _parse_select_node — FROM branch
synthetic = _tvf_synthetic_name(from_table)

# inside _parse_select_node — JOIN branch (was copy-pasted before)
synthetic = _tvf_synthetic_name(jtable)
```

**Why module-level and not inner closure?** The function has no dependency on the enclosing function's state (no `edges`, no `target_table`). It only takes a `exp.Table` node and returns a string. Module-level is the correct scope.

### 2. Inner closure for context-dependent helpers

When a repeated block needs access to the enclosing function's local state (`edges`, `target_table`, `_resolve_table_hint`, `multi_source`, `default_table`, plus the source provenance triple), an inner closure is the right scope. The closure captures state without requiring it to be threaded through as parameters.

```python
# inside _parse_select_node, before the predicate blocks
def _emit_predicate_edges(clause: exp.Expression | None, pseudo_col: str, transform_type: str) -> None:
    if clause is None:
        return
    expr_str = clause.sql(dialect="databricks")
    seen: set[str] = set()
    for col_ref in clause.find_all(exp.Column):
        col_name = col_ref.name
        if not col_name:
            continue
        table_hint = col_ref.table
        if table_hint:
            resolved_table, _certain = _resolve_table_hint(table_hint)
            qualified = True
        else:
            resolved_table = default_table
            qualified = not multi_source
        src = f"{resolved_table}.{col_name}"
        if src in seen:
            continue
        seen.add(src)
        edges.append(LineageEdge(
            source_col=src,
            target_col=f"{target_table}.{pseudo_col}",
            transform_type=transform_type,
            expression=expr_str,
            source_file=source_file,
            source_cell=source_cell,
            source_line=source_line,
            confidence="certain" if qualified else "approximate",
            qualified=qualified,
        ))

# call sites — three one-liners replacing ~90 lines
_emit_predicate_edges(select_node.args.get("where"), "__filter__", "filter")
_emit_predicate_edges(select_node.args.get("qualify"), "__qualify__", "filter")
_emit_predicate_edges(select_node.args.get("having"), "__having__", "filter")
```

### 3. Factory for uniform edge construction

When multiple functions produce a `LineageEdge` with the same fixed fields and only the source/target columns vary, extract a factory. This prevents the fixed fields from drifting across call sites (e.g., one site forgets `qualified=False` or uses a different `confidence`).

```python
def _wildcard_edge(
    source_col: str,
    target_col: str,
    source_file: str,
    source_line: int | None,
    source_cell: int | None,
) -> LineageEdge:
    return LineageEdge(
        source_col=source_col,
        target_col=target_col,
        transform_type="passthrough",
        expression=None,
        source_file=source_file,
        source_cell=source_cell,
        source_line=source_line,
        confidence="approximate",
        qualified=False,
    )

# _parse_copy
return [_wildcard_edge("__file__.*", f"{target_table}.*", source_file, source_line, source_cell)]

# _parse_clone
return [_wildcard_edge(f"{source_table}.*", f"{target_table}.*", source_file, source_line, source_cell)]

# _parse_command_fallback
return [_wildcard_edge(f"{source_table}.*", f"{target_table}.*", source_file, source_line, source_cell)]
```

### 4. Remove guards that duplicate library early-returns

`_resolve_temp_views` begins with `if not temp_views: return edges`. Any `if subquery_aliases:` guard before calling it is dead. Call unconditionally:

```python
# before
if subquery_aliases:
    return _resolve_temp_views(edges, subquery_aliases)
return edges

# after
return _resolve_temp_views(edges, subquery_aliases)
```

### 5. Remove guards that duplicate construction-time filtering

If a list is built with an explicit `if x is not None: lst.append(x)` guard, iterating that list and checking `if x is None: continue` again is dead code. Trust the construction invariant.

## Why This Matters

The JOIN TVF branch was using `re.sub(r'[^a-zA-Z0-9_]', '_', raw_name)` (recompiling the regex every call) while the FROM branch had already been updated to use the pre-compiled `_TVF_SANITIZE_RE`. This is the canonical failure mode of copy-paste duplication: a fix in one branch silently doesn't propagate to the identical sibling.

Inner closures also make the intent of predicate-edge emission explicit. The three pseudo-column types (filter, qualify, having) are structurally identical; expressing that identity in code makes it obvious when a fourth type is added.

## When to Apply

- After any coverage expansion pass that adds N similar handlers — look for structural duplication before merging
- When a bug is fixed in one branch of `_parse_select_node` but the same fix would apply to other branches
- When reviewing PRs touching `parsers/sql.py`: check whether a new inline block is a candidate for `_emit_predicate_edges` or a new factory

## Examples

Before and after are shown inline in the Guidance section above. The complete refactor is in commit `26ebab2` on the `feat/codebase-ask` branch.

## Related

- `docs/solutions/best-practices/databricks-sql-parser-extensions-2026-04-25.md` — what constructs are supported and how they map to SQLGlot AST nodes (complementary doc: coverage patterns, not organisation patterns)
