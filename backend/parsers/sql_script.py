"""Pre-processor for Databricks SQL/PSM (procedural SQL) constructs.

Scope (flat data-flow projection):
- Strip procedural wrappers — BEGIN…END, BEGIN ATOMIC, labelled blocks, IF/CASE-statement/
  WHILE/LOOP/REPEAT, FOR cursor loops (U4), CREATE PROCEDURE bodies (U5) — and hoist
  embedded DML to top-level statements joined by ';'.
- Recognise DECLARE / SET as a literal-binding map for EXECUTE IMMEDIATE folding (U6).
- Walk DECLARE…HANDLER action bodies for embedded DML (handlers can INSERT into error
  logs); strip the handler wrapper itself.

Non-goals (carried from the plan and origin):
- No SQL/PSM semantics (reachability, type checking, exception flow). Every branch /
  loop body is walked unconditionally — `WHILE i < N DO INSERT …` produces the same
  edges whether the loop runs once or a thousand times.
- No expression evaluation. Variable-binding fold is literal-only:
  string / numeric literals, NULL, booleans, and `||` chains over bound literals.
- USING / INTO clauses on EXECUTE IMMEDIATE are dropped in v1 (parameter substitution
  deferred per origin).

Synthetic prefix conventions:
- `__for_<label_or_var>__`         — FOR cursor virtual sources (U4)
- `__call_<sanitised_proc>__`      — CALL placeholder targets (U5)
- `__dynamic_sql__`                — non-foldable EXECUTE IMMEDIATE placeholder (U6)

Procedure registry:
- `_PROCEDURE_REGISTRY` is a module-level dict keyed by qualified procedure name; CTAS
  callers populate it via U5 for v2 cross-file CALL resolution. It accumulates across
  upload-refresh cycles in a single process — `OR REPLACE` overwrites, but there is
  no eviction. Acceptable for v1; eviction is part of the deferred v2 work.

References:
- docs/plans/2026-05-04-006-feat-databricks-sql-scripting-coverage-plan.md
- docs/brainstorms/2026-05-04-databricks-sql-scripting-coverage-requirements.md
- sql-scripting.md (Databricks SQL/PSM spec)
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlglot import tokens

# ---------------------------------------------------------------------------
# Public dataclasses & module-level state
# ---------------------------------------------------------------------------


@dataclass
class VirtualSource:
    """A synthetic source emitted by the normaliser (e.g. FOR-cursor body).

    Empty in U2; populated by U4. Carries the synthetic name and the original
    body SQL so the engine can hoist it as `CREATE TEMP VIEW <name> AS <body_sql>`
    or similar form that flows through the existing temp-view resolution path.
    """

    name: str
    body_sql: str


# Module-level procedure registry (populated by U5; queryable by v2 cross-file work).
_PROCEDURE_REGISTRY: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Cheap pre-check
# ---------------------------------------------------------------------------

# Lower-cased keywords that signal procedural content. The check is intentionally
# noisy (matches inside string literals or comments). The walker does the precise
# work; this just decides whether to run it at all.
_PROCEDURAL_HINT_RE = None  # built lazily

import re as _re

_PROCEDURAL_KEYWORDS = (
    "begin", "declare", "execute immediate", "create procedure",
    "create or replace procedure", "call ",
    # Control-flow keywords likely to indicate scripting (not a guarantee — the
    # walker confirms). FOR appears inside SELECT INTO too, so the walker must
    # disambiguate.
    " if ", " while ", " loop ", " repeat ", " for ",
)


def _has_procedural_keyword(sql: str) -> bool:
    lo = sql.lower()
    # Normalise whitespace once for the boundary-sensitive checks.
    padded = " " + lo + " "
    return any(kw in padded for kw in _PROCEDURAL_KEYWORDS)


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------


def _tokens(sql: str):
    """Return SQLGlot's token list. Returns [] if tokenisation fails entirely."""
    try:
        return list(tokens.Tokenizer().tokenize(sql))
    except Exception:
        return []


def _kw(tok, *keywords: str) -> bool:
    """True if the token's text matches any of the given keywords (case-insensitive).

    Procedural keywords come through SQLGlot's tokenizer in mixed forms — some as
    bona-fide token types (BEGIN, END, ELSE, FOR), others as bare VAR tokens with
    the keyword as text (DECLARE, IF, WHILE, ELSEIF, DO, LEAVE, etc.). Matching by
    text avoids version-drift on token-type renames.

    Critical: STRING / NUMBER / comment tokens MUST NOT match, otherwise a literal
    `'END'` inside a SELECT would be treated as a block terminator.
    """
    if tok is None:
        return False
    if tok.token_type in (
        tokens.TokenType.STRING, tokens.TokenType.NUMBER,
        tokens.TokenType.NATIONAL_STRING, tokens.TokenType.RAW_STRING,
        tokens.TokenType.HEX_STRING, tokens.TokenType.BYTE_STRING,
    ):
        return False
    t = (tok.text or "").upper()
    return t in {k.upper() for k in keywords}


def _is_semicolon(tok) -> bool:
    return tok is not None and tok.token_type == tokens.TokenType.SEMICOLON


# Statement-starting keywords that we hoist verbatim. Anything else inside a body
# (DECLARE, SET, SIGNAL, etc.) is consumed by dedicated handlers.
_HOIST_STARTERS = {
    "INSERT", "SELECT", "WITH", "UPDATE", "DELETE", "MERGE",
    "CREATE", "REPLACE", "TRUNCATE", "ALTER", "DROP", "GRANT", "REVOKE",
    "USE", "REFRESH", "ANALYZE", "OPTIMIZE", "VACUUM", "MSCK", "DESCRIBE",
    "EXPLAIN", "SHOW", "COPY",
}


# ---------------------------------------------------------------------------
# Walker
# ---------------------------------------------------------------------------


@dataclass
class _Scope:
    bindings: dict[str, str]
    parent: "_Scope | None" = None

    def lookup(self, name: str) -> str | None:
        n = name.lower()
        cur: _Scope | None = self
        while cur is not None:
            if n in cur.bindings:
                return cur.bindings[n]
            cur = cur.parent
        return None

    def bind(self, name: str, literal: str) -> None:
        self.bindings[name.lower()] = literal

    def unbind(self, name: str) -> None:
        self.bindings.pop(name.lower(), None)


class _Walker:
    """Token-stream walker that hoists DML and tracks bindings.

    Token offsets (`tok.start`, `tok.end`) are inclusive char offsets in the original
    source; we use them to slice raw text for hoisted statements (preserves comments,
    formatting, and original identifiers).
    """

    def __init__(self, sql: str, toks: list):
        self.sql = sql
        self.toks = toks
        self.i = 0  # index into toks
        self.hoisted: list[str] = []  # collected DML statements (raw text)
        self.virtual_sources: list[VirtualSource] = []
        self.top_scope = _Scope(bindings={}, parent=None)
        self.failed = False
        # Stack of active FOR-cursor variable rewrites: each entry is (var_name, synthetic_name).
        # Body hoists check this stack and rewrite qualified `var.col` -> `synthetic.col`.
        self._for_rewrites: list[tuple[str, str]] = []
        # Track issued synthetic names to keep them unique within a single normalise call.
        self._synthetic_names: set[str] = set()
        self._next_label_label_for_label: str | None = None  # passed from labelled-block dispatch

    # ---- low-level cursor helpers ----------------------------------------

    def _peek(self, offset: int = 0):
        idx = self.i + offset
        return self.toks[idx] if 0 <= idx < len(self.toks) else None

    def _advance(self) -> None:
        self.i += 1

    def _eof(self) -> bool:
        return self.i >= len(self.toks)

    # ---- raw-text extraction ---------------------------------------------

    def _slice(self, start_tok, end_tok) -> str:
        return self.sql[start_tok.start:end_tok.end + 1]

    # ---- top-level walk ---------------------------------------------------

    def walk_top_level(self) -> None:
        scope = self.top_scope
        try:
            while not self._eof():
                self._dispatch_statement(scope)
                # Consume statement terminator if any
                while not self._eof() and _is_semicolon(self._peek()):
                    self._advance()
        except _UnbalancedBlock:
            self.failed = True

    # ---- statement dispatcher --------------------------------------------

    def _dispatch_statement(self, scope: _Scope) -> None:
        """Read one statement (or block) starting at self.i and act on it."""
        tok = self._peek()
        if tok is None:
            return

        # Labelled statement: <name> : (BEGIN|FOR|WHILE|LOOP|REPEAT)
        nxt = self._peek(1)
        if (
            tok.token_type == tokens.TokenType.VAR
            and nxt is not None
            and nxt.token_type == tokens.TokenType.COLON
        ):
            label_tok = tok
            after = self._peek(2)
            if after is not None and _kw(after, "BEGIN", "WHILE", "LOOP", "REPEAT", "FOR"):
                self._advance()  # label
                self._advance()  # ':'
                # Pass label down for FOR-cursor synthetic naming (label takes precedence).
                self._next_label_label_for_label = label_tok.text
                try:
                    self._dispatch_statement(scope)
                finally:
                    self._next_label_label_for_label = None
                return

        # BEGIN ... END (compound block)
        if _kw(tok, "BEGIN"):
            self._handle_begin_end(scope)
            return
        # IF ... END IF
        if _kw(tok, "IF"):
            self._handle_if(scope)
            return
        # CASE statement form (must reach END CASE — distinguish from CASE expression
        # which appears inside a hoisted statement body, never at statement boundary).
        if tok.token_type == tokens.TokenType.CASE or _kw(tok, "CASE"):
            self._handle_case_statement(scope)
            return
        # WHILE ... END WHILE
        if _kw(tok, "WHILE"):
            self._handle_while(scope)
            return
        # LOOP ... END LOOP
        if _kw(tok, "LOOP"):
            self._handle_loop(scope)
            return
        # REPEAT ... UNTIL ... END REPEAT
        if _kw(tok, "REPEAT"):
            self._handle_repeat(scope)
            return
        # FOR ... DO ... END FOR (cursor loop — U4 will populate virtual_sources;
        # U2 just walks the body so embedded DML is hoisted)
        if tok.token_type == tokens.TokenType.FOR or _kw(tok, "FOR"):
            self._handle_for(scope)
            return

        # DECLARE
        if _kw(tok, "DECLARE"):
            self._handle_declare(scope)
            return

        # SET (and SET VAR alias)
        if tok.token_type == tokens.TokenType.SET or _kw(tok, "SET"):
            self._handle_set(scope)
            return

        # No-op control-flow statements
        if _kw(tok, "SIGNAL", "RESIGNAL", "LEAVE", "ITERATE"):
            self._consume_until_terminator()
            return

        # Hoist DML / DDL verbatim
        if (
            tok.text and tok.text.upper() in _HOIST_STARTERS
        ) or tok.token_type in (
            tokens.TokenType.INSERT, tokens.TokenType.SELECT,
            tokens.TokenType.UPDATE, tokens.TokenType.DELETE,
            tokens.TokenType.MERGE, tokens.TokenType.CREATE,
        ):
            self._hoist_statement()
            return

        # Unknown token at statement boundary — consume to next ';' to keep moving.
        self._consume_until_terminator()

    # ---- helpers: terminator scanning ------------------------------------

    def _consume_until_terminator(self, *, stop_kws: tuple[str, ...] = ()) -> int:
        """Advance until ';' (at depth 0) or one of stop_kws (at depth 0).

        Returns the index of the terminator (or len(toks) at EOF). Tracks paren
        depth so a ';' inside a function call doesn't terminate (defensive — SQL
        statements rarely embed bare ';' inside parens, but be safe).
        """
        depth = 0
        while not self._eof():
            tok = self._peek()
            if tok.token_type == tokens.TokenType.L_PAREN:
                depth += 1
            elif tok.token_type == tokens.TokenType.R_PAREN:
                depth -= 1
            elif depth == 0:
                if _is_semicolon(tok):
                    return self.i
                if stop_kws and tok.text and tok.text.upper() in stop_kws:
                    return self.i
            self._advance()
        return self.i

    # ---- BEGIN ... END ---------------------------------------------------

    def _handle_begin_end(self, scope: _Scope) -> None:
        begin_tok = self._peek()
        self._advance()  # consume BEGIN
        # Optional ATOMIC qualifier
        if not self._eof() and _kw(self._peek(), "ATOMIC"):
            self._advance()
        # v1 binding semantics are flat (last-binding-wins across the whole script).
        # We pass the same scope down rather than creating a child — proper SQL/PSM
        # scoping would require expression evaluation, which is explicitly out of scope.
        inner_scope = scope
        while not self._eof():
            tok = self._peek()
            if _is_semicolon(tok):
                self._advance()
                continue
            if _kw(tok, "END"):
                # Could be END (block close), END IF (handled inside _handle_if), etc.
                # At BEGIN block level, this END closes us.
                self._advance()  # END
                # Optional trailing label
                if not self._eof() and self._peek().token_type == tokens.TokenType.VAR:
                    self._advance()
                return
            self._dispatch_statement(inner_scope)
        # Reached EOF without END
        raise _UnbalancedBlock(f"unclosed BEGIN at offset {begin_tok.start}")

    # ---- IF ... END IF ---------------------------------------------------

    def _handle_if(self, scope: _Scope) -> None:
        if_tok = self._peek()
        self._advance()  # IF
        # Skip the condition until THEN
        self._consume_until_kw("THEN")
        if self._eof() or not _kw(self._peek(), "THEN"):
            raise _UnbalancedBlock(f"IF without THEN at offset {if_tok.start}")
        self._advance()  # THEN
        # Walk branches: each branch ends at ELSEIF / ELSE / END
        self._walk_if_branches(scope)

    def _walk_if_branches(self, scope: _Scope) -> None:
        while not self._eof():
            self._walk_until_branch_terminator(scope, ("ELSEIF", "ELSE", "END"))
            tok = self._peek()
            if tok is None:
                raise _UnbalancedBlock("IF body without END IF")
            if _kw(tok, "ELSEIF"):
                self._advance()
                self._consume_until_kw("THEN")
                if not self._eof() and _kw(self._peek(), "THEN"):
                    self._advance()
                continue
            if _kw(tok, "ELSE"):
                self._advance()
                continue
            if _kw(tok, "END"):
                self._advance()  # END
                # Expect IF
                if not self._eof() and _kw(self._peek(), "IF"):
                    self._advance()
                return
        raise _UnbalancedBlock("IF body without END IF")

    def _walk_until_branch_terminator(
        self, scope: _Scope, terminators: tuple[str, ...]
    ) -> None:
        """Walk statements within a branch body until we hit a terminator keyword."""
        while not self._eof():
            tok = self._peek()
            if _is_semicolon(tok):
                self._advance()
                continue
            if tok.text and tok.text.upper() in terminators:
                return
            self._dispatch_statement(scope)

    def _consume_until_kw(self, *keywords: str) -> None:
        """Advance until we hit one of the given keywords (case-insensitive) or EOF.

        Tracks paren depth so a keyword inside a sub-expression isn't matched at depth>0.
        """
        depth = 0
        target = {k.upper() for k in keywords}
        while not self._eof():
            tok = self._peek()
            if tok.token_type == tokens.TokenType.L_PAREN:
                depth += 1
            elif tok.token_type == tokens.TokenType.R_PAREN:
                depth -= 1
            elif depth == 0 and tok.text and tok.text.upper() in target:
                return
            self._advance()

    # ---- CASE statement form (END CASE) ---------------------------------

    def _handle_case_statement(self, scope: _Scope) -> None:
        """CASE ... WHEN ... THEN ... [ELSE ...] END CASE.

        Statement-form CASE always terminates with `END CASE`. Walk all WHEN and ELSE
        bodies unconditionally.
        """
        case_tok = self._peek()
        self._advance()  # CASE
        # Skip the CASE selector (or jump to first WHEN if absent)
        self._consume_until_kw("WHEN", "ELSE", "END")
        while not self._eof():
            tok = self._peek()
            if _kw(tok, "WHEN"):
                self._advance()  # WHEN
                self._consume_until_kw("THEN")
                if not self._eof() and _kw(self._peek(), "THEN"):
                    self._advance()
                self._walk_until_branch_terminator(scope, ("WHEN", "ELSE", "END"))
                continue
            if _kw(tok, "ELSE"):
                self._advance()
                self._walk_until_branch_terminator(scope, ("WHEN", "END"))
                continue
            if _kw(tok, "END"):
                self._advance()  # END
                if not self._eof() and _kw(self._peek(), "CASE"):
                    self._advance()
                return
            # Unknown: defensive break
            self._advance()
        raise _UnbalancedBlock(f"unclosed CASE at offset {case_tok.start}")

    # ---- WHILE / LOOP / REPEAT -------------------------------------------

    def _handle_while(self, scope: _Scope) -> None:
        while_tok = self._peek()
        self._advance()  # WHILE
        self._consume_until_kw("DO")
        if not self._eof() and _kw(self._peek(), "DO"):
            self._advance()
        # Walk body until END WHILE
        while not self._eof():
            tok = self._peek()
            if _is_semicolon(tok):
                self._advance()
                continue
            if _kw(tok, "END"):
                self._advance()
                if not self._eof() and _kw(self._peek(), "WHILE"):
                    self._advance()
                return
            self._dispatch_statement(scope)
        raise _UnbalancedBlock(f"unclosed WHILE at offset {while_tok.start}")

    def _handle_loop(self, scope: _Scope) -> None:
        loop_tok = self._peek()
        self._advance()  # LOOP
        while not self._eof():
            tok = self._peek()
            if _is_semicolon(tok):
                self._advance()
                continue
            if _kw(tok, "END"):
                self._advance()
                if not self._eof() and _kw(self._peek(), "LOOP"):
                    self._advance()
                return
            self._dispatch_statement(scope)
        raise _UnbalancedBlock(f"unclosed LOOP at offset {loop_tok.start}")

    def _handle_repeat(self, scope: _Scope) -> None:
        rep_tok = self._peek()
        self._advance()  # REPEAT
        # Walk body until UNTIL
        while not self._eof():
            tok = self._peek()
            if _is_semicolon(tok):
                self._advance()
                continue
            if _kw(tok, "UNTIL"):
                # Skip UNTIL <condition> END REPEAT
                self._advance()
                self._consume_until_kw("END")
                if not self._eof() and _kw(self._peek(), "END"):
                    self._advance()
                if not self._eof() and _kw(self._peek(), "REPEAT"):
                    self._advance()
                return
            if _kw(tok, "END"):
                # END REPEAT without UNTIL (some dialects) — also valid
                self._advance()
                if not self._eof() and _kw(self._peek(), "REPEAT"):
                    self._advance()
                return
            self._dispatch_statement(scope)
        raise _UnbalancedBlock(f"unclosed REPEAT at offset {rep_tok.start}")

    # ---- FOR (cursor virtual source — U4)

    def _handle_for(self, scope: _Scope) -> None:
        """`FOR var AS query DO body END FOR`:
        - Hoist `CREATE OR REPLACE TEMPORARY VIEW __for_<name>__ AS <query>` so the
          synthetic source flows through resolve_temp_views to the real underlying tables.
        - Push a (var, synthetic) rewrite onto the stack so qualified `var.col`
          references inside the body are rewritten to `__for_<name>__.col` at hoist time.
        - Walk the body once (no loop semantics — flat data-flow projection).
        - Pop the rewrite when leaving the FOR scope.

        `FOR query DO` (no var) is supported but body refs use unqualified columns —
        documented v1 limitation; no body rewrite is registered.
        """
        for_tok = self._peek()
        self._advance()  # FOR

        # Try to parse `var AS` prefix. If not present, treat as anonymous FOR.
        var_name: str | None = None
        first = self._peek()
        if first is not None and first.token_type == tokens.TokenType.VAR:
            # Cursor-variable form requires AS following the var name
            if self._peek(1) is not None and (
                self._peek(1).token_type == tokens.TokenType.ALIAS
                or _kw(self._peek(1), "AS")
            ):
                var_name = first.text
                self._advance()  # var
                self._advance()  # AS
            elif self._peek(1) is not None and _kw(self._peek(1), "AS"):
                var_name = first.text
                self._advance()
                self._advance()
        # Some Databricks tokenizations expose `row` as ROW token, not VAR
        elif first is not None and first.token_type == tokens.TokenType.ROW:
            if self._peek(1) is not None and (
                self._peek(1).token_type == tokens.TokenType.ALIAS
                or _kw(self._peek(1), "AS")
            ):
                var_name = first.text
                self._advance()
                self._advance()

        # Synthetic name: label > var > 'cursor'
        label = self._next_label_label_for_label
        base_id = label or var_name or "cursor"
        synthetic = self._next_synthetic_name(f"__for_{base_id}__")

        # Cursor query starts here, runs until DO (at depth 0)
        query_start = self._peek()
        if query_start is None:
            raise _UnbalancedBlock(f"FOR at offset {for_tok.start} missing query")
        query_end = query_start
        depth = 0
        while not self._eof():
            tok = self._peek()
            if tok.token_type == tokens.TokenType.L_PAREN:
                depth += 1
            elif tok.token_type == tokens.TokenType.R_PAREN:
                depth -= 1
            if depth == 0 and _kw(tok, "DO"):
                break
            query_end = tok
            self._advance()
        if self._eof():
            raise _UnbalancedBlock(f"FOR at offset {for_tok.start} missing DO")
        # Consume DO
        self._advance()

        cursor_query = self._slice(query_start, query_end).strip()
        self.hoisted.append(
            f"CREATE OR REPLACE TEMPORARY VIEW {synthetic} AS {cursor_query}"
        )
        self.virtual_sources.append(VirtualSource(name=synthetic, body_sql=cursor_query))

        if var_name:
            self._for_rewrites.append((var_name.lower(), synthetic))
        try:
            while not self._eof():
                tok = self._peek()
                if _is_semicolon(tok):
                    self._advance()
                    continue
                if _kw(tok, "END"):
                    self._advance()
                    if not self._eof() and _kw(self._peek(), "FOR"):
                        self._advance()
                    # Optional trailing label
                    if not self._eof() and self._peek().token_type == tokens.TokenType.VAR:
                        self._advance()
                    return
                self._dispatch_statement(scope)
        finally:
            if var_name:
                self._for_rewrites.pop()
        raise _UnbalancedBlock(f"unclosed FOR at offset {for_tok.start}")

    def _next_synthetic_name(self, base: str) -> str:
        if base not in self._synthetic_names:
            self._synthetic_names.add(base)
            return base
        n = 2
        # base looks like __for_X__ — insert _N before the trailing __
        while True:
            if base.endswith("__"):
                candidate = f"{base[:-2]}_{n}__"
            else:
                candidate = f"{base}_{n}"
            if candidate not in self._synthetic_names:
                self._synthetic_names.add(candidate)
                return candidate
            n += 1

    # ---- DECLARE ---------------------------------------------------------

    def _handle_declare(self, scope: _Scope) -> None:
        self._advance()  # DECLARE
        # Two forms we care about:
        #   DECLARE name [, name2, ...] type [DEFAULT literal_expr]
        #   DECLARE name CONDITION FOR ...
        #   DECLARE [type] HANDLER FOR ... action
        # Look ahead to disambiguate.
        # Collect identifiers until we hit a keyword or a non-VAR token.
        names: list[str] = []
        # Special handler-style first-token check: DECLARE EXIT/CONTINUE/UNDO HANDLER
        first = self._peek()
        if first is not None and _kw(first, "EXIT", "CONTINUE", "UNDO"):
            self._advance()
            if not self._eof() and _kw(self._peek(), "HANDLER"):
                self._handle_handler_body(scope)
                return
            # Not a handler — fall through to consume to terminator
            self._consume_until_terminator()
            return
        # DECLARE HANDLER without modifier
        if first is not None and _kw(first, "HANDLER"):
            self._handle_handler_body(scope)
            return

        # Collect comma-separated identifier list
        while not self._eof():
            tok = self._peek()
            if tok.token_type == tokens.TokenType.VAR:
                # Detect CONDITION right after the first identifier
                names.append(tok.text)
                self._advance()
                if not self._eof() and self._peek().token_type == tokens.TokenType.COMMA:
                    self._advance()
                    continue
                break
            break
        # CONDITION declaration
        nxt = self._peek()
        if nxt is not None and _kw(nxt, "CONDITION"):
            self._consume_until_terminator()
            return
        # Skip the type — advance until DEFAULT or ';'
        had_default = False
        default_tokens: list = []
        while not self._eof():
            tok = self._peek()
            if _is_semicolon(tok):
                break
            if _kw(tok, "DEFAULT"):
                self._advance()
                had_default = True
                # Collect the default expression tokens until ';'
                while not self._eof():
                    t = self._peek()
                    if _is_semicolon(t):
                        break
                    default_tokens.append(t)
                    self._advance()
                break
            self._advance()
        if had_default and default_tokens:
            literal = _fold_literal_expression(default_tokens, scope)
            if literal is not None:
                for name in names:
                    scope.bind(name, literal)

    def _handle_handler_body(self, scope: _Scope) -> None:
        """DECLARE [EXIT|CONTINUE|UNDO] HANDLER FOR <cond_list> <action>.

        Consume the `HANDLER FOR <cond_list>` prefix, then walk the action body. The
        action can be a single statement or a BEGIN…END compound — dispatch normally.
        """
        # Already past EXIT/CONTINUE/UNDO; expect HANDLER
        if not self._eof() and _kw(self._peek(), "HANDLER"):
            self._advance()
        # Skip FOR <conditions...> until first non-condition keyword
        if not self._eof() and _kw(self._peek(), "FOR"):
            self._advance()
            # Conditions: SQLEXCEPTION / SQLWARNING / NOT FOUND / SQLSTATE 'xxxxx' /
            # named condition. They're separated by commas. Stop at the first token
            # that isn't a condition keyword/value/comma.
            while not self._eof():
                tok = self._peek()
                if (
                    _kw(tok, "SQLEXCEPTION", "SQLWARNING", "NOT", "FOUND", "SQLSTATE")
                    or tok.token_type == tokens.TokenType.STRING
                    or tok.token_type == tokens.TokenType.COMMA
                    or tok.token_type == tokens.TokenType.VAR
                ):
                    self._advance()
                    continue
                break
        # Now walk the action body
        self._dispatch_statement(scope)

    # ---- SET -------------------------------------------------------------

    def _handle_set(self, scope: _Scope) -> None:
        self._advance()  # SET (or SET VAR after first hop)
        # Optional VAR keyword
        if not self._eof() and _kw(self._peek(), "VAR"):
            self._advance()
        # Two forms:
        #   SET name = expr
        #   SET (a, b, ...) = (e1, e2, ...)
        tok = self._peek()
        if tok is None:
            return
        if tok.token_type == tokens.TokenType.L_PAREN:
            # Multi-target set — collect names, then (e1, e2, ...) on RHS
            self._advance()  # (
            target_names: list[str] = []
            while not self._eof():
                t = self._peek()
                if t.token_type == tokens.TokenType.R_PAREN:
                    self._advance()
                    break
                if t.token_type == tokens.TokenType.VAR:
                    target_names.append(t.text)
                self._advance()
            # Skip until '='
            self._consume_until_kw_or_token("=", token_type=tokens.TokenType.EQ)
            if not self._eof() and self._peek().token_type == tokens.TokenType.EQ:
                self._advance()
            # RHS — for now, conservatively clear all bindings; literal-tuple folding
            # is a v1 nicety we can add if customer fixtures need it.
            for name in target_names:
                scope.unbind(name)
            self._consume_until_terminator()
            return
        if tok.token_type == tokens.TokenType.VAR:
            target_name = tok.text
            self._advance()
            # Expect '='
            if not self._eof() and self._peek().token_type == tokens.TokenType.EQ:
                self._advance()
            # Collect RHS until ';'
            rhs_tokens: list = []
            depth = 0
            while not self._eof():
                t = self._peek()
                if t.token_type == tokens.TokenType.L_PAREN:
                    depth += 1
                elif t.token_type == tokens.TokenType.R_PAREN:
                    depth -= 1
                if depth == 0 and _is_semicolon(t):
                    break
                rhs_tokens.append(t)
                self._advance()
            literal = _fold_literal_expression(rhs_tokens, scope)
            if literal is not None:
                scope.bind(target_name, literal)
            else:
                scope.unbind(target_name)
            return
        # Unknown form — skip to terminator
        self._consume_until_terminator()

    def _consume_until_kw_or_token(self, *_kws: str, token_type=None) -> None:
        while not self._eof():
            tok = self._peek()
            if token_type is not None and tok.token_type == token_type:
                return
            if _is_semicolon(tok):
                return
            self._advance()

    # ---- Hoist ----------------------------------------------------------

    def _hoist_statement(self) -> None:
        """Capture the raw text of one statement (until ';' or end-of-block) and
        append it to self.hoisted.

        Tracks paren depth AND CASE-expression depth so embedded `CASE WHEN ... END`
        expressions inside a SELECT do not prematurely close the hoisted statement.
        """
        start_tok = self._peek()
        paren_depth = 0
        case_depth = 0
        end_tok = start_tok
        while not self._eof():
            tok = self._peek()
            if tok.token_type == tokens.TokenType.L_PAREN:
                paren_depth += 1
            elif tok.token_type == tokens.TokenType.R_PAREN:
                paren_depth -= 1
            elif _kw(tok, "CASE"):
                case_depth += 1
            elif _kw(tok, "END") and case_depth > 0:
                case_depth -= 1
                end_tok = tok
                self._advance()
                continue
            if paren_depth == 0 and case_depth == 0:
                if _is_semicolon(tok):
                    break
                # Stop at block terminators we shouldn't consume past. _kw filters
                # out STRING tokens so a literal `'END'` inside a SELECT does not
                # terminate the hoisted statement.
                if (
                    tok is not start_tok
                    and _kw(tok, "END", "ELSEIF", "ELSE", "WHEN", "UNTIL")
                ):
                    break
            end_tok = tok
            self._advance()
        text = self._slice(start_tok, end_tok).strip()
        if text:
            text = self._apply_for_rewrites(text)
            self.hoisted.append(text)

    def _apply_for_rewrites(self, text: str) -> str:
        """Rewrite qualified `var.col` references to `__for_<id>__.col` for any
        active FOR-cursor variables on the rewrite stack.

        Case-insensitive match, word-boundary anchored on the variable name. Inner-
        most rewrites apply first so nested FOR loops resolve correctly when the
        outer cursor's variable would otherwise shadow the inner.

        SQL/PSM cursor bodies often have no FROM clause (the cursor variable is
        implicit). When we rewrite `var.col` -> `synthetic.col` and the resulting
        statement has no FROM, inject `FROM <synthetic>` so SQLGlot can resolve
        the column attribution. If a FROM clause exists, leave it alone.
        """
        injected_synthetics: list[str] = []
        for var, synthetic in reversed(self._for_rewrites):
            pattern = _re.compile(r"\b" + _re.escape(var) + r"\.", _re.IGNORECASE)
            new_text, n_subs = pattern.subn(f"{synthetic}.", text)
            text = new_text
            if n_subs > 0:
                injected_synthetics.append(synthetic)
        if injected_synthetics and not _re.search(r"\bfrom\b", text, _re.IGNORECASE):
            text = text.rstrip() + " FROM " + injected_synthetics[0]
        return text


class _UnbalancedBlock(Exception):
    """Raised when the walker detects an unclosed or mismatched block."""


# ---------------------------------------------------------------------------
# Literal folder
# ---------------------------------------------------------------------------


def _fold_literal_expression(toks: list, scope: _Scope, *, depth: int = 0) -> str | None:
    """Fold a token list into a literal SQL expression.

    Supported:
    - String literal:        STRING(text='foo')                  → "'foo'"
    - Numeric literal:       NUMBER(text='42')                   → "42"
    - NULL / TRUE / FALSE:                                       → "NULL" / "TRUE" / "FALSE"
    - Bound variable:        VAR(text='x') with scope binding    → bound literal
    - Concatenation chain:   a || b || c (each foldable)         → concatenated

    Returns None if any operand is not foldable (function calls, subqueries,
    arithmetic, unbound variables, etc.).
    """
    # Strip leading/trailing whitespace tokens (none come from SQLGlot tokenizer)
    if not toks:
        return None
    if depth > 4:
        return None  # bound recursion depth

    # Split on top-level DPIPE (||); concat-chain
    parts = _split_on_top_level_dpipe(toks)
    if len(parts) == 1:
        return _fold_single_operand(parts[0], scope, depth=depth)
    folded_parts: list[str] = []
    for p in parts:
        v = _fold_single_operand(p, scope, depth=depth)
        if v is None:
            return None
        # Strip surrounding quotes from string literals before concatenation
        if v.startswith("'") and v.endswith("'") and len(v) >= 2:
            folded_parts.append(v[1:-1])
        else:
            return None  # Only concat string literals
    return "'" + "".join(folded_parts) + "'"


def _split_on_top_level_dpipe(toks: list) -> list[list]:
    parts: list[list] = []
    current: list = []
    depth = 0
    for t in toks:
        if t.token_type == tokens.TokenType.L_PAREN:
            depth += 1
            current.append(t)
            continue
        if t.token_type == tokens.TokenType.R_PAREN:
            depth -= 1
            current.append(t)
            continue
        if depth == 0 and t.token_type == tokens.TokenType.DPIPE:
            parts.append(current)
            current = []
            continue
        current.append(t)
    parts.append(current)
    return parts


def _fold_single_operand(toks: list, scope: _Scope, *, depth: int) -> str | None:
    if not toks:
        return None
    if len(toks) == 1:
        t = toks[0]
        if t.token_type == tokens.TokenType.STRING:
            return f"'{t.text}'"
        if t.token_type == tokens.TokenType.NUMBER:
            return t.text
        if _kw(t, "NULL"):
            return "NULL"
        if _kw(t, "TRUE", "FALSE"):
            return t.text.upper()
        if t.token_type == tokens.TokenType.VAR:
            looked = scope.lookup(t.text)
            if looked is not None:
                return looked
            return None
        return None
    # Multi-token operand: only foldable case in v1 is parenthesised expr — strip parens and recurse
    if (
        toks[0].token_type == tokens.TokenType.L_PAREN
        and toks[-1].token_type == tokens.TokenType.R_PAREN
    ):
        return _fold_literal_expression(toks[1:-1], scope, depth=depth + 1)
    return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def normalize_script(sql: str) -> tuple[str, list[VirtualSource], dict[str, str]]:
    """Pre-process Databricks SQL/PSM into flat DML.

    Returns:
        flattened_sql      — SQL safe for _split_top_level_statements; identical to
                             input when no procedural keywords are present, or when
                             the walker fails to balance blocks (graceful degradation).
        virtual_sources    — list of VirtualSource entries the engine should hoist
                             as synthetic temp views (empty in U2; populated in U4).
        bindings           — top-scope variable bindings the EXECUTE IMMEDIATE folder
                             (U6) consults to resolve `||` chains over DECLAREd literals.
    """
    if not _has_procedural_keyword(sql):
        return sql, [], {}
    toks = _tokens(sql)
    if not toks:
        return sql, [], {}
    walker = _Walker(sql, toks)
    walker.walk_top_level()
    if walker.failed or not walker.hoisted:
        # Graceful degradation: return input unchanged so parse_sql falls through.
        return sql, [], {}
    return ";\n".join(walker.hoisted) + ";", walker.virtual_sources, walker.top_scope.bindings
