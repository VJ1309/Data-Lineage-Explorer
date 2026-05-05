"""Tests for parsers/sql_script.py — block normaliser for Databricks SQL scripting.

Drives U2 of the SQL scripting coverage plan: block tokeniser + control-flow
walker + variable bindings. Test-first per the plan's Execution note.
"""
from __future__ import annotations

import pytest

from parsers.sql_script import VirtualSource, normalize_script


def _flat(sql: str) -> str:
    """Run normalize_script and return only the flattened SQL (no virtual sources)."""
    return normalize_script(sql)[0]


def _stmts(sql: str) -> list[str]:
    """Split the flattened SQL into stripped statements."""
    return [s.strip() for s in _flat(sql).split(";") if s.strip()]


# ---------------------------------------------------------------------------
# Pre-check: non-procedural input passes through unchanged
# ---------------------------------------------------------------------------


def test_non_procedural_passes_through_unchanged():
    sql = "SELECT * FROM t"
    flat, virt, bindings = normalize_script(sql)
    assert flat == sql
    assert virt == []
    assert bindings == {}


def test_pure_dml_with_semicolons_passes_through_unchanged():
    sql = "INSERT INTO t SELECT a FROM s; INSERT INTO u SELECT b FROM s;"
    flat, _, _ = normalize_script(sql)
    assert flat == sql


# ---------------------------------------------------------------------------
# BEGIN ... END (compound blocks)
# ---------------------------------------------------------------------------


def test_minimal_begin_end_hoists_inner_dml():
    sql = "BEGIN INSERT INTO t SELECT a FROM s; END"
    stmts = _stmts(sql)
    assert stmts == ["INSERT INTO t SELECT a FROM s"]


def test_begin_atomic_recognised_and_skipped():
    sql = "BEGIN ATOMIC INSERT INTO t SELECT a FROM s; END"
    stmts = _stmts(sql)
    assert stmts == ["INSERT INTO t SELECT a FROM s"]


def test_labelled_begin_block():
    sql = "proc: BEGIN INSERT INTO t SELECT a FROM s; END proc"
    stmts = _stmts(sql)
    assert stmts == ["INSERT INTO t SELECT a FROM s"]


def test_nested_begin_end_blocks_hoisted_flat():
    sql = """
    BEGIN
      INSERT INTO t1 SELECT a FROM s1;
      BEGIN
        INSERT INTO t2 SELECT b FROM s2;
        BEGIN
          INSERT INTO t3 SELECT c FROM s3;
        END;
      END;
    END
    """
    stmts = _stmts(sql)
    assert "INSERT INTO t1 SELECT a FROM s1" in stmts
    assert "INSERT INTO t2 SELECT b FROM s2" in stmts
    assert "INSERT INTO t3 SELECT c FROM s3" in stmts


# ---------------------------------------------------------------------------
# Tokenizer-shared handling: comments and strings containing END
# ---------------------------------------------------------------------------


def test_comments_inside_block_do_not_break_walking():
    sql = """BEGIN
      /* this END is in a comment */
      INSERT INTO t SELECT a FROM s;
      -- trailing END in a line comment too
    END"""
    stmts = _stmts(sql)
    assert any("INSERT INTO t SELECT a FROM s" in s for s in stmts)


def test_string_literal_containing_end_does_not_close_block():
    sql = "BEGIN INSERT INTO t SELECT 'END' AS lit FROM s; END"
    stmts = _stmts(sql)
    assert any("INSERT INTO t" in s and "'END'" in s for s in stmts)


# ---------------------------------------------------------------------------
# DECLARE bindings
# ---------------------------------------------------------------------------


def test_declare_with_string_default_binds_literal():
    sql = "BEGIN DECLARE x STRING DEFAULT 'hello'; INSERT INTO t SELECT a FROM s; END"
    _, _, bindings = normalize_script(sql)
    assert bindings.get("x") == "'hello'"


def test_declare_without_default_does_not_bind():
    sql = "BEGIN DECLARE x STRING; INSERT INTO t SELECT a FROM s; END"
    _, _, bindings = normalize_script(sql)
    assert "x" not in bindings


def test_multi_variable_declare_binds_all():
    sql = "BEGIN DECLARE x, y, z STRING DEFAULT 'a'; INSERT INTO t SELECT a FROM s; END"
    _, _, bindings = normalize_script(sql)
    assert bindings.get("x") == "'a'"
    assert bindings.get("y") == "'a'"
    assert bindings.get("z") == "'a'"


def test_declare_concat_chain_over_bound_var():
    sql = (
        "BEGIN DECLARE p STRING DEFAULT 'foo'; "
        "DECLARE q STRING DEFAULT p || '_bar'; "
        "INSERT INTO t SELECT a FROM s; END"
    )
    _, _, bindings = normalize_script(sql)
    assert bindings.get("p") == "'foo'"
    assert bindings.get("q") == "'foo_bar'"


def test_declare_condition_recognised_and_skipped():
    sql = "BEGIN DECLARE bad_state CONDITION FOR SQLSTATE '42000'; INSERT INTO t SELECT a FROM s; END"
    stmts = _stmts(sql)
    assert any("INSERT INTO t" in s for s in stmts)


def test_declare_handler_action_walked_for_dml():
    sql = (
        "BEGIN "
        "DECLARE EXIT HANDLER FOR SQLEXCEPTION "
        "BEGIN INSERT INTO err_log SELECT 1 AS code FROM dual; END; "
        "INSERT INTO main_target SELECT a FROM s; "
        "END"
    )
    stmts = _stmts(sql)
    assert any("INSERT INTO err_log" in s for s in stmts)
    assert any("INSERT INTO main_target" in s for s in stmts)


# ---------------------------------------------------------------------------
# SET / SET VAR / multi-target SET
# ---------------------------------------------------------------------------


def test_set_with_literal_rhs_binds():
    sql = "BEGIN DECLARE x STRING DEFAULT 'old'; SET x = 'new'; INSERT INTO t SELECT a FROM s; END"
    _, _, bindings = normalize_script(sql)
    assert bindings.get("x") == "'new'"


def test_set_with_non_literal_rhs_clears_binding():
    sql = (
        "BEGIN DECLARE total INT DEFAULT 0; "
        "SET total = (SELECT SUM(x) FROM t); "
        "INSERT INTO u SELECT a FROM s; END"
    )
    _, _, bindings = normalize_script(sql)
    assert "total" not in bindings


def test_set_var_alias_works():
    sql = "BEGIN DECLARE x STRING DEFAULT 'a'; SET VAR x = 'b'; INSERT INTO t SELECT a FROM s; END"
    _, _, bindings = normalize_script(sql)
    assert bindings.get("x") == "'b'"


# ---------------------------------------------------------------------------
# IF / ELSEIF / ELSE / END IF — every branch walked unconditionally
# ---------------------------------------------------------------------------


def test_if_elseif_else_with_dml_in_each_branch():
    sql = """
    BEGIN
      IF flag = 1 THEN
        INSERT INTO branch_a SELECT a FROM s;
      ELSEIF flag = 2 THEN
        INSERT INTO branch_b SELECT a FROM s;
      ELSE
        INSERT INTO branch_c SELECT a FROM s;
      END IF;
    END
    """
    stmts = _stmts(sql)
    assert any("INSERT INTO branch_a" in s for s in stmts)
    assert any("INSERT INTO branch_b" in s for s in stmts)
    assert any("INSERT INTO branch_c" in s for s in stmts)


def test_if_with_no_else_branch():
    sql = """
    BEGIN
      IF flag = 1 THEN
        INSERT INTO t SELECT a FROM s;
      END IF;
    END
    """
    stmts = _stmts(sql)
    assert any("INSERT INTO t" in s for s in stmts)


# ---------------------------------------------------------------------------
# CASE statement-form vs CASE expression
# ---------------------------------------------------------------------------


def test_case_statement_form_walks_all_branches():
    sql = """
    BEGIN
      CASE flag
        WHEN 1 THEN INSERT INTO b1 SELECT a FROM s;
        WHEN 2 THEN INSERT INTO b2 SELECT a FROM s;
        ELSE INSERT INTO b3 SELECT a FROM s;
      END CASE;
    END
    """
    stmts = _stmts(sql)
    assert any("INSERT INTO b1" in s for s in stmts)
    assert any("INSERT INTO b2" in s for s in stmts)
    assert any("INSERT INTO b3" in s for s in stmts)


def test_case_expression_form_passes_through_unchanged():
    """CASE WHEN ... THEN ... END inside SELECT is an expression, not a statement.
    The walker must NOT confuse it with the statement form (no 'END CASE')."""
    sql = (
        "BEGIN "
        "INSERT INTO t SELECT CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END AS sign FROM s; "
        "END"
    )
    stmts = _stmts(sql)
    # The CASE expression survives intact inside the hoisted statement.
    assert any("CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END" in s for s in stmts)


# ---------------------------------------------------------------------------
# WHILE / LOOP / REPEAT
# ---------------------------------------------------------------------------


def test_while_loop_body_walked_once():
    sql = """
    BEGIN
      WHILE i < 10 DO
        INSERT INTO log SELECT i FROM dual;
      END WHILE;
    END
    """
    stmts = _stmts(sql)
    inserts = [s for s in stmts if "INSERT INTO log" in s]
    assert len(inserts) == 1


def test_loop_body_walked_once():
    sql = """
    BEGIN
      LOOP
        INSERT INTO log SELECT 1 FROM dual;
        LEAVE;
      END LOOP;
    END
    """
    stmts = _stmts(sql)
    inserts = [s for s in stmts if "INSERT INTO log" in s]
    assert len(inserts) == 1


def test_repeat_until_body_walked_once():
    sql = """
    BEGIN
      REPEAT
        INSERT INTO log SELECT 1 FROM dual;
      UNTIL i > 10 END REPEAT;
    END
    """
    stmts = _stmts(sql)
    inserts = [s for s in stmts if "INSERT INTO log" in s]
    assert len(inserts) == 1


# ---------------------------------------------------------------------------
# Control-flow no-ops: SIGNAL / RESIGNAL / LEAVE / ITERATE
# ---------------------------------------------------------------------------


def test_signal_resignal_leave_iterate_skipped():
    sql = """
    BEGIN
      LOOP
        SIGNAL SQLSTATE '01000';
        RESIGNAL;
        LEAVE;
        ITERATE;
        INSERT INTO t SELECT a FROM s;
      END LOOP;
    END
    """
    stmts = _stmts(sql)
    assert any("INSERT INTO t" in s for s in stmts)
    # No SIGNAL / RESIGNAL / LEAVE / ITERATE statements survive
    for kw in ("SIGNAL", "RESIGNAL", "LEAVE", "ITERATE"):
        assert not any(s.startswith(kw) for s in stmts), f"{kw} leaked: {stmts}"


# ---------------------------------------------------------------------------
# Error paths: unbalanced blocks
# ---------------------------------------------------------------------------


def test_unclosed_begin_returns_input_unchanged():
    sql = "BEGIN INSERT INTO t SELECT a FROM s;"
    flat, _, _ = normalize_script(sql)
    # Graceful degradation: input returned unchanged so parse_sql can fall through
    # to its existing per-statement parse-error path.
    assert flat == sql


def test_end_without_matching_begin_returns_input_unchanged():
    sql = "INSERT INTO t SELECT a FROM s; END"
    flat, _, _ = normalize_script(sql)
    assert flat == sql


# ---------------------------------------------------------------------------
# VirtualSource list (empty in U2; populated in U4)
# ---------------------------------------------------------------------------


def test_virtual_sources_empty_in_u2():
    sql = "BEGIN INSERT INTO t SELECT a FROM s; END"
    _, virt, _ = normalize_script(sql)
    assert virt == []
