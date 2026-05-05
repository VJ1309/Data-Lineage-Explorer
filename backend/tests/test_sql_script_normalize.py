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


# ---------------------------------------------------------------------------
# FOR cursor virtual sources (U4)
# ---------------------------------------------------------------------------


def test_for_cursor_emits_temp_view_and_rewrites_body():
    """FOR row AS query DO body END FOR — the cursor query becomes a synthetic
    temp view; qualified references to `row.col` inside the body are rewritten
    to `__for_row__.col` before hoisting."""
    sql = """
    BEGIN
      FOR row AS SELECT order_id, amount FROM orders DO
        INSERT INTO summary SELECT row.order_id, row.amount;
      END FOR;
    END
    """
    flat, _, _ = normalize_script(sql)
    # Synthetic temp view defined from cursor query
    assert "__for_row__" in flat
    assert "CREATE OR REPLACE TEMPORARY VIEW __for_row__" in flat
    assert "SELECT order_id, amount FROM orders" in flat
    # Body's row.X references rewritten to __for_row__.X
    assert "__for_row__.order_id" in flat
    assert "__for_row__.amount" in flat
    # Original `row.` references must not survive
    assert "row.order_id" not in flat
    assert "row.amount" not in flat


def test_for_cursor_labelled_uses_label_for_synthetic_name():
    sql = """
    BEGIN
      process_orders: FOR row AS SELECT id FROM orders DO
        INSERT INTO out SELECT row.id;
      END FOR process_orders;
    END
    """
    flat, _, _ = normalize_script(sql)
    assert "__for_process_orders__" in flat
    assert "__for_process_orders__.id" in flat


def test_for_cursor_nested_loops_each_get_own_synthetic():
    sql = """
    BEGIN
      FOR outer_row AS SELECT order_id FROM orders DO
        FOR inner_row AS SELECT line_id FROM order_lines WHERE order_id = outer_row.order_id DO
          INSERT INTO log SELECT outer_row.order_id, inner_row.line_id;
        END FOR;
      END FOR;
    END
    """
    flat, _, _ = normalize_script(sql)
    assert "__for_outer_row__" in flat
    assert "__for_inner_row__" in flat
    # Each cursor's body references resolve to its own synthetic
    assert "__for_outer_row__.order_id" in flat
    assert "__for_inner_row__.line_id" in flat


def test_for_cursor_unique_names_on_collision():
    """Two FOR loops with the same variable name get unique synthetic names."""
    sql = """
    BEGIN
      FOR row AS SELECT a FROM s1 DO
        INSERT INTO t1 SELECT row.a;
      END FOR;
      FOR row AS SELECT b FROM s2 DO
        INSERT INTO t2 SELECT row.b;
      END FOR;
    END
    """
    flat, _, _ = normalize_script(sql)
    assert "__for_row__" in flat
    assert "__for_row_2__" in flat


def test_for_cursor_leave_inside_body_skipped():
    """LEAVE <label> inside a FOR body is recognised and skipped; body DML still hoisted."""
    sql = """
    BEGIN
      process: FOR row AS SELECT a FROM s DO
        INSERT INTO t SELECT row.a;
        LEAVE process;
      END FOR process;
    END
    """
    flat, _, _ = normalize_script(sql)
    assert "INSERT INTO t" in flat
    assert "LEAVE" not in flat.upper()


def test_for_keyword_inside_select_not_treated_as_loop():
    """A bare INSERT must not have its `FOR` (e.g. in `FOR UPDATE`, or column-list
    contexts) confused with a loop — only block-level `FOR var AS query DO`
    triggers the cursor path."""
    sql = "BEGIN INSERT INTO t SELECT a FROM s; END"
    flat, _, _ = normalize_script(sql)
    assert "__for_" not in flat


# ---------------------------------------------------------------------------
# CREATE PROCEDURE / CALL / DROP / DESCRIBE / SHOW PROCEDURE (U5)
# ---------------------------------------------------------------------------


def test_create_procedure_strips_wrapper_and_hoists_body():
    sql = """
    CREATE PROCEDURE p()
    LANGUAGE SQL
    AS BEGIN
      INSERT INTO t SELECT a FROM s;
    END
    """
    flat, _, _ = normalize_script(sql)
    # Wrapper gone, body hoisted
    assert "CREATE PROCEDURE" not in flat
    assert "LANGUAGE SQL" not in flat
    assert "INSERT INTO t SELECT a FROM s" in flat


def test_create_or_replace_procedure_strips_with_full_qualifier():
    sql = """
    CREATE OR REPLACE PROCEDURE my_catalog.my_schema.run_etl()
    LANGUAGE SQL SQL SECURITY INVOKER COMMENT 'runs etl' NOT DETERMINISTIC
    AS BEGIN
      INSERT INTO logs SELECT 1 AS x FROM dual;
    END
    """
    flat, _, _ = normalize_script(sql)
    assert "CREATE OR REPLACE PROCEDURE" not in flat
    assert "INSERT INTO logs" in flat


def test_create_procedure_with_param_modes_and_defaults():
    sql = """
    CREATE PROCEDURE p(IN n INT, OUT result STRING, INOUT counter BIGINT DEFAULT 0)
    LANGUAGE SQL
    AS BEGIN
      INSERT INTO logs SELECT a FROM s;
    END
    """
    flat, _, _ = normalize_script(sql)
    assert "INSERT INTO logs" in flat
    # Parameter list should be entirely stripped — no IN/OUT/INOUT survives
    for kw in ("IN n", "OUT result", "INOUT counter"):
        assert kw not in flat


def test_create_procedure_registers_qualified_name():
    from parsers.sql_script import _PROCEDURE_REGISTRY
    sql = """
    CREATE OR REPLACE PROCEDURE my_catalog.my_schema.proc_a()
    LANGUAGE SQL
    AS BEGIN
      INSERT INTO t SELECT 1 AS x FROM dual;
    END
    """
    normalize_script(sql)
    assert "my_catalog.my_schema.proc_a" in _PROCEDURE_REGISTRY
    body = _PROCEDURE_REGISTRY["my_catalog.my_schema.proc_a"]
    assert "INSERT INTO t" in body


def test_call_emits_placeholder_marker():
    """CALL <proc>(args) becomes a placeholder INSERT/SELECT through __call_<proc>__
    so the parser emits an approximate wildcard edge."""
    sql = "CALL my_catalog.my_schema.run_etl('raw', 'silver')"
    flat, _, _ = normalize_script(sql)
    # Synthetic name uses sanitised proc name (dots -> underscores)
    assert "__call_my_catalog_my_schema_run_etl__" in flat


def test_call_with_named_parameters():
    sql = "CALL run_etl(target_schema => 'silver', source_schema => 'raw')"
    flat, _, _ = normalize_script(sql)
    assert "__call_run_etl__" in flat


def test_drop_procedure_silently_skipped():
    sql = "DROP PROCEDURE IF EXISTS my_proc"
    flat, _, _ = normalize_script("BEGIN " + sql + "; INSERT INTO t SELECT a FROM s; END")
    assert "DROP" not in flat
    assert "INSERT INTO t" in flat


def test_describe_procedure_silently_skipped():
    sql = "BEGIN DESCRIBE PROCEDURE EXTENDED my_proc; INSERT INTO t SELECT a FROM s; END"
    flat, _, _ = normalize_script(sql)
    assert "DESCRIBE" not in flat
    assert "INSERT INTO t" in flat


def test_show_procedures_silently_skipped():
    sql = "BEGIN SHOW PROCEDURES FROM my_schema; INSERT INTO t SELECT a FROM s; END"
    flat, _, _ = normalize_script(sql)
    assert "SHOW PROCEDURES" not in flat
    assert "INSERT INTO t" in flat
