"""Comprehensive table-driven test suite for Cypher read-only guardrails.

This suite tests the CURRENT guardrail (api/guardrail.py) against a wide corpus of:
1. MUST-REJECT queries (destructive clauses, obfuscation, procedure calls, injection tricks, LIMIT violations).
2. MUST-ACCEPT queries (benign read queries, columns with keyword substrings, string literals, comments).

Any test case that the CURRENT guardrail gets wrong is marked with:
    xfail(strict=True, reason="fix in Phase 2: ...")
Gaps are categorized by root cause and exploitability status.
"""

import pytest
from guardrail import NO_QUERY, validate_read_only

# ==============================================================================
# MUST-REJECT CORPUS: Queries that MUST be rejected by a secure read-only guardrail
# ==============================================================================
MUST_REJECT_CASES = [
    # -------------------------------------------------------------------------
    # Baseline destructive write clauses (handled correctly by current regex)
    # -------------------------------------------------------------------------
    pytest.param("CREATE (n:Test {name: 'evil'})", id="reject-create-node"),
    pytest.param("MERGE (n:Test {name: 'evil'})", id="reject-merge-node"),
    pytest.param("MATCH (n:Row) DELETE n", id="reject-delete-node"),
    pytest.param("MATCH (n:Row) DETACH DELETE n", id="reject-detach-delete"),
    pytest.param("MATCH (n:Row) SET n.hacked = true", id="reject-set-property"),
    pytest.param("MATCH (n:Row) REMOVE n.amount", id="reject-remove-property"),
    pytest.param("DROP INDEX ON :Row(id)", id="reject-drop-index"),
    pytest.param("DROP CONSTRAINT unique_id IF EXISTS", id="reject-drop-constraint"),
    pytest.param("MATCH (n) FOREACH (x IN [1] | CREATE (:Test))", id="reject-foreach-create"),
    pytest.param("UNWIND [1, 2] AS x CREATE (n:Test {val: x})", id="reject-unwind-create"),
    pytest.param("OPTIONAL MATCH (r:Row) DELETE r", id="reject-optional-match-delete"),
    pytest.param("LOAD CSV FROM 'http://malicious.site/data.csv' AS row CREATE (:Row)", id="reject-load-csv"),
    pytest.param("CALL db.clearQueryCaches()", id="reject-call-db-procedure"),
    pytest.param("CALL db.indexes()", id="reject-call-db-indexes"),
    pytest.param("MATCH (n)\n\tCALL\tdb.clearQueryCaches()", id="reject-call-db-tab-newline"),

    # -------------------------------------------------------------------------
    # Obfuscation: mixed case, whitespace, newlines, tabs
    # -------------------------------------------------------------------------
    pytest.param("cReAtE (n:Node)", id="reject-mixed-case-create"),
    pytest.param("mErGe (n:Node)", id="reject-mixed-case-merge"),
    pytest.param("dElEtE n", id="reject-mixed-case-delete"),
    pytest.param("MATCH (n)\n\tCREATE (m)", id="reject-newline-tab-create"),
    pytest.param("MATCH (r:Row) CREATE (n:Test)", id="reject-match-then-create"),
    pytest.param("MATCH (r:Row) CALL { WITH r CREATE (n:Test) } RETURN count(*)", id="reject-subquery-call-create"),
    pytest.param("", id="reject-empty-query"),
    pytest.param(NO_QUERY, id="reject-no-query-token"),
    pytest.param("EXPLAIN MATCH (n) RETURN n", id="reject-non-read-start-explain"),
    pytest.param("PROFILE MATCH (n) RETURN n", id="reject-non-read-start-profile"),

    # -------------------------------------------------------------------------
    # Root Cause: Denylist, not allowlist (FAIL-OPEN, exploitable: unverified (Step 6))
    # -------------------------------------------------------------------------
    pytest.param(
        "MATCH (r:Row) CALL apoc.refactor.renameLabel('Row', 'OldRow', [r]) RETURN count(*)",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: CALL apoc.* procedures not blocked by CALL db. regex (FAIL-OPEN; exploitable: unverified (Step 6))",
        ),
        id="reject-apoc-call-mutation",
    ),
    pytest.param(
        "MATCH (r:Row) CALL apoc.schema.assert({}, {}) RETURN count(*)",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: CALL apoc.* schema procedures not blocked (FAIL-OPEN; exploitable: unverified (Step 6))",
        ),
        id="reject-apoc-call-schema",
    ),
    pytest.param(
        "MATCH (n)\n\tCALL\tapoc.refactor.renameLabel('A', 'B', [n]) RETURN count(*)",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: CALL apoc.* with tabs/newlines not blocked (FAIL-OPEN; exploitable: unverified (Step 6))",
        ),
        id="reject-apoc-tab-newline",
    ),
    pytest.param(
        "MATCH (r:Row) CALL dbms.security.createUser('attacker', 'pass', false) RETURN r",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: CALL dbms.* procedures not blocked by CALL db. regex (FAIL-OPEN; exploitable: unverified (Step 6))",
        ),
        id="reject-dbms-call-security",
    ),
    pytest.param(
        "MATCH (r:Row) RETURN count(r); CALL dbms.security.createUser('attacker', 'pass', false)",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: multi-statement query with semicolon not blocked (FAIL-OPEN; exploitable: unverified (Step 6))",
        ),
        id="reject-semicolon-dbms-injection",
    ),
    pytest.param(
        "MATCH (r:Row) RETURN count(r); RETURN 1",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: multi-statement query with semicolon not blocked (FAIL-OPEN; exploitable: unverified (Step 6))",
        ),
        id="reject-semicolon-piggyback",
    ),

    # -------------------------------------------------------------------------
    # Root Cause: No lexing (FAIL-OPEN, exploitable: unverified (Step 6))
    # -------------------------------------------------------------------------
    pytest.param(
        "MATCH (r:Row) LOAD/*trick*/CSV FROM 'http://malicious.site/data.csv' AS row RETURN row",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: inline comment breaks LOAD\\s+CSV regex (FAIL-OPEN; exploitable: unverified (Step 6))",
        ),
        id="reject-load-csv-comment-obfuscation",
    ),

    # -------------------------------------------------------------------------
    # Root Cause: No LIMIT policy (POLICY-PENDING)
    # Aggregates like count(r) legitimately have no LIMIT; policy decided in Phase 2:
    # inject LIMIT 200 when absent on row returns, reject literal > 200.
    # -------------------------------------------------------------------------
    pytest.param(
        "MATCH (r:Row) RETURN r",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: POLICY-PENDING (inject LIMIT 200 when absent on row returns; reject literal > 200)",
        ),
        id="reject-missing-limit",
    ),
    pytest.param(
        "MATCH (r:Row) RETURN r LIMIT 50000",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: POLICY-PENDING (reject literal > 200)",
        ),
        id="reject-excessive-limit",
    ),

    # -------------------------------------------------------------------------
    # Root Cause: No Unicode normalization (FAIL-OPEN: validator gap, Neo4j rejects (verified via EXPLAIN))
    # -------------------------------------------------------------------------
    pytest.param(
        "MATCH (r:Row) ＣＲＥＡＴＥ (n:Test)",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: fullwidth unicode homoglyphs bypass ASCII word boundaries (FAIL-OPEN: validator gap, Neo4j rejects (verified via EXPLAIN))",
        ),
        id="reject-unicode-fullwidth-create",
    ),
    pytest.param(
        "MATCH (r:Row) C\u200bREATE (n:Test)",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: zero-width characters break ASCII word boundaries (FAIL-OPEN: validator gap, Neo4j rejects (verified via EXPLAIN))",
        ),
        id="reject-unicode-zerowidth-create",
    ),
]


# ==============================================================================
# MUST-ACCEPT CORPUS: Benign read queries that MUST be accepted
# ==============================================================================
MUST_ACCEPT_CASES = [
    # -------------------------------------------------------------------------
    # Baseline read queries (handled correctly by current regex)
    # -------------------------------------------------------------------------
    pytest.param("MATCH (r:Row) RETURN count(r) AS count", id="accept-count-rows"),
    pytest.param("RETURN 1 AS one", id="accept-return-scalar"),
    pytest.param("OPTIONAL MATCH (r:Row) RETURN r.id LIMIT 20", id="accept-optional-match"),
    pytest.param("WITH 1 AS x RETURN x LIMIT 10", id="accept-with-clause"),
    pytest.param("UNWIND [1, 2, 3] AS x RETURN x LIMIT 10", id="accept-unwind-clause"),
    pytest.param("MATCH (r:Row) RETURN r.Offset LIMIT 10", id="accept-column-offset"),
    pytest.param("MATCH (r:Row) RETURN r.created_at LIMIT 10", id="accept-column-created-at"),
    pytest.param("MATCH (r:Row) RETURN r.Settings LIMIT 10", id="accept-column-settings"),
    pytest.param(
        "MATCH (r:Row) WHERE toFloat(r.amount) IS NOT NULL RETURN r ORDER BY toFloat(r.amount) DESC LIMIT 10",
        id="accept-filter-orderby-limit",
    ),
    pytest.param(
        "MATCH (r:Row) RETURN [k IN keys(r) WHERE NOT k IN ['row_index', 'dataset_id']] AS columns LIMIT 1",
        id="accept-schema-columns-query",
    ),
    pytest.param("MATCH (r:Row) WHERE r.text = 'a;b' RETURN r LIMIT 10", id="accept-semicolon-in-string-literal"),
    pytest.param("MATCH (r:Row) RETURN r LIMIT 200", id="accept-limit-200-boundary"),
    pytest.param("MATCH (r:Row) RETURN r LIMIT $limit", id="accept-limit-parameter"),
    pytest.param("MATCH (r:Row) RETURN r.apoc, r.call LIMIT 10", id="accept-properties-named-apoc-call"),

    # -------------------------------------------------------------------------
    # Root Cause: No lexing (FAIL-CLOSED: benign queries rejected by string/comment regex collision)
    # -------------------------------------------------------------------------
    pytest.param(
        "MATCH (r:Row) WHERE r.action = 'delete' RETURN r LIMIT 10",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: 'delete' inside string literal falsely triggers write keyword regex (FAIL-CLOSED; no lexer)",
        ),
        id="accept-string-literal-delete",
    ),
    pytest.param(
        "MATCH (r:Row) WHERE r.status = 'SET' RETURN r LIMIT 10",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: 'SET' inside string literal falsely triggers write keyword regex (FAIL-CLOSED; no lexer)",
        ),
        id="accept-string-literal-set",
    ),
    pytest.param(
        "MATCH (r:Row) WHERE r.comment = 'REMOVE BEFORE FLIGHT' RETURN r LIMIT 10",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: 'REMOVE' inside string literal falsely triggers write keyword regex (FAIL-CLOSED; no lexer)",
        ),
        id="accept-string-literal-remove",
    ),
    pytest.param(
        "MATCH (r:Row) // CREATE comment\nRETURN count(r) LIMIT 10",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: 'CREATE' inside line comment falsely triggers write keyword regex (FAIL-CLOSED; no lexer)",
        ),
        id="accept-comment-containing-keyword",
    ),
    pytest.param(
        "/* detailed analytics query */ MATCH (r:Row) RETURN count(r) LIMIT 10",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: leading block comment /* ... */ fails READ_START prefix check (FAIL-CLOSED; no lexer)",
        ),
        id="accept-leading-block-comment",
    ),
    pytest.param(
        "MATCH (r:Row) RETURN r.`SET` LIMIT 10",
        marks=pytest.mark.xfail(
            strict=True,
            reason="fix in Phase 2: backticked property `SET` falsely triggers write keyword regex (FAIL-CLOSED; no lexer)",
        ),
        id="accept-backtick-property-keyword",
    ),
]


# ==============================================================================
# Pytest Test Functions
# ==============================================================================
@pytest.mark.parametrize("query", MUST_REJECT_CASES)
def test_guardrail_must_reject(query: str):
    """Verify that potentially destructive, bypass, or malformed queries are rejected."""
    assert validate_read_only(query) is False, f"Guardrail failed open! Query was accepted: {query}"


@pytest.mark.parametrize("query", MUST_ACCEPT_CASES)
def test_guardrail_must_accept(query: str):
    """Verify that safe, legitimate read queries are accepted."""
    assert validate_read_only(query) is True, f"Guardrail failed closed! Query was rejected: {query}"
