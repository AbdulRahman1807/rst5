import os
import sys

# Ensure api and loader are on path
sys.path.insert(0, os.path.abspath("api"))
sys.path.insert(0, os.path.abspath("loader"))


def test_imports():
    """Verify all core service modules import without syntax or runtime import errors."""
    from api import chatbot, chatbot_deterministic
    from api import main as api_main
    from loader import main as loader_main

    assert hasattr(chatbot, "answer_question")
    assert hasattr(chatbot_deterministic, "try_answer")
    assert hasattr(api_main, "app")
    assert hasattr(loader_main, "MERGE_ROW")


def test_quote_ident():
    """Verify that identifier quoting safely wraps column names in backticks."""
    from chatbot_deterministic import _quote_ident

    assert _quote_ident("customer") == "`customer`"
    assert _quote_ident("col`name") == "`col``name`"


def test_guardrail_baseline():
    """Verify the baseline read-only validator blocks write clauses and approves read queries."""
    from chatbot import NO_QUERY, validate_read_only

    # Should approve read-only queries
    assert validate_read_only("MATCH (r:Row) RETURN count(r)") is True
    assert validate_read_only("RETURN 1") is True
    assert validate_read_only("WITH 1 AS x RETURN x") is True

    # Should reject destructive queries
    assert validate_read_only("MATCH (r:Row) DELETE r") is False
    assert validate_read_only("CREATE (n:Node)") is False
    assert validate_read_only("DROP CONSTRAINT foo") is False
    assert validate_read_only("MATCH (r:Row) SET r.amount = 0") is False
    assert validate_read_only(NO_QUERY) is False
    assert validate_read_only("") is False
