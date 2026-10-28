"""Comprehensive unit tests for chatbot_deterministic.py.

Tests run entirely in-memory with a Spy Neo4j driver (zero Docker/network dependencies).
Verifies:
1. Identifier quoting (_quote_ident) across hostile characters, unicode, quotes, and backticks.
2. Parameterization and safety: hostile column names and filter values are strictly parameterized;
   user-supplied text never appears in the Cypher query string except as safely quoted identifiers.
3. Every pattern branch and intent in chatbot_deterministic.py.
"""

from typing import Any, Callable

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from api.chatbot_deterministic import (
    _aggregate_intent,
    _count_intent,
    _dataset_status_intent,
    _distinct_values_intent,
    _extract_agg_func,
    _extract_comparison,
    _extract_number,
    _find_equality,
    _groupby_count_intent,
    _list_intent,
    _matched_columns,
    _quote_ident,
    _row_lookup,
    _schema_intent,
    _topn_intent,
    try_answer,
)


# ==============================================================================
# In-Memory Spy Neo4j Infrastructure (No Docker, No Network)
# ==============================================================================
class SpyRecord:
    def __init__(self, data: dict[str, Any]):
        self._data = data

    def data(self) -> dict[str, Any]:
        return self._data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]


class SpyTx:
    def __init__(self, session: "SpySession"):
        self.session = session

    def run(self, cypher: str, **params: Any) -> list[SpyRecord]:
        self.session.recorded_calls.append((cypher, params))
        results = self.session.match_results(cypher, params)
        return [SpyRecord(r) for r in results]


class SpySession:
    def __init__(self, query_responses: dict[str, Any] | None = None):
        self.recorded_calls: list[tuple[str, dict[str, Any]]] = []
        self.query_responses = query_responses or {}

    def __enter__(self) -> "SpySession":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def execute_read(self, tx_func: Callable[[SpyTx], Any]) -> Any:
        tx = SpyTx(self)
        return tx_func(tx)

    def match_results(self, cypher: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        for pattern, response in self.query_responses.items():
            if pattern in cypher:
                if callable(response):
                    return response(cypher, params)
                return response
        return []


class SpyDriver:
    def __init__(self, query_responses: dict[str, Any] | None = None):
        self.session_instance = SpySession(query_responses)

    def session(self, database: str | None = None) -> SpySession:
        return self.session_instance

    @property
    def recorded_calls(self) -> list[tuple[str, dict[str, Any]]]:
        return self.session_instance.recorded_calls


# ==============================================================================
# 1. _quote_ident Tests
# ==============================================================================
@pytest.mark.parametrize(
    ("input_ident", "expected"),
    [
        ("", "``"),
        ("col", "`col`"),
        ("column_name", "`column_name`"),
        ("col`name", "`col``name`"),
        ("col``double", "`col````double`"),
        ("col'quote", "`col'quote`"),
        ('col"doublequote', '`col"doublequote`'),
        ("col{brace}", "`col{brace}`"),
        ("col; DROP TABLE", "`col; DROP TABLE`"),
        ("col--comment", "`col--comment`"),
        ("колонka", "`колонka`"),
        ("col 🚀 emoji", "`col 🚀 emoji`"),
        ("a.b.c", "`a.b.c`"),
        ("`", "````"),
    ],
    ids=[
        "empty-string",
        "simple-ascii",
        "snake-case",
        "single-backtick",
        "double-backtick",
        "single-quote",
        "double-quote",
        "curly-braces",
        "semicolon-sql-injection",
        "sql-comment-syntax",
        "unicode-cyrillic",
        "unicode-emoji",
        "dots",
        "lone-backtick",
    ],
)
def test_quote_ident(input_ident: str, expected: str):
    """Verify that identifiers are always properly escaped inside backticks."""
    assert _quote_ident(input_ident) == expected


# ==============================================================================
# 2. Helper Parsing Functions Tests
# ==============================================================================
def test_extract_number():
    assert _extract_number("amount over 100") == 100.0
    assert _extract_number("balance is $1,234.56") == 1234.56
    assert _extract_number("temperature below -15.5") == -15.5
    assert _extract_number("how many rows are there") is None


def test_extract_comparison():
    assert _extract_comparison("amount greater than or equal to 10") == ">="
    assert _extract_comparison("at least 5 items") == ">="
    assert _extract_comparison("cost >= 100") == ">="
    assert _extract_comparison("less than or equal to 50") == "<="
    assert _extract_comparison("at most 25") == "<="
    assert _extract_comparison("rating <= 4") == "<="
    assert _extract_comparison("amount greater than 10") == ">"
    assert _extract_comparison("more than 5") == ">"
    assert _extract_comparison("amount above 100") == ">"
    assert _extract_comparison("items over 20") == ">"
    assert _extract_comparison("val > 0") == ">"
    assert _extract_comparison("less than 10") == "<"
    assert _extract_comparison("under 50") == "<"
    assert _extract_comparison("below 0") == "<"
    assert _extract_comparison("val < 5") == "<"
    assert _extract_comparison("how many rows are in the table") is None


def test_extract_agg_func():
    assert _extract_agg_func("sum of amount") == "sum"
    assert _extract_agg_func("total amount") == "sum"
    # Negative lookahead: 'total number' must not match 'total' aggregate
    assert _extract_agg_func("total number of rows") is None
    assert _extract_agg_func("average balance") == "avg"
    assert _extract_agg_func("avg score") == "avg"
    assert _extract_agg_func("mean amount") == "avg"
    assert _extract_agg_func("maximum value") == "max"
    assert _extract_agg_func("highest amount") == "max"
    assert _extract_agg_func("largest order") == "max"
    assert _extract_agg_func("greatest revenue") == "max"
    assert _extract_agg_func("minimum price") == "min"
    assert _extract_agg_func("lowest cost") == "min"
    assert _extract_agg_func("smallest quantity") == "min"
    assert _extract_agg_func("show me the data") is None


def test_matched_columns():
    columns = ["amount", "amount_usd", "department"]
    # Longest match wins
    assert _matched_columns("what is the total amount_usd", columns) == ["amount_usd", "amount"]
    # Plural match
    assert _matched_columns("list all departments", columns) == ["department"]
    # Longest column name first ("department" len 10 > "amount" len 6)
    assert _matched_columns("show AMOUNT and Department", columns) == ["department", "amount"]
    assert _matched_columns("tell me a story", columns) == []


# ==============================================================================
# 3. Parameterization & Hostile Input Spy Verification
#    "Assert user-supplied text never appears in query string except via _quote_ident,
#     and values only appear in params."
# ==============================================================================
# Unique canary tokens designed to ensure that any appearance in query text is an unambiguous leak.
CANARY_VAL_SEMI = "x9f3_canary_'; DROP ALL NODES; --"
CANARY_VAL_OR = "x9f3_canary_admin' OR '1'='1"
CANARY_COL_SEMI = "department_x9f3_canary; DROP (n)"
CANARY_COL_BACKTICK = "`evil`col_x9f3_canary`"


@pytest.mark.parametrize(
    ("question", "columns", "distinct_map", "expects_value_param"),
    [
        (
            f"how many rows belong to {CANARY_VAL_SEMI}",
            [CANARY_COL_SEMI],
            {CANARY_COL_SEMI: [CANARY_VAL_SEMI]},
            True,
        ),
        (
            f"average {CANARY_COL_BACKTICK} for {CANARY_VAL_OR}",
            [CANARY_COL_BACKTICK, "group"],
            {"group": [CANARY_VAL_OR]},
            True,
        ),
        (
            f"show rows where {CANARY_COL_SEMI} is {CANARY_VAL_SEMI}",
            [CANARY_COL_SEMI],
            {CANARY_COL_SEMI: [CANARY_VAL_SEMI]},
            True,
        ),
        (
            f"how many rows have {CANARY_COL_BACKTICK} over 100",
            [CANARY_COL_BACKTICK],
            {},
            False,
        ),
        (
            f"top 5 by {CANARY_COL_SEMI}",
            [CANARY_COL_SEMI],
            {},
            False,
        ),
        (
            f"show rows where {CANARY_COL_BACKTICK} is over 50.5",
            [CANARY_COL_BACKTICK],
            {},
            False,
        ),
    ],
    ids=[
        "count-equality-canary-col-and-val",
        "aggregate-equality-backtick-col-and-canary-sqli-val",
        "list-equality-canary-col-and-val",
        "count-comparison-backtick-col",
        "topn-canary-col",
        "list-comparison-backtick-col",
    ],
)
def test_no_user_text_in_cypher_spy_assertion(
    question: str,
    columns: list[str],
    distinct_map: dict[str, list[str]],
    expects_value_param: bool,
):
    """Rigorous Spy Test: Assert user-supplied hostile strings NEVER appear in the Cypher query.

    Note: covers the deterministic tier only; the LLM tier is guarded by api/guardrail.py (see xfail gaps).
    Column names must ONLY appear through _quote_ident output.
    Filter values must ONLY appear inside the query parameter dictionary.
    """
    def mock_results(cypher: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "RETURN DISTINCT" in cypher:
            for col, vals in distinct_map.items():
                if _quote_ident(col) in cypher:
                    return [{"v": v} for v in vals]
            return []
        if "count(r)" in cypher:
            return [{"count": 42}]
        if "result" in cypher:
            return [{"result": 99.9}]
        return [{"r": {"id": 1}}]

    driver = SpyDriver({"MATCH": mock_results})

    res = try_answer(driver, "csv-graph-db", question, columns)
    assert res is not None, f"Deterministic intent failed to match question: {question}"
    assert len(driver.recorded_calls) >= 1

    value_param_found = False

    # Check every query executed during intent resolution
    for cypher_str, params_dict in driver.recorded_calls:
        # 1. Hostile canary values must NEVER appear anywhere in the cypher query string
        for col_vals in distinct_map.values():
            for val in col_vals:
                assert val not in cypher_str, (
                    f"CRITICAL VULNERABILITY: Canary value '{val}' leaked into Cypher query: {cypher_str}"
                )
                if "$value" in cypher_str:
                    assert params_dict.get("value") == val
                    value_param_found = True

        # 2. Hostile column names must ONLY appear in cypher as exact escaped _quote_ident(col)
        for col in columns:
            quoted = _quote_ident(col)
            if quoted in cypher_str:
                # Raw unquoted version must not leak outside backticks
                if "`" in col:
                    assert f"r.{col}" not in cypher_str
            else:
                assert col not in cypher_str

    # 3. Non-vacuous assertion: verify that value-filtering queries actually executed a parameterized $value query
    if expects_value_param:
        assert value_param_found is True, (
            f"Vacuous test detected! Query was expected to dispatch with $value, but recorded calls were: {driver.recorded_calls}"
        )

    # 4. Verify that the primary query actually contains the target column formatted via _quote_ident
    final_query = driver.recorded_calls[-1][0]
    matched = _matched_columns(question, columns)
    if matched:
        expected_quoted_target = _quote_ident(matched[0])
        assert expected_quoted_target in final_query, (
            f"Expected quoted identifier '{expected_quoted_target}' in final query: {final_query}"
        )


@given(
    col_name=st.text(
        alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters=("\x00", "\n", "\r")),  # type: ignore[arg-type]
        min_size=1,
        max_size=20,
    ),
    filter_val=st.text(
        alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters=("\x00", "\n", "\r")),  # type: ignore[arg-type]
        min_size=1,
        max_size=30,
    ),
)
@settings(max_examples=30)
def test_no_user_text_in_cypher_hypothesis(col_name: str, filter_val: str):
    """Hypothesis property: arbitrary user-supplied column names and values never appear as raw Cypher text.

    Note: covers the deterministic tier only; the LLM tier is guarded by api/guardrail.py (see xfail gaps).
    """
    canary_col = f"col_x9f3_{col_name}"
    canary_val = f"val_x9f3_{filter_val}"

    driver = SpyDriver({
        "RETURN DISTINCT": [{"v": canary_val}],
        "toLower($value)": [{"count": 7}],
    })

    question = f"how many rows have {canary_col} equal to {canary_val}"
    res = try_answer(driver, "csv-graph-db", question, [canary_col])
    if res is not None:
        for cypher_str, params_dict in driver.recorded_calls:
            # Canary value must never appear in raw query text
            assert canary_val not in cypher_str, (
                f"Filter value leaked into Cypher query: {cypher_str}"
            )
            if "$value" in cypher_str:
                assert params_dict.get("value") == canary_val
            # Column name must appear only through _quote_ident
            quoted = _quote_ident(canary_col)
            if quoted in cypher_str and "`" in canary_col:
                assert f"r.{canary_col}" not in cypher_str




# ==============================================================================
# 4. Pattern Branches in chatbot_deterministic.py
# ==============================================================================
def test_row_lookup_found():
    driver = SpyDriver({"row_index: $idx": [{"r": {"row_index": 5, "name": "Alice"}}]})
    res = _row_lookup(driver, "csv-graph-db", "show row 5", ["name"])
    assert res is not None
    assert "Row 5: {'row_index': 5, 'name': 'Alice'}" in res["answer"]
    assert res["grounded"] is True
    assert driver.recorded_calls[0][1] == {"idx": 5}


def test_row_lookup_not_found():
    driver = SpyDriver({"row_index: $idx": []})
    res = _row_lookup(driver, "csv-graph-db", "what's in row 99", ["name"])
    assert res is not None
    assert "There is no row at index 99." in res["answer"]
    assert res["grounded"] is True


def test_row_lookup_no_pattern():
    driver = SpyDriver()
    assert _row_lookup(driver, "csv-graph-db", "who is rowan", ["name"]) is None


def test_distinct_values_intent():
    driver = SpyDriver({"RETURN DISTINCT": [{"value": "Engineering"}, {"value": "Marketing"}]})
    res = _distinct_values_intent(driver, "csv-graph-db", "what different departments are there", ["department"])
    assert res is not None
    assert "Distinct values of department: Engineering, Marketing." == res["answer"]
    assert "LIMIT 50" in res["cypher"]


def test_distinct_values_intent_no_match():
    driver = SpyDriver()
    # Missing column
    assert _distinct_values_intent(driver, "csv-graph-db", "what distinct values are there", ["department"]) is None
    # Missing distinct keyword
    assert _distinct_values_intent(driver, "csv-graph-db", "show departments", ["department"]) is None


def test_groupby_count_intent():
    driver = SpyDriver({"ORDER BY count DESC": [{"value": "Sales", "count": 15}, {"value": "IT", "count": 8}]})
    res = _groupby_count_intent(driver, "csv-graph-db", "how many rows per department", ["department"])
    assert res is not None
    assert "Row counts by department — Sales: 15; IT: 8." == res["answer"]
    assert "ORDER BY count DESC LIMIT 50" in res["cypher"]


def test_groupby_count_intent_no_match():
    driver = SpyDriver()
    # No column
    assert _groupby_count_intent(driver, "csv-graph-db", "how many rows per each", ["department"]) is None
    # No count word
    assert _groupby_count_intent(driver, "csv-graph-db", "per department", ["department"]) is None


def test_topn_intent_desc():
    driver = SpyDriver({"LIMIT $n": [{"r": {"name": "Bob", "amount": 500}}]})
    res = _topn_intent(driver, "csv-graph-db", "top 5 by amount", ["amount"])
    assert res is not None
    assert "Top 1 row(s) by amount (descending)." in res["answer"]
    assert "ORDER BY toFloat(r.`amount`) DESC LIMIT $n" in res["cypher"]
    assert driver.recorded_calls[0][1] == {"n": 5}


def test_topn_intent_asc():
    driver = SpyDriver({"LIMIT $n": [{"r": {"name": "Charlie", "amount": 10}}]})
    res = _topn_intent(driver, "csv-graph-db", "lowest 3 amount", ["amount"])
    assert res is not None
    assert "Top 1 row(s) by amount (ascending)." in res["answer"]
    assert "ORDER BY toFloat(r.`amount`) ASC LIMIT $n" in res["cypher"]
    assert driver.recorded_calls[0][1] == {"n": 3}


def test_topn_intent_cap():
    driver = SpyDriver({"LIMIT $n": []})
    _topn_intent(driver, "csv-graph-db", "top 50000 by amount", ["amount"])
    # Must cap n at 200
    assert driver.recorded_calls[0][1] == {"n": 200}


def test_topn_intent_no_match():
    driver = SpyDriver()
    assert _topn_intent(driver, "csv-graph-db", "top 5 by unknown", ["amount"]) is None
    assert _topn_intent(driver, "csv-graph-db", "show amount", ["amount"]) is None


def test_aggregate_intent_unfiltered():
    driver = SpyDriver({"RETURN avg(v) AS result": [{"result": 123.45}]})
    res = _aggregate_intent(driver, "csv-graph-db", "what is the average amount", ["amount"])
    assert res is not None
    assert "The avg of amount is 123.45." == res["answer"]
    assert "WHERE v IS NOT NULL RETURN avg(v) AS result" in res["cypher"]


def test_aggregate_intent_filtered():
    driver = SpyDriver({
        "RETURN DISTINCT": [{"v": "Billing"}, {"v": "HR"}],
        "RETURN sum(v) AS result": [{"result": 5000.0}],
    })
    res = _aggregate_intent(
        driver,
        "csv-graph-db",
        "total amount for Billing",
        ["amount", "department"],
    )
    assert res is not None
    assert "The sum of amount where department = 'Billing' is 5000.0." == res["answer"]
    call = driver.recorded_calls[-1]
    assert "WHERE r.`department` = $value" in call[0]
    assert call[1] == {"value": "Billing"}


def test_aggregate_intent_unresolvable_filter_intent():
    driver = SpyDriver({"RETURN DISTINCT": [{"v": "HR"}]})
    # Filter intent word "for" is present, but value "Nonexistent" does not resolve
    res = _aggregate_intent(driver, "csv-graph-db", "average amount for Nonexistent", ["amount", "department"])
    # Must return None to prevent answering unfiltered query silently
    assert res is None


def test_count_intent_comparison():
    driver = SpyDriver({"RETURN count(r) AS count": [{"count": 14}]})
    res = _count_intent(driver, "csv-graph-db", "how many rows have amount over 100", ["amount"])
    assert res is not None
    assert "There are 14 rows where amount > 100.0." in res["answer"]
    assert "WHERE toFloat(r.`amount`) > $n RETURN count(r) AS count" in res["cypher"]
    assert driver.recorded_calls[0][1] == {"n": 100.0}


def test_count_intent_equality():
    driver = SpyDriver({
        "RETURN DISTINCT": [{"v": "Billing"}],
        "toLower($value)": [{"count": 8}],
    })
    res = _count_intent(driver, "csv-graph-db", "how many rows belong to Billing", ["department"])
    assert res is not None
    assert "There are 8 rows where department = 'Billing'." in res["answer"]
    assert "WHERE toLower(r.`department`) = toLower($value) RETURN count(r) AS count" in res["cypher"]
    assert driver.recorded_calls[-1][1] == {"value": "Billing"}


def test_count_intent_total():
    driver = SpyDriver({"MATCH (r:Row) RETURN count(r) AS count": [{"count": 100}]})
    res = _count_intent(driver, "csv-graph-db", "how many rows are there", ["amount"])
    assert res is not None
    assert "There are 100 rows in total." in res["answer"]


def test_count_intent_unresolvable_filter_intent():
    driver = SpyDriver({"RETURN DISTINCT": []})
    # "belong to" implies filter, but no value matches
    assert _count_intent(driver, "csv-graph-db", "how many rows belong to Nowhere", ["department"]) is None


def test_list_intent_comparison():
    driver = SpyDriver({"RETURN r LIMIT 20": [{"r": {"amount": 150}}]})
    res = _list_intent(driver, "csv-graph-db", "show rows where amount is over 100", ["amount"])
    assert res is not None
    assert "Found 1 row(s) where amount > 100.0." in res["answer"]
    assert "WHERE toFloat(r.`amount`) > $n RETURN r LIMIT 20" in res["cypher"]
    assert driver.recorded_calls[0][1] == {"n": 100.0}


def test_list_intent_equality():
    driver = SpyDriver({
        "RETURN DISTINCT": [{"v": "Billing"}],
        "toLower($value) RETURN r LIMIT 20": [{"r": {"department": "Billing"}}],
    })
    res = _list_intent(driver, "csv-graph-db", "show rows where department is Billing", ["department"])
    assert res is not None
    assert "Found 1 row(s) where department = 'Billing'." in res["answer"]
    assert "WHERE toLower(r.`department`) = toLower($value) RETURN r LIMIT 20" in res["cypher"]
    assert driver.recorded_calls[-1][1] == {"value": "Billing"}


def test_schema_intent():
    driver = SpyDriver({"keys(r)": [{"columns": ["id", "amount"]}]})
    res = _schema_intent(driver, "csv-graph-db", "what columns do we have", ["id", "amount"])
    assert res is not None
    assert "The columns are: id, amount." in res["answer"]
    assert "keys(r) WHERE NOT k IN ['row_index', 'dataset_id']" in res["cypher"]


def test_dataset_status_intent_singular_and_plural():
    # Singular
    driver_1 = SpyDriver({"MATCH (d:Dataset)": [{"filename": "data.csv"}]})
    res_1 = _dataset_status_intent(driver_1, "csv-graph-db", "what is the dataset status", [])
    assert res_1 is not None
    assert "1 dataset loaded." in res_1["answer"]

    # Plural
    driver_2 = SpyDriver({"MATCH (d:Dataset)": [{"filename": "a.csv"}, {"filename": "b.csv"}]})
    res_2 = _dataset_status_intent(driver_2, "csv-graph-db", "dataset status", [])
    assert res_2 is not None
    assert "2 datasets loaded." in res_2["answer"]


def test_aggregate_intent_multiple_matched_columns():
    driver = SpyDriver({
        "RETURN DISTINCT": [{"v": "Billing"}],
        "RETURN avg(v) AS result": [{"result": 500.0}],
    })
    # 'balance' (len 7) is matched[0] (target), 'dept' (len 4) is matched[1] (filter)
    res = _aggregate_intent(driver, "csv-graph-db", "average balance for dept Billing", ["balance", "dept"])
    assert res is not None
    assert "The avg of balance where dept = 'Billing' is 500.0." == res["answer"]



def test_aggregate_intent_no_column_matched():
    driver = SpyDriver()
    # 'average' matches agg func, but no column in question
    assert _aggregate_intent(driver, "csv-graph-db", "what is the average", ["amount"]) is None


def test_find_equality_not_found():

    driver = SpyDriver({"RETURN DISTINCT": [{"v": "Marketing"}]})
    assert _find_equality("question with no matching value", "department", driver, "csv-graph-db") is None


def test_list_intent_matched_column_but_no_filter():
    driver = SpyDriver({"RETURN DISTINCT": []})
    # 'show' and 'amount' matched, but no comparison op or value filter
    assert _list_intent(driver, "csv-graph-db", "show amount", ["amount"]) is None


def test_try_answer_unhandled_and_error_handling():
    driver = SpyDriver()
    # Unhandled question
    assert try_answer(driver, "csv-graph-db", "write me a haiku about databases", ["amount"]) is None

    # Intent raising exception is logged and gracefully falls through
    class FailingDriver:
        def session(self, database=None):
            raise RuntimeError("Database unavailable")

    assert try_answer(FailingDriver(), "csv-graph-db", "show row 1", ["amount"]) is None


def test_extract_number_deterministic_cases():
    """Verify _extract_number correctly parses numbers and returns None on non-number input."""
    assert _extract_number("how many rows over $1,000") == 1000.0
    assert _extract_number("balance at least +$1,234.56") == 1234.56
    assert _extract_number("temperature below -50") == -50.0
    assert _extract_number("count under 0.05") == 0.05
    assert _extract_number("no number in this query") is None
    assert _extract_number("a lone - or + sign") is None
    assert _extract_number("a bare $ sign") is None


@given(text=st.text())
@settings(max_examples=100)
def test_extract_number_never_raises(text: str):
    r"""Hypothesis property: _extract_number returns float or None for arbitrary text, never raises.

    Analytical proof: _NUMBER = re.compile(r"[-+]?\$?\d[\d,]*(?:\.\d+)?").
    Any match stripped of '$' and ',' matches ^[-+]?\d+(\.\d+)?$, which Python's float() always parses.
    Lines 109-110 in api/chatbot_deterministic.py (except ValueError: return None) are unreachable.
    """
    res = _extract_number(text)
    assert res is None or isinstance(res, float)
