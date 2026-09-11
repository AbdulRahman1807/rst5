"""Keyword/value matcher — answers questions from the graph with zero LLM dependency.

This is the always-available core: it works whether or not GROQ_API_KEY is set. chatbot.py
uses the LLM (when configured) to handle question phrasings this doesn't cover, and falls
back to this module if the LLM is unavailable, misconfigured, or fails.

Every Cypher query here is built programmatically from matched columns/values/numbers —
never from raw user text spliced in — so it's safe by construction, no validation step
needed (unlike the LLM path). Column names are escaped (_quote_ident) since they come from
CSV headers, which are hostile input (req 6.7).

Intents supported (tried in order, most specific first):
  1. row lookup by index          "show row 5" / "what's in row 12"
  2. distinct values of a column  "what groups are there" / "list unique group"
  3. group-by count               "how many rows per group" / "count by group"
  4. top-N by column               "top 5 by amount" / "highest 3 amount"
  5. aggregate (sum/avg/min/max)  "average amount" / "total amount for Billing"
  6. count with comparison        "how many rows have amount over 100"
  7. count with equality filter   "how many rows belong to Billing"
  8. total count                  "how many rows are there"
  9. list with comparison filter  "show rows where amount is over 100"
 10. list with equality filter    "show rows where group is Billing"
 11. schema / columns question    "what columns do we have"
 12. dataset/status meta          "what's the status" / "how many datasets"
"""
import logging
import re

log = logging.getLogger("chatbot_deterministic")

_COUNT_WORDS = re.compile(r"\b(how many|count|total number|number of)\b", re.IGNORECASE)
_LIST_WORDS = re.compile(r"\b(show|list|which rows|what rows|find)\b", re.IGNORECASE)
_STATUS_WORDS = re.compile(r"\b(dataset|upload|status)\b", re.IGNORECASE)
_DISTINCT_WORDS = re.compile(r"\b(different|unique|distinct)\b", re.IGNORECASE)
_SCHEMA_WORDS = re.compile(r"\b(what columns|which columns|what fields|what data|what properties)\b", re.IGNORECASE)
_ROW_LOOKUP = re.compile(r"\brow\s*(?:index|number|#)?\s*(\d+)\b", re.IGNORECASE)
_GROUPBY_WORDS = re.compile(r"\b(per|by|for each|each|breakdown (?:of|by))\b", re.IGNORECASE)
_TOPN_WORDS = re.compile(r"\btop\s*(\d+)\b|\bhighest\s*(\d+)\b|\blowest\s*(\d+)\b", re.IGNORECASE)

_AGG_FUNCS = [
    (re.compile(r"\b(sum|total)\s+of\b|\btotal\b(?!\s+number)", re.IGNORECASE), "sum"),
    (re.compile(r"\b(average|avg|mean)\b", re.IGNORECASE), "avg"),
    (re.compile(r"\b(max(?:imum)?|highest|largest|greatest)\b", re.IGNORECASE), "max"),
    (re.compile(r"\b(min(?:imum)?|lowest|smallest)\b", re.IGNORECASE), "min"),
]

# Longest/most-specific phrasing first so e.g. "at least" wins over a bare ">" style match.
_COMPARISONS = [
    (re.compile(r"greater than or equal to|at least|>=", re.IGNORECASE), ">="),
    (re.compile(r"less than or equal to|at most|<=", re.IGNORECASE), "<="),
    (re.compile(r"greater than|more than|above|over|>", re.IGNORECASE), ">"),
    (re.compile(r"less than|under|below|<", re.IGNORECASE), "<"),
]

_NUMBER = re.compile(r"[-+]?\$?\d[\d,]*(?:\.\d+)?")


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _run(driver, database: str, cypher: str, params: dict) -> list[dict]:
    def _tx(tx):
        return [r.data() for r in tx.run(cypher, **params)]

    with driver.session(database=database) as session:
        return session.execute_read(_tx)


def _distinct_values(driver, database: str, column: str, limit: int = 500) -> list[str]:
    col = _quote_ident(column)
    result = _run(
        driver, database,
        f"MATCH (r:Row) WHERE r.{col} IS NOT NULL RETURN DISTINCT r.{col} AS v LIMIT $limit",
        {"limit": limit},
    )
    return [str(rec["v"]) for rec in result]


def _matched_columns(question: str, columns: list[str]) -> list[str]:
    """Columns mentioned in the question, longest name first (so 'amount_usd' beats 'amount')."""
    q_lower = question.lower()
    hits = []
    for col in sorted(columns, key=len, reverse=True):
        cl = col.lower()
        if cl in q_lower or f"{cl}s" in q_lower:
            hits.append(col)
    return hits


def _extract_number(question: str) -> float | None:
    m = _NUMBER.search(question)
    if not m:
        return None
    try:
        return float(m.group(0).replace("$", "").replace(",", ""))
    except ValueError:
        return None


def _extract_comparison(question: str) -> str | None:
    for pattern, op in _COMPARISONS:
        if pattern.search(question):
            return op
    return None


def _extract_agg_func(question: str) -> str | None:
    for pattern, func in _AGG_FUNCS:
        if pattern.search(question):
            return func
    return None


def _find_equality(question: str, column: str, driver, database: str) -> str | None:
    q_lower = question.lower()
    for value in _distinct_values(driver, database, column):
        if value and value.lower() in q_lower:
            return value
    return None


def _find_any_filter(question: str, columns: list[str], driver, database: str) -> tuple[str | None, str | None]:
    """Fallback for phrasing that names a value but not its column, e.g. 'how many rows
    belong to Billing' (no 'group' in the question at all). Scans every column's distinct
    values for one that appears in the question; prefers the longest match to avoid
    accidental short-string hits (e.g. a value 'A' matching almost anything)."""
    q_lower = question.lower()
    best: tuple[str, str] | None = None
    for column in columns:
        for value in _distinct_values(driver, database, column):
            if value and value.lower() in q_lower:
                if best is None or len(value) > len(best[1]):
                    best = (column, value)
    return best if best else (None, None)


def _row_lookup(driver, database, question, columns):
    m = _ROW_LOOKUP.search(question)
    if not m:
        return None
    idx = int(m.group(1))
    cypher = "MATCH (r:Row {row_index: $idx}) RETURN r"
    result = _run(driver, database, cypher, {"idx": idx})
    if not result:
        return {"answer": f"There is no row at index {idx}.", "cypher": cypher, "result": result, "grounded": True}
    return {"answer": f"Row {idx}: {result[0]['r']}", "cypher": cypher, "result": result, "grounded": True}


def _distinct_values_intent(driver, database, question, columns):
    if not _DISTINCT_WORDS.search(question):
        return None
    matched = _matched_columns(question, columns)
    if not matched:
        return None
    column = matched[0]
    col = _quote_ident(column)
    cypher = f"MATCH (r:Row) WHERE r.{col} IS NOT NULL RETURN DISTINCT r.{col} AS value ORDER BY value LIMIT 50"
    result = _run(driver, database, cypher, {})
    values = [row["value"] for row in result]
    return {"answer": f"Distinct values of {column}: {', '.join(map(str, values))}.", "cypher": cypher, "result": result, "grounded": True}


def _groupby_count_intent(driver, database, question, columns):
    if not (_GROUPBY_WORDS.search(question) and _COUNT_WORDS.search(question)):
        return None
    matched = _matched_columns(question, columns)
    if not matched:
        return None
    column = matched[0]
    col = _quote_ident(column)
    cypher = f"MATCH (r:Row) RETURN r.{col} AS value, count(r) AS count ORDER BY count DESC LIMIT 50"
    result = _run(driver, database, cypher, {})
    parts = "; ".join(f"{row['value']}: {row['count']}" for row in result)
    return {"answer": f"Row counts by {column} — {parts}.", "cypher": cypher, "result": result, "grounded": True}


def _topn_intent(driver, database, question, columns):
    m = _TOPN_WORDS.search(question)
    if not m:
        return None
    n = int(next(g for g in m.groups() if g))
    matched = _matched_columns(question, columns)
    if not matched:
        return None
    column = matched[0]
    col = _quote_ident(column)
    direction = "ASC" if re.search(r"\blowest\b", question, re.IGNORECASE) else "DESC"
    cypher = (
        f"MATCH (r:Row) WHERE toFloat(r.{col}) IS NOT NULL "
        f"RETURN r ORDER BY toFloat(r.{col}) {direction} LIMIT $n"
    )
    result = _run(driver, database, cypher, {"n": n})
    return {"answer": f"Top {len(result)} row(s) by {column} ({direction.lower()}ending).", "cypher": cypher, "result": result, "grounded": True}


def _aggregate_intent(driver, database, question, columns):
    func = _extract_agg_func(question)
    if not func:
        return None
    matched = _matched_columns(question, columns)
    if not matched:
        return None

    target_col = matched[0]

    filter_col, value = None, None
    if len(matched) > 1:
        filter_col = matched[1]
        value = _find_equality(question, filter_col, driver, database)
    if value is None:
        # e.g. "average amount for Billing" — no column name for the filter, just the value.
        other_cols = [c for c in columns if c != target_col]
        filter_col, value = _find_any_filter(question, other_cols, driver, database)

    where_clause, params = "", {}
    if filter_col and value is not None:
        where_clause = f"WHERE r.{_quote_ident(filter_col)} = $value "
        params["value"] = value

    col = _quote_ident(target_col)
    cypher = (
        f"MATCH (r:Row) {where_clause}"
        f"WITH toFloat(r.{col}) AS v WHERE v IS NOT NULL RETURN {func}(v) AS result"
    )
    result = _run(driver, database, cypher, params)
    value = result[0]["result"] if result else None
    filter_desc = f" where {filter_col} = '{params.get('value')}'" if params.get("value") else ""
    return {"answer": f"The {func} of {target_col}{filter_desc} is {value}.", "cypher": cypher, "result": result, "grounded": True}


def _count_intent(driver, database, question, columns):
    if not _COUNT_WORDS.search(question):
        return None

    matched = _matched_columns(question, columns)
    filter_col = matched[0] if matched else None

    op = _extract_comparison(question)
    if filter_col and op:
        number = _extract_number(question)
        if number is not None:
            col = _quote_ident(filter_col)
            cypher = f"MATCH (r:Row) WHERE toFloat(r.{col}) {op} $n RETURN count(r) AS count"
            result = _run(driver, database, cypher, {"n": number})
            count = result[0]["count"] if result else 0
            return {"answer": f"There are {count} rows where {filter_col} {op} {number}.", "cypher": cypher, "result": result, "grounded": True}

    value = _find_equality(question, filter_col, driver, database) if filter_col else None
    if value is None:
        # No column name mentioned (or no matching value for it) — try inferring from any
        # column's values, e.g. "how many rows belong to Billing" with no "group" in the text.
        filter_col, value = _find_any_filter(question, columns, driver, database)

    if filter_col and value is not None:
        col = _quote_ident(filter_col)
        cypher = f"MATCH (r:Row) WHERE toLower(r.{col}) = toLower($value) RETURN count(r) AS count"
        result = _run(driver, database, cypher, {"value": value})
        count = result[0]["count"] if result else 0
        return {"answer": f"There are {count} rows where {filter_col} = '{value}'.", "cypher": cypher, "result": result, "grounded": True}

    if not matched:
        cypher = "MATCH (r:Row) RETURN count(r) AS count"
        result = _run(driver, database, cypher, {})
        count = result[0]["count"] if result else 0
        return {"answer": f"There are {count} rows in total.", "cypher": cypher, "result": result, "grounded": True}

    return None  # column mentioned but no usable filter extracted — let list/other intents try


def _list_intent(driver, database, question, columns):
    if not _LIST_WORDS.search(question):
        return None
    matched = _matched_columns(question, columns)
    filter_col = matched[0] if matched else None

    op = _extract_comparison(question)
    if filter_col and op:
        number = _extract_number(question)
        if number is not None:
            col = _quote_ident(filter_col)
            cypher = f"MATCH (r:Row) WHERE toFloat(r.{col}) {op} $n RETURN r LIMIT 20"
            result = _run(driver, database, cypher, {"n": number})
            return {"answer": f"Found {len(result)} row(s) where {filter_col} {op} {number}.", "cypher": cypher, "result": result, "grounded": True}

    value = _find_equality(question, filter_col, driver, database) if filter_col else None
    if value is None:
        filter_col, value = _find_any_filter(question, columns, driver, database)

    if filter_col and value is not None:
        col = _quote_ident(filter_col)
        cypher = f"MATCH (r:Row) WHERE toLower(r.{col}) = toLower($value) RETURN r LIMIT 20"
        result = _run(driver, database, cypher, {"value": value})
        return {"answer": f"Found {len(result)} row(s) where {filter_col} = '{value}'.", "cypher": cypher, "result": result, "grounded": True}

    return None


def _schema_intent(driver, database, question, columns):
    if not _SCHEMA_WORDS.search(question):
        return None
    cypher = "MATCH (r:Row) RETURN keys(r) AS keys LIMIT 1"
    result = _run(driver, database, cypher, {})
    return {"answer": f"The columns are: {', '.join(columns)}.", "cypher": cypher, "result": result, "grounded": True}


def _dataset_status_intent(driver, database, question, columns):
    if not _STATUS_WORDS.search(question):
        return None
    cypher = (
        "MATCH (d:Dataset) RETURN d.filename AS filename, d.status AS status, "
        "d.rows_total AS rows_total, d.rows_loaded AS rows_loaded, d.rows_failed AS rows_failed"
    )
    result = _run(driver, database, cypher, {})
    n = len(result)
    return {"answer": f"{n} dataset{'s' if n != 1 else ''} loaded.", "cypher": cypher, "result": result, "grounded": True}


# Order matters: most specific/unambiguous intents first.
_INTENTS = [
    _row_lookup,
    _distinct_values_intent,
    _groupby_count_intent,
    _topn_intent,
    _aggregate_intent,
    _count_intent,
    _list_intent,
    _schema_intent,
    _dataset_status_intent,
]


def try_answer(driver, database: str, question: str, columns: list[str]) -> dict | None:
    """Returns a full /chat response dict if any intent matched the question, else None."""
    for intent in _INTENTS:
        try:
            result = intent(driver, database, question, columns)
        except Exception as e:
            log.warning("deterministic intent %s raised: %s", intent.__name__, e)
            continue
        if result is not None:
            return result
    return None
