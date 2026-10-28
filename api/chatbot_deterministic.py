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
_SCHEMA_WORDS = re.compile(
    r"\b(what columns|which columns|list.*columns|column names|what fields|list.*fields|"
    r"field names|what data|what properties|the schema|list.*properties)\b",
    re.IGNORECASE,
)
_ROW_LOOKUP = re.compile(r"\brow\s*(?:index|number|#)?\s*(\d+)\b", re.IGNORECASE)
_GROUPBY_WORDS = re.compile(r"\b(per|by|for each|each|breakdown (?:of|by))\b", re.IGNORECASE)
_TOPN_WORDS = re.compile(r"\btop\s*(\d+)\b|\bhighest\s*(\d+)\b|\blowest\s*(\d+)\b", re.IGNORECASE)
_CROSS_DATASET_WORDS = re.compile(
    r"\b(connect|connected|connection|relationship|relationships|join|match(?:ing)?|across|between|both datasets|in these two)\b",
    re.IGNORECASE,
)
# Words implying the question intends a filter even when no column name/value could be
# resolved (e.g. "how many rows belong to Nonexistent") — distinguishes "no filter intended"
# from "filter intended but unresolvable", so we don't silently answer the wrong question.
_FILTER_INTENT_WORDS = re.compile(
    r"\b(belong|belongs|where|for|with|having|whose|that (?:are|is)|equal)\b", re.IGNORECASE
)

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
        driver,
        database,
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
    return {
        "answer": f"Distinct values of {column}: {', '.join(map(str, values))}.",
        "cypher": cypher,
        "result": result,
        "grounded": True,
    }


def _groupby_count_intent(driver, database, question, columns, dataset_id=None):
    if not (_GROUPBY_WORDS.search(question) and _COUNT_WORDS.search(question)):
        return None
    matched = _matched_columns(question, columns)
    if not matched:
        return None
    column = matched[0]
    col = _quote_ident(column)
    cypher = (
        f"MATCH (r:Row) WHERE r.{col} IS NOT NULL AND ($dataset_id IS NULL OR r.dataset_id = $dataset_id) "
        f"RETURN r.{col} AS value, count(r) AS count ORDER BY count DESC LIMIT 50"
    )
    result = _run(driver, database, cypher, {"dataset_id": dataset_id})
    parts = "; ".join(f"{row['value']}: {row['count']}" for row in result)
    
    chart_data = [
        {"category": str(row["value"]) if row["value"] is not None else "(empty)", "count": row["count"]}
        for row in result[:15]
    ]
    chart = {
        "type": "bar",
        "title": f"Count by {column}",
        "x_key": "category",
        "y_key": "count",
        "data": chart_data,
    } if chart_data else None

    return {
        "answer": f"Row counts by {column} — {parts}.",
        "cypher": cypher,
        "result": result,
        "grounded": True,
        "chart": chart,
    }


def _topn_intent(driver, database, question, columns):
    m = _TOPN_WORDS.search(question)
    if not m:
        return None
    n = min(int(next(g for g in m.groups() if g)), 200)  # cap — "top 999999 by amount" shouldn't dump the graph
    matched = _matched_columns(question, columns)
    if not matched:
        return None
    column = matched[0]
    col = _quote_ident(column)
    direction = "ASC" if re.search(r"\blowest\b", question, re.IGNORECASE) else "DESC"
    cypher = (
        f"MATCH (r:Row) WHERE toFloat(r.{col}) IS NOT NULL " f"RETURN r ORDER BY toFloat(r.{col}) {direction} LIMIT $n"
    )
    chart_data = []
    label_col = next((c for c in columns if c != column), "row_index")
    for row in result[:15]:
        r_data = row.get("r", {})
        val = r_data.get(column)
        lbl = r_data.get(label_col, r_data.get("row_index", "Row"))
        try:
            num_val = float(val) if val is not None else 0.0
            chart_data.append({"label": str(lbl), "value": num_val})
        except (ValueError, TypeError):
            pass

    chart = (
        {
            "type": "bar",
            "title": f"Top {len(result)} by {column}",
            "x_key": "label",
            "y_key": "value",
            "data": chart_data,
        }
        if chart_data
        else None
    )

    return {
        "answer": f"Top {len(result)} row(s) by {column} ({direction.lower()}ending).",
        "cypher": cypher,
        "result": result,
        "grounded": True,
        "chart": chart,
    }


def _aggregate_intent(driver, database, question, columns, dataset_id=None):
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
        other_cols = [c for c in columns if c != target_col]
        filter_col, value = _find_any_filter(question, other_cols, driver, database)

    if value is None and _FILTER_INTENT_WORDS.search(question):
        return None

    params = {}
    where_parts = []
    prefix = "MATCH (r:Row) "
    if dataset_id:
        prefix = "MATCH (d:Dataset {id: $dataset_id})-[:HAS_ROW]->(r:Row) "
        params["dataset_id"] = dataset_id

    if filter_col and value is not None:
        where_parts.append(f"r.{_quote_ident(filter_col)} = $value")
        params["value"] = value

    where_clause = f"WHERE {' AND '.join(where_parts)} " if where_parts else ""
    col = _quote_ident(target_col)
    cypher = (
        f"{prefix}{where_clause}"
        f"WITH toFloat(r.{col}) AS v WHERE v IS NOT NULL RETURN {func}(v) AS result"
    )
    result = _run(driver, database, cypher, params)
    value = result[0]["result"] if result else None
    filter_desc = f" where {filter_col} = '{params.get('value')}'" if params.get("value") else ""
    return {
        "answer": f"The {func} of {target_col}{filter_desc} is {value}.",
        "cypher": cypher,
        "result": result,
        "grounded": True,
    }


def _count_intent(driver, database, question, columns, dataset_id=None):
    if not _COUNT_WORDS.search(question):
        return None

    matched = _matched_columns(question, columns)
    filter_col = matched[0] if matched else None

    prefix = "MATCH (r:Row) "
    base_params = {}
    if dataset_id:
        prefix = "MATCH (d:Dataset {id: $dataset_id})-[:HAS_ROW]->(r:Row) "
        base_params["dataset_id"] = dataset_id

    op = _extract_comparison(question)
    if filter_col and op:
        number = _extract_number(question)
        if number is not None:
            col = _quote_ident(filter_col)
            cypher = f"{prefix}WHERE toFloat(r.{col}) {op} $n RETURN count(r) AS count"
            params = {**base_params, "n": number}
            result = _run(driver, database, cypher, params)
            count = result[0]["count"] if result else 0
            return {
                "answer": f"There are {count} rows where {filter_col} {op} {number}.",
                "cypher": cypher,
                "result": result,
                "grounded": True,
            }

    value = _find_equality(question, filter_col, driver, database) if filter_col else None
    if value is None:
        filter_col, value = _find_any_filter(question, columns, driver, database)

    if filter_col and value is not None:
        col = _quote_ident(filter_col)
        cypher = f"{prefix}WHERE toLower(r.{col}) = toLower($value) RETURN count(r) AS count"
        params = {**base_params, "value": value}
        result = _run(driver, database, cypher, params)
        count = result[0]["count"] if result else 0
        return {
            "answer": f"There are {count} rows where {filter_col} = '{value}'.",
            "cypher": cypher,
            "result": result,
            "grounded": True,
        }

    if not matched and not _FILTER_INTENT_WORDS.search(question):
        cypher = f"{prefix}RETURN count(r) AS count"
        result = _run(driver, database, cypher, base_params)
        count = result[0]["count"] if result else 0
        scope_txt = " in this dataset" if dataset_id else " in total"
        return {"answer": f"There are {count} rows{scope_txt}.", "cypher": cypher, "result": result, "grounded": True}

    return None


def _list_intent(driver, database, question, columns, dataset_id=None):
    if not _LIST_WORDS.search(question):
        return None
    matched = _matched_columns(question, columns)
    filter_col = matched[0] if matched else None

    prefix = "MATCH (r:Row) "
    base_params = {}
    if dataset_id:
        prefix = "MATCH (d:Dataset {id: $dataset_id})-[:HAS_ROW]->(r:Row) "
        base_params["dataset_id"] = dataset_id

    op = _extract_comparison(question)
    if filter_col and op:
        number = _extract_number(question)
        if number is not None:
            col = _quote_ident(filter_col)
            cypher = f"{prefix}WHERE toFloat(r.{col}) {op} $n RETURN r LIMIT 20"
            result = _run(driver, database, cypher, {**base_params, "n": number})
            return {
                "answer": f"Found {len(result)} row(s) where {filter_col} {op} {number}.",
                "cypher": cypher,
                "result": result,
                "grounded": True,
            }

    value = _find_equality(question, filter_col, driver, database) if filter_col else None
    if value is None:
        filter_col, value = _find_any_filter(question, columns, driver, database)

    if filter_col and value is not None:
        col = _quote_ident(filter_col)
        cypher = f"{prefix}WHERE toLower(r.{col}) = toLower($value) RETURN r LIMIT 20"
        result = _run(driver, database, cypher, {**base_params, "value": value})
        return {
            "answer": f"Found {len(result)} row(s) where {filter_col} = '{value}'.",
            "cypher": cypher,
            "result": result,
            "grounded": True,
        }

    return None


def _schema_intent(driver, database, question, columns):
    if not _SCHEMA_WORDS.search(question):
        return None
    # row_index/dataset_id are internal bookkeeping fields, not real CSV columns — exclude them
    # from the raw result too, not just the phrased answer, so "View Cypher & result" stays clean.
    cypher = "MATCH (r:Row) RETURN [k IN keys(r) WHERE NOT k IN ['row_index', 'dataset_id']] AS columns LIMIT 1"
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
    return {
        "answer": f"{n} dataset{'s' if n != 1 else ''} loaded.",
        "cypher": cypher,
        "result": result,
        "grounded": True,
    }


def _cross_dataset_intent(driver, database, question: str, columns: list[str], dataset_id: str | None = None, scope: str = "single", dataset_ids: list[str] | None = None):
    is_cross_scope = scope in ("cross_dataset", "selected", "workspace") or (dataset_ids and len(dataset_ids) > 1)
    has_cross_words = bool(_CROSS_DATASET_WORDS.search(question))
    
    cypher_rels = """
    MATCH (r:DatasetRelation)
    RETURN r.id AS id, r.source_id AS source_id, r.target_id AS target_id,
           r.source_filename AS source_filename, r.target_filename AS target_filename,
           r.source_col AS source_col, r.target_col AS target_col,
           r.confidence AS confidence, r.overlap_count AS overlap_count,
           r.cardinality AS cardinality, r.status AS status,
           r.sample_matches AS sample_matches
    ORDER BY r.confidence DESC
    """
    rels = _run(driver, database, cypher_rels, {})

    rel_status_words = re.compile(r"\b(which csv.*connected|connected datasets|what relationships|show connections|show relationships|how are.*connected)\b", re.IGNORECASE)
    if rel_status_words.search(question) or (has_cross_words and ("connected" in question.lower() or "relationship" in question.lower())):
        confirmed = [r for r in rels if r["status"] == "confirmed"]
        candidates = [r for r in rels if r["status"] == "candidate"]
        if confirmed:
            desc = ", ".join(f"{r['source_filename']} ({r['source_col']}) ↔ {r['target_filename']} ({r['target_col']}) [{r.get('cardinality', '1:N')}]" for r in confirmed)
            return {
                "answer": f"Confirmed cross-dataset relationship(s): {desc}.",
                "cypher": cypher_rels,
                "result": confirmed,
                "grounded": True,
                "scope": "cross_dataset",
                "datasets_involved": list({r["source_filename"] for r in confirmed} | {r["target_filename"] for r in confirmed}),
            }
        elif candidates:
            desc = ", ".join(f"{r['source_filename']} ({r['source_col']}) ↔ {r['target_filename']} ({r['target_col']}) ({int(r['confidence']*100)}% match)" for r in candidates)
            return {
                "answer": f"Discovered candidate relationship(s) pending user confirmation: {desc}. Please review and confirm in the visualizer to enable join queries.",
                "cypher": cypher_rels,
                "result": candidates,
                "grounded": True,
                "scope": "cross_dataset",
            }
        else:
            return {
                "answer": "No relationships have been discovered or confirmed yet between datasets in this workspace.",
                "cypher": cypher_rels,
                "result": [],
                "grounded": True,
                "scope": "workspace",
            }

    if not (is_cross_scope or has_cross_words):
        return None

    if dataset_ids and len(dataset_ids) > 1:
        scoped_rels = [r for r in rels if (r["source_id"] in dataset_ids and r["target_id"] in dataset_ids)]
        if scoped_rels:
            rels = scoped_rels

    confirmed_rels = [r for r in rels if r["status"] == "confirmed"]
    candidate_rels = [r for r in rels if r["status"] == "candidate"]

    q_low = question.lower()
    selected_rel = None
    # Prioritize confirmed relations first
    for r in confirmed_rels:
        s_fn = (r.get("source_filename") or "").lower()
        t_fn = (r.get("target_filename") or "").lower()
        s_col = (r.get("source_col") or "").lower()
        t_col = (r.get("target_col") or "").lower()
        sample_m = [str(x).lower() for x in r.get("sample_matches", [])]
        if (s_fn in q_low or t_fn in q_low or s_col in q_low or t_col in q_low or any(sm in q_low for sm in sample_m)):
            selected_rel = r
            break

    if selected_rel is None:
        for r in candidate_rels:
            s_fn = (r.get("source_filename") or "").lower()
            t_fn = (r.get("target_filename") or "").lower()
            s_col = (r.get("source_col") or "").lower()
            t_col = (r.get("target_col") or "").lower()
            sample_m = [str(x).lower() for x in r.get("sample_matches", [])]
            if (s_fn in q_low or t_fn in q_low or s_col in q_low or t_col in q_low or any(sm in q_low for sm in sample_m)):
                selected_rel = r
                break

    if selected_rel is None and confirmed_rels:
        selected_rel = confirmed_rels[0]
    elif selected_rel is None and candidate_rels:
        selected_rel = candidate_rels[0]

    if selected_rel is None:
        if has_cross_words:
            return {
                "answer": "No relationships exist between these datasets. Discover and confirm a relationship in the visualizer before querying across datasets.",
                "cypher": None,
                "result": None,
                "grounded": False,
            }
        return None

    if selected_rel["status"] != "confirmed":
        return {
            "answer": f"A candidate relationship exists between '{selected_rel['source_filename']}' ({selected_rel['source_col']}) and '{selected_rel['target_filename']}' ({selected_rel['target_col']}) with {int(selected_rel['confidence']*100)}% confidence ({selected_rel['overlap_count']} overlapping keys), but it is currently unconfirmed. Please confirm it in the graph visualizer first.",
            "cypher": None,
            "result": None,
            "grounded": False,
            "join_used": {
                "source_col": selected_rel["source_col"],
                "target_col": selected_rel["target_col"],
                "status": "candidate",
                "confidence": selected_rel["confidence"],
            },
        }

    s_id = selected_rel["source_id"]
    t_id = selected_rel["target_id"]
    s_col = _quote_ident(selected_rel["source_col"])
    t_col = _quote_ident(selected_rel["target_col"])
    s_fn = selected_rel["source_filename"]
    t_fn = selected_rel["target_filename"]

    filter_value = None
    sample_matches = selected_rel.get("sample_matches") or []
    for sm in sample_matches:
        if str(sm).lower() in q_low:
            filter_value = str(sm)
            break

    filter_clause = ""
    params = {"s_id": s_id, "t_id": t_id}
    if filter_value:
        filter_clause = f"AND toLower(toString(r1.{s_col})) = toLower($val)"
        params["val"] = filter_value

    join_cypher = f"""
    MATCH (d1:Dataset {{id: $s_id}})-[:HAS_ROW]->(r1:Row),
          (d2:Dataset {{id: $t_id}})-[:HAS_ROW]->(r2:Row)
    WHERE r1.{s_col} IS NOT NULL AND toString(r1.{s_col}) = toString(r2.{t_col}) {filter_clause}
    RETURN toString(r1.{s_col}) AS join_key,
           properties(r1) AS `{s_fn}`,
           properties(r2) AS `{t_fn}`
    LIMIT 25
    """
    res = _run(driver, database, join_cypher, params)

    for rec in res:
        for k in ("row_index", "dataset_id"):
            if isinstance(rec.get(s_fn), dict):
                rec[s_fn].pop(k, None)
            if isinstance(rec.get(t_fn), dict):
                rec[t_fn].pop(k, None)

    count_matches = len(res)
    if filter_value:
        ans = f"Found {count_matches} joined record(s) matching '{filter_value}' across {s_fn} and {t_fn} on {selected_rel['source_col']}."
    else:
        ans = f"Found {count_matches} joined record(s) between {s_fn} and {t_fn} linked on confirmed key '{selected_rel['source_col']}' = '{selected_rel['target_col']}'."

    return {
        "answer": ans,
        "cypher": join_cypher,
        "result": res,
        "grounded": True,
        "scope": "cross_dataset",
        "datasets_involved": [s_fn, t_fn],
        "join_used": {
            "source_dataset": s_fn,
            "target_dataset": t_fn,
            "source_col": selected_rel["source_col"],
            "target_col": selected_rel["target_col"],
            "cardinality": selected_rel.get("cardinality", "1:N"),
            "status": "confirmed",
        },
    }


# Order matters: most specific/unambiguous intents first.
_INTENTS = [
    _cross_dataset_intent,
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


def try_answer(driver, database: str, question: str, columns: list[str], dataset_id: str | None = None, scope: str = "single", dataset_ids: list[str] | None = None) -> dict | None:
    """Returns a full /chat response dict if any intent matched the question, else None."""
    import inspect
    for intent in _INTENTS:
        try:
            sig = inspect.signature(intent)
            kwargs = {}
            if "dataset_id" in sig.parameters:
                kwargs["dataset_id"] = dataset_id
            if "scope" in sig.parameters:
                kwargs["scope"] = scope
            if "dataset_ids" in sig.parameters:
                kwargs["dataset_ids"] = dataset_ids
            result = intent(driver, database, question, columns, **kwargs)
        except Exception as e:
            log.warning("deterministic intent %s raised: %s", intent.__name__, e)
            continue
        if result is not None:
            return result
    return None
