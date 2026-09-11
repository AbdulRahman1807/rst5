import logging
import os
import re

from groq import Groq

import chatbot_deterministic

log = logging.getLogger("chatbot")

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")

_client: Groq | None = None

# Defense in depth #1: keyword reject list. Whole-word, case-insensitive.
_WRITE_KEYWORDS = re.compile(
    r"\b(CREATE|MERGE|DELETE|SET|REMOVE|DROP|DETACH|FOREACH|LOAD\s+CSV|CALL\s+db\.)\b",
    re.IGNORECASE,
)
_READ_START = re.compile(r"^\s*(MATCH|OPTIONAL MATCH|WITH|UNWIND|RETURN)\b", re.IGNORECASE)

NO_QUERY = "NO_QUERY"

SYSTEM_PROMPT = """You translate a user's plain-English question into a single read-only Cypher query \
for a Neo4j graph with exactly this schema:

  (:Dataset {{id, filename, uploaded_at, rows_total, rows_loaded, rows_failed, status}})
    -[:HAS_ROW]->
  (:Row {{row_index, ...one property per CSV column}})

The columns actually present on :Row nodes right now are: {columns}
Example row data: {sample_row}

Rules:
- Output ONLY the Cypher query. No explanation, no markdown code fences, no commentary.
- The query must be read-only: MATCH/OPTIONAL MATCH/WITH/UNWIND/RETURN only. Never CREATE, MERGE, \
DELETE, SET, REMOVE, DROP, or any write.
- Only reference the labels, relationship type, and properties listed above. Never invent a property \
that isn't in the columns list.
- If the question cannot be answered from this schema at all (asks about something with no relation to \
this data, or requires information not present), output exactly: {no_query}
- A question whose honest answer is zero or empty (e.g. counting rows that don't exist) is still \
answerable — write the query normally; do not use {no_query} just because the answer might be zero.
"""

PHRASE_PROMPT = """You are phrasing a chatbot's answer. You will be given the user's question, the \
Cypher query that was run against a Neo4j graph, and the raw JSON result of that query.

Write a short, direct 1-2 sentence answer using ONLY the data in the result. Do not add any outside \
information or general knowledge. If the result is empty or a count/aggregate is zero, say so plainly \
(e.g. "There are no rows where ..." or "The count is 0."). Do not apologize, do not hedge beyond what \
the data shows.

Question: {question}
Cypher: {cypher}
Result: {result}

Answer:"""


def get_client() -> Groq:
    global _client
    if _client is None:
        _client = Groq(api_key=GROQ_API_KEY)
    return _client


def build_schema_context(driver, database: str) -> tuple[list[str], dict]:
    """Look at one real Row node to learn the current CSV's columns, for prompt grounding."""
    with driver.session(database=database) as session:
        record = session.run(
            "MATCH (r:Row) RETURN keys(r) AS keys, r AS row LIMIT 1"
        ).single()
    if record is None:
        return [], {}
    columns = [k for k in record["keys"] if k not in ("row_index", "dataset_id")]
    sample_row = {k: v for k, v in dict(record["row"]).items() if k in columns}
    return columns, sample_row


def has_any_data(driver, database: str) -> bool:
    with driver.session(database=database) as session:
        record = session.run("MATCH (d:Dataset) RETURN count(d) AS c").single()
    return record is not None and record["c"] > 0


def generate_cypher(question: str, columns: list[str], sample_row: dict) -> str:
    system = SYSTEM_PROMPT.format(columns=columns, sample_row=sample_row, no_query=NO_QUERY)
    resp = get_client().chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": question},
        ],
        temperature=0,
        max_tokens=300,
        timeout=8,
    )
    raw = resp.choices[0].message.content.strip()
    # Strip markdown code fences if the model added them despite instructions — formatting only,
    # never a semantic rewrite of the query (see IMPLEMENTATION_PLAN.md "reject, don't strip").
    raw = re.sub(r"^```(?:cypher)?\s*|\s*```$", "", raw, flags=re.IGNORECASE).strip()
    return raw


def validate_read_only(cypher: str) -> bool:
    if not cypher or cypher == NO_QUERY:
        return False
    if _WRITE_KEYWORDS.search(cypher):
        return False
    if not _READ_START.match(cypher):
        return False
    return True


def execute_cypher(driver, database: str, cypher: str) -> list[dict]:
    def _run(tx):
        result = tx.run(cypher)
        return [record.data() for record in result]

    with driver.session(database=database) as session:
        # access_mode READ is enforced by execute_read at the server too — defense in depth #2.
        return session.execute_read(_run)


def phrase_answer(question: str, cypher: str, result: list[dict]) -> str:
    prompt = PHRASE_PROMPT.format(question=question, cypher=cypher, result=result[:50])
    resp = get_client().chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=200,
        timeout=8,
    )
    return resp.choices[0].message.content.strip()


def _groq_configured() -> bool:
    return bool(GROQ_API_KEY) and GROQ_API_KEY != "your_groq_api_key_here"


def _try_llm(driver, database: str, question: str, columns: list[str], sample_row: dict) -> dict | None:
    """Returns a full /chat response dict on success, or None if the LLM path couldn't
    produce a grounded answer — caller falls back to the deterministic matcher, not to
    an apology, so a Groq hiccup degrades gracefully instead of just giving up."""
    try:
        cypher = generate_cypher(question, columns, sample_row)
    except Exception as e:
        log.warning("groq generate_cypher failed: %s", e)
        return None

    if not validate_read_only(cypher):
        return None  # let the deterministic matcher try; if it also can't, that's the honest "I don't know"

    try:
        result = execute_cypher(driver, database, cypher)
    except Exception as e:
        log.warning("cypher execution failed: %s | query=%s", e, cypher)
        return None

    # Query executed successfully against the real graph -> grounded, regardless of whether
    # the result is empty or a zero-count aggregate (locked in PROBLEM_STATEMENT.md req #7).
    try:
        answer = phrase_answer(question, cypher, result)
    except Exception as e:
        log.warning("groq phrase_answer failed: %s", e)
        answer = f"Result: {result}"  # still grounded — we have real data, just no LLM phrasing

    return {"answer": answer, "cypher": cypher, "result": result, "grounded": True}


def answer_question(driver, database: str, question: str) -> dict:
    """Two-tier: LLM (Groq) when configured, deterministic keyword/value matcher always as
    the guaranteed fallback (and the whole system, if GROQ_API_KEY isn't set at all) — so the
    pipeline answers grounded questions correctly with zero LLM dependency, and the LLM is
    strictly an enhancement for handling more phrasings, not a requirement. See
    SOLUTION_ANALYSIS.md for why."""
    if not question or not question.strip():
        return {"answer": "Ask me something about the uploaded data.", "cypher": None, "result": None, "grounded": False}

    try:
        if not has_any_data(driver, database):
            return {"answer": "I don't have any data yet — upload a CSV first.", "cypher": None, "result": None, "grounded": False}

        columns, sample_row = build_schema_context(driver, database)

        if _groq_configured():
            llm_result = _try_llm(driver, database, question, columns, sample_row)
            if llm_result is not None:
                return llm_result
            log.info("LLM path unavailable/unhelpful for this question, falling back to deterministic matcher")

        det_result = chatbot_deterministic.try_answer(driver, database, question, columns)
        if det_result is not None:
            return det_result

        return {"answer": "I don't have that in the data.", "cypher": None, "result": None, "grounded": False}

    except Exception as e:
        log.error("unexpected /chat failure: %s", e)
        return {"answer": "I couldn't process that right now — try again in a moment.", "cypher": None, "result": None, "grounded": False}
