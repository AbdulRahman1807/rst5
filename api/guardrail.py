import re

# Defense in depth #1: keyword reject list. Whole-word, case-insensitive.
WRITE_KEYWORDS = re.compile(
    r"\b(CREATE|MERGE|DELETE|SET|REMOVE|DROP|DETACH|FOREACH|LOAD\s+CSV|CALL\s+db\.)\b",
    re.IGNORECASE,
)
READ_START = re.compile(r"^\s*(MATCH|OPTIONAL MATCH|WITH|UNWIND|RETURN)\b", re.IGNORECASE)

NO_QUERY = "NO_QUERY"

__all__ = ["WRITE_KEYWORDS", "READ_START", "NO_QUERY", "validate_read_only"]


def validate_read_only(cypher: str) -> bool:
    """Validate that a Cypher query is read-only using current keyword-based guardrails."""
    if not cypher or cypher == NO_QUERY:
        return False
    if WRITE_KEYWORDS.search(cypher):
        return False
    if not READ_START.match(cypher):
        return False
    return True
