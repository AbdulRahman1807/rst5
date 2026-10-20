# RST5 Upgrade — Project Progress & Architecture Decisions

This document tracks phase progression, architectural decisions, verified baselines, and known issues across sessions.

---

## 1. Phase Status Summary

| Phase | Description | Status | Commit / Reference |
|---|---|---|---|
| **Phase 0** | Baseline setup, test framework, pinned deps, makefile | **COMPLETED** | `07e04c4` |
| **Phase 0.5** | Python 3.11 venv, fail-fast env vars, non-destructive `make down`, port isolation, baseline outputs | **COMPLETED** | `40cc262` |
| **Phase 1: Step 1** | Behavior-preserving extraction: `guardrail.py` & `ingest_validation.py` | **COMPLETED** | `a553342` |
| **Phase 1: Step 2** | Table-driven guardrail test suite (`tests/test_guardrail.py`) with strict `xfail` gap analysis | **COMPLETED** | 35 passed, 15 xfailed (`strict=True`), 100% guardrail coverage |
| **Phase 1: Step 3** | Unit tests for deterministic chatbot & loader with mock Neo4j | Pending | — |
| **Phase 1: Step 4** | Hypothesis property-based testing on `parse_and_validate_csv` | Pending | — |
| **Phase 1: Step 5** | Port bash scripts (`verify_idempotency`, `test_hostile_input`) to pytest (`@pytest.mark.e2e`) | Pending | — |
| **Phase 1: Step 6** | Testcontainers integration (Neo4j idempotency replay, Kafka roundtrip) | Pending | — |
| **Phase 2** | Guardrail rewrite (clause/function allowlist, code-enforced LIMIT, AST/tokenizer) | Pending | — |
| **Phase 3** | Hybrid retrieval & local embeddings (`fastembed` / `bge-small-en-v1.5`) | Pending | — |
| **Phase 4** | Evaluation benchmark & pandas-generated golden dataset (50+ queries) | Pending | — |
| **Phase 5** | Production hardening, Docker multi-stage builds, portfolio write-up | Pending | — |

---

## 2. Key Architecture Decisions & Constraints

### A. Cypher Validator & Guardrail Architecture (Phase 2 Roadmap)
- **Decision**: Use a strict clause and function **ALLOWLIST** approach (with tokenizer stripping strings and comments) rather than an unmaintainable denylist.
- **Why**: Denylists inevitably fail against obfuscation (e.g., `CALL apoc.*`, `CALL dbms.*`, unicode lookalikes, comments breaking keywords). An allowlist restricted to `MATCH`, `OPTIONAL MATCH`, `WITH`, `UNWIND`, `RETURN`, `ORDER BY`, `WHERE`, `LIMIT` and safe scalar functions (`count`, `sum`, `avg`, `min`, `max`, `toFloat`, `toLower`, `coalesce`, `size`, `round`, `abs`, `collect`, `toString`) guarantees read-only semantics.
- **LIMIT Enforcement**: LIMIT must be strictly verified or injected at code level, not requested merely via prompt or stream capping.

### B. Neo4j Community Edition Constraints
- **Constraint**: The stack runs `neo4j:5.24-community`. Role-based access control (`GRANT ... TO role`, multi-user security) is Enterprise-only.
- **Strategy**: Read safety is strictly enforced via:
  1. Application-level driver session configuration: `session.execute_read()` and `default_access_mode=neo4j.AccessMode.READ`.
  2. Guardrail validation prior to database execution.
  3. Strict dataset scoping at the validator level.
  4. Server-side transaction timeout in Neo4j config.

### C. Local Embeddings for Hybrid Search (Phase 3)
- **Constraint**: GroqCloud provides inference endpoints for LLMs (`openai/gpt-oss-120b`), but no embeddings API endpoint.
- **Decision**: Use a local embedding model via `fastembed` or `sentence-transformers` (target: `BAAI/bge-small-en-v1.5`).
- **Metric to Track**: Measure and report Docker image size impact when local model weights are baked in.

### D. Ground Truth Verification (Phase 4)
- **Decision**: Compute expected golden dataset answers programmatically with pandas over the source CSV files.
- **Why**: Eliminates human arithmetic error in benchmark metrics and ensures exact reproducibility. Cache LLM responses for repeatable evaluation runs without Groq rate limit exhaustion.

### E. Two-Tier Chatbot Architecture
- **Decision**: The chatbot uses a two-tier resolution model:
  1. Primary: Text-to-Cypher via Groq LLM when `GROQ_API_KEY` is present.
  2. Fallback: Deterministic keyword/value pattern matcher (`chatbot_deterministic.py`) which operates 100% offline without LLM dependencies.

---

## 3. Baseline Verification Evidence

### A. Regression Baselines (Current vs v0.1-hackathon)
Both `scripts/verify_idempotency.sh` and `scripts/test_hostile_input.sh` were executed against:
1. Current running stack (`main` branch @ `HEAD`, port 8000)
2. Tag `v0.1-hackathon` (worktree `.worktrees/v0.1` containerized, port 8001)

Results:
- `docs/baseline/verify_idempotency_current.txt` & `docs/baseline/verify_idempotency_v0.1.txt`: **100% Identical** (`PASS: identical job_id and row counts across two clean loads of the same file.`)
- `docs/baseline/test_hostile_input_current.txt` & `docs/baseline/test_hostile_input_v0.1.txt`: **100% Identical** (Clean 400s on empty, header_only, not_a_csv; 202 on broken, small_clean; identical chat outputs).

### B. Environment & Packaging
- Python interpreter: 3.11.17 in local `.venv`.
- Production dependencies pinned in `api/requirements.txt` and `loader/requirements.txt`.
- Development/test dependencies pinned in `requirements-dev.txt` (`pytest==8.3.3`, `hypothesis==6.115.3`, `testcontainers==4.8.2`, `ruff==0.6.9`, `mypy==1.11.2`).
- Secrets safety: `.env` verified gitignored (`git check-ignore .env` confirms).

---

## 4. Known Issues & Gaps (Specification for Phase 2 Guardrail Rewrite)

Executed table-driven test suite in `tests/test_guardrail.py` (50 total tests in suite: 35 passed, 15 strictly xfailed). Coverage on `api/guardrail.py` is **100%**.

### A. FAIL-OPEN Gaps (9 test cases — Dangerous queries currently accepted)
1. **`reject-apoc-call-mutation`**: `MATCH (r:Row) CALL apoc.refactor.renameLabel('Row', 'OldRow', [r]) RETURN count(*)` — regex only denies `CALL db.`, allowing arbitrary `CALL apoc.*` mutations.
2. **`reject-apoc-call-schema`**: `MATCH (r:Row) CALL apoc.schema.assert({}, {}) RETURN count(*)` — APOC administrative/schema modifications are unblocked.
3. **`reject-dbms-call-security`**: `MATCH (r:Row) CALL dbms.security.createUser('attacker', 'pass', false) RETURN r` — `CALL dbms.*` administrative procedures bypass regex.
4. **`reject-load-csv-comment-obfuscation`**: `MATCH (r:Row) LOAD/*trick*/CSV FROM 'http://malicious.site/data.csv' AS row RETURN row` — inline comments break the `LOAD\s+CSV` whitespace pattern.
5. **`reject-semicolon-dbms-injection`**: `MATCH (r:Row) RETURN count(r); CALL dbms.security.createUser('attacker', 'pass', false)` — semicolons are permitted, enabling chained administrative execution.
6. **`reject-semicolon-piggyback`**: `MATCH (r:Row) RETURN count(r); RETURN 1` — multi-statement Cypher queries are not rejected.
7. **`reject-missing-limit`**: `MATCH (r:Row) RETURN r` — guardrail lacks validation that node/row result queries terminate in `LIMIT <= 200`.
8. **`reject-excessive-limit`**: `MATCH (r:Row) RETURN r LIMIT 50000` — LIMIT exceeding maximum safety cap (200 rows) is accepted.
9. **`reject-unicode-fullwidth-create`**: `MATCH (r:Row) ＣＲＥＡＴＥ (n:Test)` — fullwidth Unicode homoglyphs bypass ASCII word boundaries. *(Verified live in running Neo4j container: Neo4j's Cypher parser raises a syntax error `Invalid input 'ＣＲＥＡＴＥ'` rather than executing the mutation; however, the guardrail itself failed open).*

### B. FAIL-CLOSED Gaps (6 test cases — Benign queries currently rejected)
1. **`accept-string-literal-delete`**: `MATCH (r:Row) WHERE r.action = 'delete' RETURN r LIMIT 10` — keyword inside string literal falsely triggers write rejection.
2. **`accept-string-literal-set`**: `MATCH (r:Row) WHERE r.status = 'SET' RETURN r LIMIT 10` — keyword inside string literal falsely triggers write rejection.
3. **`accept-string-literal-remove`**: `MATCH (r:Row) WHERE r.comment = 'REMOVE BEFORE FLIGHT' RETURN r LIMIT 10` — keyword inside string literal falsely triggers write rejection.
4. **`accept-comment-containing-keyword`**: `MATCH (r:Row) // CREATE comment\nRETURN count(r) LIMIT 10` — keyword inside single-line comment falsely triggers write rejection.
5. **`accept-leading-block-comment`**: `/* detailed analytics query */ MATCH (r:Row) RETURN count(r) LIMIT 10` — leading comment prevents `READ_START` regex from matching `MATCH`.
6. **`accept-backtick-property-keyword`**: `MATCH (r:Row) RETURN r.`SET` LIMIT 10` — backtick-escaped column name matching a reserved word falsely triggers write rejection.
