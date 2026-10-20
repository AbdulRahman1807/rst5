# RST5 Upgrade — Project Progress & Architecture Decisions

This document tracks phase progression, architectural decisions, verified baselines, and known issues across sessions.

---

## 1. Phase Status Summary

| Phase | Description | Status | Commit / Reference |
|---|---|---|---|
| **Phase 0** | Baseline setup, test framework, pinned deps, makefile | **COMPLETED** | `07e04c4` |
| **Phase 0.5** | Python 3.11 venv, fail-fast env vars, non-destructive `make down`, port isolation, baseline outputs | **COMPLETED** | `40cc262` |
| **Phase 1: Step 1** | Behavior-preserving extraction: `guardrail.py` & `ingest_validation.py` | **COMPLETED** | `a553342` |
| **Phase 1: Step 2** | Table-driven guardrail test suite (`tests/test_guardrail.py`) with strict `xfail` gap analysis | **COMPLETED** | 39 passed, 17 xfailed (`strict=True`), 100% guardrail coverage |
| **Phase 1: Step 3** | Unit tests for deterministic chatbot & loader with mock Neo4j | In Progress | — |
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
- **LIMIT Enforcement**: Aggregates like `RETURN count(r)` legitimately have no LIMIT. Policy decided for Phase 2: inject `LIMIT 200` when absent on row returns; reject (not clamp) literal > 200 so LLM errors are visible in Phase 4 metrics and feed Phase 3 self-repair loop.
- **Vector Search (Phase 3)**: Vector search will be executed by application code with parameterized `CALL db.index.vector...`, never generated directly by the LLM. That way, the LLM validator allowlist remains completely `CALL`-free.

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

**Caveats on Baseline Comparison**:
1. *Baseline comparison swapped only the API container*: The loader and Neo4j database were the current ones, and the database was already populated prior to execution (the chat check returned 21 rows before any upload). Therefore, this comparison demonstrates that the v0.1 API returns identical answers on pre-existing data, not that a fresh load behaves identically from scratch.
2. *Conclusive Idempotency Evidence*: Real, rigorous fresh-load idempotency evidence will be established in Phase 1 Step 6 using Testcontainers against a clean, empty database replaying the same CSV 3 times.

### B. Environment & Packaging
- Python interpreter: 3.11.17 in local `.venv`.
- Production dependencies pinned in `api/requirements.txt` and `loader/requirements.txt`.
- Development/test dependencies pinned in `requirements-dev.txt` (`pytest==8.3.3`, `hypothesis==6.115.3`, `testcontainers==4.8.2`, `ruff==0.6.9`, `mypy==1.11.2`).
- Secrets safety: `.env` verified gitignored (`git check-ignore .env` confirms).

---

## 4. Known Issues & Gaps (Specification for Phase 2 Guardrail Rewrite)

Executed table-driven test suite in `tests/test_guardrail.py` (56 total tests in suite: 39 passed, 17 strictly xfailed). Coverage on `api/guardrail.py` is **100%**.

The 17 strictly xfailed gaps collapse into four fundamental root causes. Note that while labelled "FAIL-OPEN" from a validator perspective, database-level protections (`session.execute_read()`) mitigate most against actual data corruption:

### Root Cause 1: No Lexing (Strings, Comments, and Backticks Ignored)
The validator uses simple regexes across the raw query string without tokenization or lexical analysis.
- **FAIL-OPEN (1 case — exploitable: unverified (Step 6))**:
  - `reject-load-csv-comment-obfuscation`: `MATCH (r:Row) LOAD/*trick*/CSV FROM 'http://malicious.site/data.csv' AS row RETURN row` — inline comment breaks the `LOAD\s+CSV` regex. *Likely the primary exploitable hole: LOAD CSV is a read operation permitted by read transactions and could allow SSRF from the Neo4j container.*
- **FAIL-CLOSED (6 cases — Benign queries falsely rejected)**:
  - `accept-string-literal-delete`: `MATCH (r:Row) WHERE r.action = 'delete' RETURN r LIMIT 10`
  - `accept-string-literal-set`: `MATCH (r:Row) WHERE r.status = 'SET' RETURN r LIMIT 10`
  - `accept-string-literal-remove`: `MATCH (r:Row) WHERE r.comment = 'REMOVE BEFORE FLIGHT' RETURN r LIMIT 10`
  - `accept-comment-containing-keyword`: `MATCH (r:Row) // CREATE comment\nRETURN count(r) LIMIT 10`
  - `accept-leading-block-comment`: `/* detailed analytics query */ MATCH (r:Row) RETURN count(r) LIMIT 10`
  - `accept-backtick-property-keyword`: `MATCH (r:Row) RETURN r.`SET` LIMIT 10`

### Root Cause 2: Denylist, Not Allowlist
The validator specifically checks for `CALL db.` but fails to restrict other procedure namespaces or multi-statement delimiters.
- **FAIL-OPEN (6 cases — exploitable: unverified (Step 6))**:
  - `reject-apoc-call-mutation`: `MATCH (r:Row) CALL apoc.refactor.renameLabel('Row', 'OldRow', [r]) RETURN count(*)` — APOC not blocked by regex (likely non-exploitable in practice if APOC is not installed or blocked by read transaction).
  - `reject-apoc-call-schema`: `MATCH (r:Row) CALL apoc.schema.assert({}, {}) RETURN count(*)` — APOC schema procedures not blocked.
  - `reject-apoc-tab-newline`: `MATCH (n)\n\tCALL\tapoc.refactor.renameLabel('A', 'B', [n]) RETURN count(*)` — APOC with whitespace formatting.
  - `reject-dbms-call-security`: `MATCH (r:Row) CALL dbms.security.createUser('attacker', 'pass', false) RETURN r` — `CALL dbms.*` procedures unblocked by validator (likely fails under read transaction or absent in 5.24 Community).
  - `reject-semicolon-dbms-injection`: `MATCH (r:Row) RETURN count(r); CALL dbms.security.createUser('attacker', 'pass', false)` — semicolon statement separator.
  - `reject-semicolon-piggyback`: `MATCH (r:Row) RETURN count(r); RETURN 1` — chained statements (driver typically disallows multi-statement execution).

### Root Cause 3: No LIMIT Policy (POLICY-PENDING)
The validator currently lacks a policy for result size limiting.
- **POLICY-PENDING (2 cases — Resource management, not data corruption)**:
  - `reject-missing-limit`: `MATCH (r:Row) RETURN r`
  - `reject-excessive-limit`: `MATCH (r:Row) RETURN r LIMIT 50000`
  - *Policy Decision*: Aggregates like `RETURN count(r)` legitimately need no LIMIT. Phase 2 policy: inject `LIMIT 200` when absent on row returns; reject (not clamp) literal > 200 to keep model errors visible in metrics and feed self-repair.

### Root Cause 4: No Unicode Normalization
Non-ASCII characters or zero-width joiners bypass ASCII word-boundary `\b` regexes.
- **FAIL-OPEN (2 cases — validator gap, Neo4j rejects (verified via EXPLAIN))**:
  - `reject-unicode-fullwidth-create`: `MATCH (r:Row) ＣＲＥＡＴＥ (n:Test)`
  - `reject-unicode-zerowidth-create`: `MATCH (r:Row) C\u200bREATE (n:Test)`
  - *Empirical Verification*: Both queries were tested safely using `EXPLAIN <query>` against live Neo4j 5.24 Community without executing mutations. Both queries were strictly rejected by Neo4j's Cypher parser at offset 22 with: `CypherSyntaxError: Invalid input '...' (Neo.ClientError.Statement.SyntaxError)`. Neither is exploitable against Neo4j, but both represent validator-level gaps.

