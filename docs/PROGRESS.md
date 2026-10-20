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
| **Phase 1: Step 3** | Unit tests for deterministic chatbot & loader with mock Neo4j | **COMPLETED** | 57 passed, 2 xfailed (`strict=True`), 99% chatbot / 99% loader coverage |
| **Phase 1: Step 4** | Hypothesis property-based testing on `parse_and_validate_csv` | **COMPLETED** | 10 passed, 12 xfailed (`strict=True`), 93% `ingest_validation.py` coverage |

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

---

## 5. Phase 1 Step 3 — Unit Testing, Spy Verification, Loader Coverage & Step 3 Cleanup

Implemented in-memory unit tests across `tests/test_deterministic.py` and `tests/test_loader.py` with zero Docker/network dependencies, verified with `pytest-socket` and with the Docker stack completely stopped.

### A. Unit Suite Hermeticity & Import-Time Work
- **The ~26s Baseline Run Time Explained**:
  In the initial test run, `tests/test_baseline.py::test_imports` took ~19s. Investigation with `--durations` and code inspection revealed that production modules (`api/main.py`, `api/chatbot.py`, `loader/main.py`) execute **zero network work, zero Kafka connections, and zero Neo4j driver instantiations at import time**. All drivers and producers are initialized lazily inside helper functions (`get_driver()`, `get_producer()`, `main()`).
  The delay on macOS was entirely cold-start C-extension dynamic linking (`confluent_kafka._model`, `neo4j._sync.work.workspace`, `pydantic_core`). Once cached in memory, subsequent imports and test runs complete in **~1.5 seconds**.
- **Hermeticity Verification (pytest-socket & Stack Stopped)**:
  1. Installed `pytest-socket==0.7.0` (pinned in `requirements-dev.txt`).
  2. Executed unit suite with `--disable-socket`: **102 passed, 19 xfailed in 7.20s** (zero socket calls allowed).
  3. Stopped all Docker containers via `docker compose stop` (`docker compose ps` confirmed zero running containers).
  4. Executed `make test` against the stopped stack: **102 passed, 19 xfailed in 5.75s**.

### B. Standardized Module Import Paths & Coverage (with Branch Coverage)
- **Standardized Import Paths**: All tests consistently import via `from api.<module> import ...` and `from loader.<module> import ...`, aligning with `make test`'s `--cov=api --cov=loader`.
- **Branch Coverage (`--cov-branch`) Results**:
| Module | Stmts | Miss | Branch | BrPart | Cover | Missing Lines / Uncovered Branches |
|---|---|---|---|---|---|---|
| `api/chatbot_deterministic.py` | 217 | 2 | 84 | 3 | **98%** | 109–110 (unreachable `ValueError`), 145->143, 272->284, 323->334 |
| `api/guardrail.py` | 13 | 0 | 6 | 0 | **100%** | None. All statements & branches covered. |
| `loader/main.py` | 71 | 1 | 8 | 1 | **97%** | 146 (`if __name__ == '__main__':`) |
| **Subtotal (3 Unit-Tested Modules)** | **301** | **3** | **98** | **4** | **98%** | Target unit surface thoroughly exercised |
| `api/ingest_validation.py` | 20 | 15 | 8 | 0 | **18%** | Step 4 target module (`parse_and_validate_csv`) |
| `api/chatbot.py` (LLM tier) | 97 | 73 | 20 | 0 | **21%** | Untrusted Groq LLM path (exercised in e2e/eval) |
| `api/main.py` (FastAPI routes) | 103 | 65 | 18 | 0 | **31%** | Endpoints (exercised in Step 5 e2e) |
| **OVERALL REPO TOTAL** | **521** | **156** | **144** | **4** | **69%** | Repo-wide line + branch coverage |

- **Proof of Unreachable Lines 109–110 in `api/chatbot_deterministic.py`**:
  `_NUMBER = re.compile(r"[-+]?\$?\d[\d,]*(?:\.\d+)?")`.
  In `_extract_number(question)`:
  ```python
  m = _NUMBER.search(question)
  if not m:
      return None
  try:
      return float(m.group(0).replace("$", "").replace(",", ""))
  except ValueError:
      return None
  ```
  *Analytical Proof*:
  1. `m.group(0)` matches `r"[-+]?\$?\d[\d,]*(?:\.\d+)?"`.
  2. `.replace("$", "").replace(",", "")` removes all `$` and `,` characters.
  3. The resulting string is strictly of the form `^[-+]?\d+(?:\.\d+)?$`: an optional single leading sign `+` or `-`, at least one digit `\d+`, and an optional fractional part `\.\d+`.
  4. In Python, every string conforming to this pattern is guaranteed to be parseable as a valid base-10 `float`. A lone `-` or `+` cannot match `_NUMBER` because `\d` is strictly required before any subsequent characters.
  5. Thus, `float(...)` will never raise `ValueError`. Lines 109–110 are mathematically unreachable defensive dead code. Verified via Hypothesis (`test_extract_number_never_raises`).

### C. "No User Text in Cypher" Spy Test Verification (Strengthened)
- **Scope**: Covers the **deterministic tier only**. The LLM tier's Cypher is the untrusted path, guarded by `api/guardrail.py` (see the 17 known xfail gaps).
- **Strengthened Invariants**:
  1. **Non-Vacuous Check**: Asserted that at least one recorded call executed a parameterized query using `$value` when value filtering was expected (`assert value_param_found is True`).
  2. **Canary Tokens**: Replaced short generic hostile strings with unique canary tokens (`val_x9f3_...`, `col_x9f3_...`) to guarantee zero substring collisions with Cypher keywords (`MATCH`, `Row`, `LIMIT`).
  3. **Exact Quoting Form**: Asserted that the exact output of `_quote_ident(col)` is present in the final query string and raw unquoted column text never leaks outside backticks.
  4. **Hypothesis Property Test**: Added `test_no_user_text_in_cypher_hypothesis` testing arbitrary text inputs, asserting no user value ever appears in query text.

### D. Intent Mapping: 12 Logical Intents in Source Mapped to Tests
The `chatbot_deterministic.py` docstring lists 12 logical user intents, implemented across 9 intent functions in `_INTENTS`:
1. **Row lookup** (`_row_lookup`): `test_row_lookup_found`, `test_row_lookup_not_found`, `test_row_lookup_no_pattern`
2. **Distinct values** (`_distinct_values_intent`): `test_distinct_values_intent`, `test_distinct_values_intent_no_match`
3. **Group-by count** (`_groupby_count_intent`): `test_groupby_count_intent`, `test_groupby_count_intent_no_match`
4. **Top-N** (`_topn_intent`): `test_topn_intent_desc`, `test_topn_intent_asc`, `test_topn_intent_cap`, `test_topn_intent_invalid_cap`
5. **Aggregate** (`_aggregate_intent`): `test_aggregate_intent_unfiltered`, `test_aggregate_intent_filtered`, `test_aggregate_intent_multiple_matched_columns`, `test_aggregate_intent_no_column_matched`, `test_aggregate_intent_unresolvable_filter_intent`
6. **Count with comparison** (`_count_intent`): `test_count_intent_comparison`
7. **Count with equality filter** (`_count_intent`): `test_count_intent_equality`
8. **Total count** (`_count_intent`): `test_count_intent_total`, `test_count_intent_unresolvable_filter_intent`
9. **List with comparison** (`_list_intent`): `test_list_intent_comparison`
10. **List with equality** (`_list_intent`): `test_list_intent_equality`, `test_list_intent_matched_column_but_no_filter`
11. **Schema question** (`_schema_intent`): `test_schema_intent`
12. **Dataset status** (`_dataset_status_intent`): `test_dataset_status_intent_singular_and_plural`

### E. Loader Message Handling & Poison-Pill Exposure Analysis
- **Auto-Commit & Crash Loop Mechanics**:
  `loader/main.py` initializes the Kafka consumer with `"enable.auto.commit": True`. If a malformed message (non-JSON payload) is polled at partition offset $N$, `msg = json.loads(val.decode("utf-8"))` at line 109 raises an unhandled `json.JSONDecodeError` outside any `try/except` block.
  Because the consumer process crashes immediately before offset $N$ is auto-committed, restarting the container causes the consumer to re-read from the last committed offset ($N-1$), re-poll offset $N$, and crash again in an infinite poison-pill loop.
- **Exposure Analysis**:
  **Low exposure today** because the only producer in the architecture is `api/main.py`, which serializes row dictionaries into valid JSON using Python's standard `json.dumps()`. Malformed messages could only enter the topic via manual developer debugging, out-of-band Kafka tooling, or future external producers.
- **Target Milestone for Fix**:
  Relabeled the strict xfails in `tests/test_loader.py` to `reason="fix in Phase 5: DLQ / poison pill: ..."`. The correct architectural remediation is a dead-letter topic (DLQ) in Phase 5, rather than a premature patch in Phase 2 (which is dedicated to guardrails).

---

## 6. Phase 1 Step 4 — Hypothesis Property-Based Testing on CSV Ingestion (`parse_and_validate_csv`)

Implemented property-based fuzzing and adversarial edge-case testing in `tests/test_csv_hypothesis.py` (22 tests: 10 passed, 12 strictly xfailed).
Branch coverage on `api/ingest_validation.py` increased from 18% to **93%** (20 stmts, 1 missed line 27 defensive check for `reader.fieldnames is None`, 8 branches, 1 partial branch).

### A. Universal Invariants & Fuzzing Property
- **Property Tested**: `parse_and_validate_csv(raw: bytes)` must strictly return `list[dict[str, str | None]]` or raise `CSVValidationError`. It must NEVER leak raw unhandled exceptions such as `_csv.Error`, `UnicodeDecodeError`, `KeyError`, or `ValueError`.
- **Finding (`xfail(strict=True)`)**: `test_csv_hypothesis_only_csv_validation_error_or_rows`
  Arbitrary binary fuzzing uncovers unhandled `_csv.Error` leaks:
  1. Bare CR (`\r`) in unquoted fields (`b"a,b\n1,foo\rbar\n"`) crashes with `_csv.Error: new-line character seen in unquoted field - do you need to open the file with newline=''?`.
  2. Oversized fields exceeding Python's default limit (131,072 bytes) crash with `_csv.Error: field larger than field limit (131072)`.
  *Impact*: Because `api/main.py` only catches `CSVValidationError`, any unhandled `_csv.Error` causes FastAPI to return an unhandled `500 Internal Server Error` instead of a clean `400 Bad Request`.

### B. Encodings & Line Endings
- **UTF-8 BOM Corruption (`xfail(strict=True)`)**: `test_csv_utf8_bom_stripped`
  Payload `b"\xef\xbb\xbfname,age\nAlice,30\n"` is decoded with `"utf-8"` instead of `"utf-8-sig"`. The leading Byte Order Mark is preserved as `\ufeffname`, corrupting the first column header and downstream Cypher property mapping.
- **Line Endings (PASSED)**: `test_csv_crlf_lf_mixed_endings_accepted` proves CRLF (`\r\n`), LF (`\n`), and mixed line endings are parsed identically into normalized row dictionaries.
- **Quoted Newlines (PASSED)**: `test_csv_quoted_newlines_accepted` proves multi-line quoted fields preserve internal newlines accurately.
- **Non-UTF-8 Sequences (PASSED)**: `test_csv_non_utf8_bytes_raises_validation_error` traps invalid byte sequences as `CSVValidationError("file is not valid UTF-8 text/CSV")`.

### C. Structural Gaps (Ragged Rows, Huge Fields, NUL Bytes)
- **Ragged Rows with Extra Fields (`xfail(strict=True)`)**: `test_csv_ragged_rows_extra_fields_rejected`
  For rows with more columns than the header (e.g., `b"a,b\n1,2,3\n"`), Python's `csv.DictReader` assigns extra values to the key `None`: `{'a': '1', 'b': '2', None: ['3']}`.
  *Impact*: Violates the function's return type signature (`list[dict[str, str | None]]`). When serialized via `json.dumps()`, `None` becomes `"null"`, injecting unexpected properties into the Neo4j node.
- **Ragged Rows with Missing Fields (`xfail(strict=True)`)**: `test_csv_ragged_rows_short_rows_rejected`
  For rows with fewer columns than the header (e.g., `b"a,b,c\n1,2\n"`), `csv.DictReader` silently fills missing fields with `None`: `{'a': '1', 'b': '2', 'c': None}` without validation or error.
- **NUL Bytes (`xfail(strict=True)`)**: `test_csv_nul_byte_rejected`
  NUL bytes (`\x00`) in headers or data rows are accepted by Python 3.11's CSV parser and enter dictionary keys/values unescaped.

### D. Header Edge Cases & Loader `SET r += $row` Overwrite Risk
The loader executes the following Cypher query on ingest (`loader/main.py:30-32`):
```cypher
MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index})
  ON CREATE SET r += $row, r._new = true
  ON MATCH SET r._new = false
```
Here, `$row` is the exact row dictionary emitted by `parse_and_validate_csv`.

| Header Tested | Current Behavior | Loader Overwrite & Architecture Impact | Test Status |
|---|---|---|---|
| `dataset_id` | Accepted without error | **CRITICAL IDEMPOTENCY & ISOLATION HOLE**: `r += $row` overwrites `r.dataset_id`. Downstream queries filtering by `WHERE r.dataset_id = $dataset_id` (Phase 2 isolation) will not find the row. On re-upload, `MERGE (r:Row {dataset_id: $dataset_id, ...})` creates duplicate nodes, destroying idempotency. | `xfail(strict=True)` |
| `row_index` | Accepted without error | **ORDERING HOLE**: `r += $row` overwrites `r.row_index`, corrupting row lookups (`show row 0`) and causing duplicate nodes on replay. | `xfail(strict=True)` |
| `_new` | Accepted without error | Targets loader's internal idempotency marker property. | `xfail(strict=True)` |
| Empty string `""` | Rejected (`malformed header row`) | Safe by construction. Line 28 rejects `f.strip() == ""`. | **PASSED** |
| Backtick `` ` `` | Accepted without error | Enters Neo4j as property name `` ` ``. Handled safely in deterministic tier via `_quote_ident("`") -> "````"`, but risky for LLM tier. | **PASSED** |
| 10k-char header | Accepted without error | Creates 10,000-character Neo4j property name, stressing database schema and memory. | **PASSED** |
| Duplicate headers (exact) | Accepted without error | **SILENT DATA LOSS**: `csv.DictReader` silently keeps the last value for duplicate headers (e.g. `name,age,name\nAlice,30,Bob` produces `{'name': 'Bob', 'age': '30'}`; `Alice` is permanently dropped). | `xfail(strict=True)` |
| Duplicate headers (case/space) | Accepted without error | `Name, name ` creates ambiguous, near-identical property names. | `xfail(strict=True)` |



