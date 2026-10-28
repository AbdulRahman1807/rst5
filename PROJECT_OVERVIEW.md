# CSV → Kafka → Neo4j → Chatbot: Comprehensive Architecture & Project Guide

> **Project Brief:** *Data In, Answers Out* (RISE @ RST #5 Hackathon)
> **Core Objective:** Ingest arbitrary, dynamically structured CSV spreadsheets via a fault-tolerant streaming pipeline into a Neo4j knowledge graph, and provide a conversational chatbot that delivers mathematically and structurally grounded answers with verifiable Cypher queries and raw graph results—without hallucination.

---

## 1. Executive Summary & Problem Context

### 1.1 The Real-World Problem
Modern organizations routinely receive ad-hoc spreadsheets—daily exports from customer CRMs, nightly partner inventory feeds, batch billing statements, and customer support issue logs. Typically, extracting insights from these files requires either:
1. Manually opening the spreadsheet, filtering columns, and squinting at thousands of rows.
2. Manually writing one-off import scripts into a SQL or relational database and crafting complex joins.

However, **relational spreadsheets are secretly networks**:
- Repeated values in columns (e.g., `group`, `category`, `status`, `account_id`) inherently link disparate records together.
- Records reference one another through implicit keys and shared attributes.
- Rigid SQL schemas break whenever a CSV with a new or altered column structure is submitted.

A **Property Graph Database (Neo4j)** natively models this reality: entities become nodes, and relationships connect entities directly.

### 1.2 The System Mission
The mission of this project is to build an automated, zero-touch pipeline that:
- Accepts **any dynamic CSV** at the front door.
- Buffers and streams rows through an enterprise event backbone (**Apache Kafka**).
- Incrementally loads rows into **Neo4j** with strict **idempotency** (re-running the same CSV never duplicates nodes or relationships).
- Exposes a **two-tier Chatbot** out the back door that translates plain-English questions into Cypher queries, executes them against the live graph, and phrases natural answers grounded *strictly* in the returned graph records.

### 1.3 Core Engineering Philosophy
1. **Build the Pipe Before the Logic:** In high-stakes engineering under tight constraints, a system with a simple, working end-to-end skeleton beats an over-engineered isolated component. The pipeline was wired and validated from UI → API → Kafka → Loader → Neo4j before domain logic was added.
2. **Honesty Over Hallucination:** A chatbot answering from ungrounded parametric LLM memory is dangerous in business contexts. If the graph cannot answer a question, the system returns `grounded: false` and explicitly admits uncertainty. Every answer is paired with the exact Cypher query and the raw JSON payload.
3. **Decoupled Asynchrony:** Uploading a 5,000-row file must return in milliseconds (`202 Accepted`). Database blips, maintenance, or consumer slowdowns should never fail an HTTP upload.

---

## 2. High-Level Architecture & End-to-End Pipeline

The project is composed of five containerized services coordinated through a single root [docker-compose.yml](file:///Users/abdul/Desktop/College/rst5/docker-compose.yml):

```
+----------------------------------------------------------------------------------------------------+
|                                             USER BROWSER                                           |
|                                (http://localhost:3000 - Nginx / React UI)                          |
+------------------------------------+-----------------------------------+---------------------------+
                                     |                                   ^
            POST /ingest (Multipart) |                                   | GET /status & POST /chat
                                     v                                   |
+------------------------------------------------------------------------+---------------------------+
|                                        API SERVICE (FastAPI)                                       |
|                                       (http://localhost:8000)                                      |
|                                                                                                    |
|  * Computes SHA-256 (dataset_id)            * In-memory upload tracker (jobs dict)                 |
|  * Validates CSV headers & UTF-8            * Two-Tier Chat Engine:                                |
|  * Per-row publisher (confluent-kafka)        - Tier 1: GroqCloud LLM (openai/gpt-oss-120b)        |
|  * /health & /status endpoints                - Tier 2: Deterministic Keyword/Regex Engine         |
+------------------+-----------------------------------------------------+---------------------------+
                   |                                                     ^
   Produces Rows   | (Topic: `csv-rows`, Key: `dataset_id`)               | Cypher Read Queries
                   v                                                     | (Bolt protocol :7687)
+------------------------------------+                                   |
|        KAFKA EVENT BROKER          |                                   |
|       (apache/kafka:3.7.0)         |                                   |
|   - Single broker in KRaft mode    |                                   |
|   - Partitions keyed by dataset_id |                                   |
+------------------+-----------------+                                   |
                   |                                                     |
   Consumes Rows   | Consumer Group: `loader`                            |
                   v                                                     |
+------------------------------------+                                   |
|       LOADER SERVICE (Python)      |                                   |
|                                    |                                   |
|  * Consumes `csv-rows` messages    |                                   |
|  * Idempotent Cypher `MERGE`       |                                   |
|  * Atomic FOREACH status counters  |                                   |
|  * Writes to Neo4j via Bolt        |                                   |
+------------------+-----------------+                                   |
                   |                                                     |
      Bolt Writes  | MERGE (:Dataset)-[:HAS_ROW]->(:Row)                 |
                   v                                                     |
+------------------------------------------------------------------------+---------------------------+
|                                    GRAPH DATABASE (Neo4j 5.24)                                     |
|                                     (Bolt :7687 | HTTP :7474)                                      |
|                                      Database: `csv-graph-db`                                      |
|                                                                                                    |
|    (:Dataset {id, filename, uploaded_at, rows_total, rows_loaded, rows_failed, status})             |
|                                  |                                                                 |
|                              [:HAS_ROW]                                                            |
|                                  v                                                                 |
|         (:Row {dataset_id, row_index, customer: "Acme", group: "Billing", amount: "128"})         |
+----------------------------------------------------------------------------------------------------+
```

### 2.1 The End-to-End Pipeline Walkthrough

#### Step 1: Ingestion & Deterministic Hashing (`POST /ingest`)
- **Location:** [api/main.py](file:///Users/abdul/Desktop/College/rst5/api/main.py#L83-L146)
- The user drops or selects a file in the UI. The file is transmitted via `multipart/form-data` to `POST /ingest`.
- The raw byte stream is read into memory. `hashlib.sha256(raw).hexdigest()` computes the unique `dataset_id`.
- The system validates that:
  1. The file is not empty (HTTP 400).
  2. The file extension is `.csv` (HTTP 400).
  3. The content is valid UTF-8 text (HTTP 400).
  4. The CSV contains a valid header row without blank column names (HTTP 400).
  5. The file contains at least one data row (HTTP 400).
- The API records the initial upload into an in-memory dictionary: `jobs[dataset_id] = {"rows_received": rows_total, "received_at": uploaded_at}`.
- For each row, a structured JSON message is produced to Kafka on topic `csv-rows`:
  ```json
  {
    "dataset_id": "9f2b801a...",
    "filename": "customers.csv",
    "uploaded_at": "2026-10-28T11:40:00Z",
    "row_index": 0,
    "rows_total": 15,
    "row": { "customer": "Acme Co", "group": "Billing", "amount": "128" }
  }
  ```
- The message key is set to `dataset_id.encode("utf-8")`. This guarantees that **all rows for the same dataset land on the exact same Kafka partition**, preserving sequential order per dataset.
- The endpoint calls `producer.flush(timeout=30)` and immediately responds with HTTP `202 Accepted`:
  ```json
  { "job_id": "9f2b801a...", "rows_received": 15, "status": "queued" }
  ```

#### Step 2: Streaming Buffer (Kafka in KRaft Mode)
- **Location:** Service `kafka` in [docker-compose.yml](file:///Users/abdul/Desktop/College/rst5/docker-compose.yml#L2-L33)
- Kafka acts as the decoupling shock-absorber. Even if Neo4j is executing a heavy index scan or temporarily restarting, Kafka holds messages without dropping data.
- The topic can be replayed to repopulate the graph without re-uploading the source file.

#### Step 3: Idempotent Consumer & Graph Ingestion
- **Location:** [loader/main.py](file:///Users/abdul/Desktop/College/rst5/loader/main.py#L20-L138)
- The standalone `loader` worker runs an infinite polling loop on `csv-rows` with group ID `loader`.
- For each message, it connects to Neo4j database `csv-graph-db` and executes the atomic `MERGE_ROW` Cypher query:
  ```cypher
  MERGE (d:Dataset {id: $dataset_id})
    ON CREATE SET d.filename = $filename, d.uploaded_at = $uploaded_at,
                  d.rows_total = $rows_total, d.rows_loaded = 0,
                  d.rows_failed = 0, d.status = 'loading'
  MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index})
    ON CREATE SET r += $row, r._new = true
    ON MATCH SET r._new = false
  MERGE (d)-[:HAS_ROW]->(r)
  WITH d, r
  FOREACH (_ IN CASE WHEN r._new THEN [1] ELSE [] END | SET d.rows_loaded = d.rows_loaded + 1)
  REMOVE r._new
  SET d.status = CASE WHEN d.rows_loaded + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
  ```
- **Why the `FOREACH` construct?** Cypher lacks native `IF-THEN` conditional assignments. If we wrote `SET d.rows_loaded = d.rows_loaded + 1` directly, then replaying Kafka or re-uploading the file would cause `rows_loaded` to double (e.g., 15 → 30), violating idempotency! By marking `r._new = true` only `ON CREATE` and using `FOREACH` over `[1]` or `[]`, the counter only increments when a row is genuinely newly inserted.

#### Step 4: Live Status Tracking (`GET /status`)
- **Location:** [api/main.py](file:///Users/abdul/Desktop/College/rst5/api/main.py#L148-L172)
- The frontend polls `GET /status?job_id=...` every 1.5 seconds.
- Storage separation:
  - If `job_id` is in the API's in-memory `jobs` dictionary but no `:Dataset` node exists in Neo4j yet, it returns `status: "queued"`, `rows_loaded: 0`.
  - Once the loader writes the first message, Neo4j becomes the authoritative source of truth. The endpoint queries `:Dataset` and returns real-time `rows_loaded`, `rows_failed`, and `status: "loading" | "complete" | "failed"`.
  - When `rows_loaded + rows_failed >= rows_total`, status transitions to `"complete"`.

#### Step 5: Grounded Conversational Q&A (`POST /chat`)
- **Location:** [api/chatbot.py](file:///Users/abdul/Desktop/College/rst5/api/chatbot.py) and [api/chatbot_deterministic.py](file:///Users/abdul/Desktop/College/rst5/api/chatbot_deterministic.py)
- The user types a natural-language question.
- The backend triggers a **Two-Tier Architecture**:
  1. **Tier 1 (LLM via GroqCloud):** If `GROQ_API_KEY` is set, the API inspects the graph to extract active columns and a sample row, injects them into a strict system prompt, and asks Groq (`openai/gpt-oss-120b`) to produce a single read-only Cypher query.
  2. **Tier 1 Validation:** The query is tested with regex defense-in-depth (`_WRITE_KEYWORDS` reject list for `CREATE`, `MERGE`, `DELETE`, etc., and `_READ_START` allowlist for `MATCH`, `WITH`, `RETURN`). Unsafe queries are rejected outright.
  3. **Tier 1 Execution & Phrasing:** The query executes against Neo4j in read-only transaction mode (`execute_read`), capped at 200 rows. If successful, Groq phrases the answer using *only* the result payload.
  4. **Tier 2 (Deterministic Matcher):** If Groq is unconfigured, times out, rate-limited, fails, or produces invalid Cypher, execution instantly falls back to the deterministic engine (`chatbot_deterministic.py`). This engine analyzes question intent across 12 pattern matchers (counts, filters, aggregations, top-N, distinct values) and executes programmatically safe Cypher.
  5. **Grounding Flag:** `grounded` is computed in Python code—not by the LLM. It is `true` if and only if a valid query executed successfully against the database. Valid aggregate counts that evaluate to `0` are still `grounded: true`.

---

## 3. Tech Stack & Architectural Decisions

The following table details the key architectural choices, the alternatives considered, and the explicit rationale:

| Decision Area | Selected Technology / Pattern | Rejected Alternatives | Technical Rationale |
|---|---|---|---|
| **Ingestion Pipeline** | Kafka topic `csv-rows` (1 message per row) | Direct synchronous writes to Neo4j in `/ingest` | Decouples HTTP upload response from database write latency. If Neo4j is slow or briefly offline, messages buffer safely in Kafka. Allows replaying the topic to rebuild the graph. Required by hackathon rule #2. |
| **Dataset Identity** | SHA-256 Hex Digest of raw CSV bytes (`dataset_id = job_id`) | UUIDv4, Auto-increment IDs, filename strings | Deterministic and idempotent by construction. Uploading the exact same file produces the exact same `dataset_id`, ensuring repeat loads merge into existing nodes rather than creating duplicates. |
| **Idempotent Storage** | Cypher `MERGE` on `dataset_id + row_index` | Cypher `CREATE` | Hackathon rule #3 mandates no duplicate nodes on re-run. `CREATE` would duplicate graph size on every replay or server restart. `MERGE` guarantees a clean state. |
| **Chatbot Architecture** | Two-Tier: Deterministic keyword engine + optional GroqCloud LLM | LLM-only OR Regex/Template-only | Solves the evaluators' objection: *"Why do you even need an LLM if you are querying structured data?"* The deterministic tier works offline with zero dependencies. The LLM tier adds phrasing flexibility. If Groq is down or unconfigured, the system never breaks. |
| **Grounding Verification** | Derived strictly in Python code from query execution | Relying on the LLM's own conversational claim | LLMs frequently hallucinate confidence even when they lack backing facts. In this system, code executes the real Cypher; `grounded` is true only when the query genuinely executed against Neo4j. Zero-count aggregates (e.g. 0 rows found) remain grounded. |
| **Cypher Safety Gate** | Outright query rejection (`grounded: false`) on write keywords | Query sanitization, regex stripping of write clauses | Attempting to strip or sanitize an unsafe query risks silently altering user intent (e.g., turning a destructive query into an unintended read). Outright refusal is safe, honest, and auditable. |
| **Kafka Broker Mode** | Apache Kafka 3.7.0 in KRaft mode | Kafka with separate ZooKeeper container | KRaft mode eliminates the ZooKeeper container entirely, reducing resource overhead, speeding up compose boot time, and minimizing failure points. |
| **Kafka Client Library** | `confluent-kafka` (librdkafka C wrapper) | `kafka-python` | `kafka-python` has known incompatibilities and connection hangs with modern KRaft brokers. `confluent-kafka` is actively maintained, battle-tested, and robust. |
| **Neo4j Database Name** | `csv-graph-db` | Event handout name `CSV_Graph_DB` | Neo4j 5.x strictly rejects database names containing underscores at startup (`contains illegal characters`). Substituting dashes (`csv-graph-db`) is the minimal valid deviation. |
| **Container User Security** | Explicit non-root user (`useradd appuser` / `user: "7474:7474"`) | Running as `root` (default in many Docker images) | Satisfies enterprise container hygiene (hackathon rule #9). Prevents container-escape privilege escalation vulnerabilities. |
| **Docker Volume Strategy** | Explicit named volumes (`kafka_data`, `neo4j_data`, `neo4j_logs`) | Implicit anonymous container volumes | Anonymous Docker volumes were observed surviving `docker compose down -v` in certain environments, causing old graph/topic state to bleed into fresh runs. Named volumes are reliably removed by `down -v`. |
| **Service Readiness** | Healthchecks with `condition: service_healthy` + in-code connection retries | Simple `depends_on` | Docker's default `depends_on` only checks if the container process started, not whether Kafka elected a controller or Neo4j opened the Bolt port. Healthchecks prevent startup race conditions. |
| **Frontend Framework** | React 18 UMD + Babel standalone via Nginx | Node.js build pipeline (Vite / Webpack / Next.js) | Zero build step. Eliminates npm build failures, node_modules bloat, and compilation stalls. Renders instantly from static Nginx with full React component state. |

---

## 4. Graph Data Model & Idempotency Engineering

### 4.1 Graph Schema
The graph model is designed to handle **any arbitrary CSV** without requiring schema changes or prior knowledge of columns:

```
(:Dataset {id, filename, uploaded_at, rows_total, rows_loaded, rows_failed, status})
   |
   |--[:HAS_ROW]--> (:Row {dataset_id, row_index, ...dynamic CSV properties})
```

- **`:Dataset` Node:** Represents the ingested CSV file.
  - Properties: `id` (SHA-256), `filename`, `uploaded_at`, `rows_total`, `rows_loaded`, `rows_failed`, `status`.
- **`:Row` Node:** Represents an individual row within a dataset.
  - Properties: `dataset_id`, `row_index`, plus dynamic properties for each column in the CSV (e.g., `customer`, `group`, `amount`).
- **`:FailedRow` Node:** Marker node recording rows that failed to parse or write, preventing duplicate failure counting on Kafka replays.

### 4.2 Handling Missing Values & Type Sorting
Hostile or ragged CSVs often have missing values. In Neo4j Cypher, `NULL` sorts as the largest possible value.
- If a query runs `ORDER BY toFloat(r.amount) DESC`, rows with `NULL` or missing values would appear at the top!
- **Mitigation:** The system mandates `WHERE toFloat(r.prop) IS NOT NULL` before any aggregation or sorting clause.

---

## 5. Exhaustive File-by-File Breakdown

Below is a complete description of every file across the entire repository:

### Root Directory
- `docker-compose.yml`: The central orchestration specification. Defines five services (`kafka`, `neo4j`, `api`, `loader`, `ui`), private bridge network `pipeline-net`, named volumes (`kafka_data`, `neo4j_data`, `neo4j_logs`), container healthchecks, non-root user bindings, and startup dependencies (`condition: service_healthy`).
- `.env`: Environment configuration file containing runtime secrets (`GROQ_API_KEY`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`). Kept out of version control.
- `.env.example`: Template documenting required environment variables for other developers or judges.
- `README.md`: Quickstart instructions, verification commands, and repository orientation.
- `PROBLEM_STATEMENT.md`: Detailed specifications, scoring constraints, assumptions, and functional boundaries derived from the hackathon brief.
- `PRD.md`: Product Requirements Document defining must-have MVP features, stretch goals, and explicitly out-of-scope items.
- `IMPLEMENTATION_PLAN.md`: Tactical roadmap, team roles (API/Loader/Chatbot/UI), phased milestones, and data contracts.
- `SOLUTION_ANALYSIS.md`: Engineering review analyzing strengths, trade-offs, security risks, fallback paths, and judge Q&A prep.
- `REPORT.md`: The official submission report containing verified live test results, methods table, 12-question benchmark, process retrospectives, and limitations.
- `RISE_at_RST_5_Hackathon_Handout.md`: The original competition brief containing rules, API contracts, and evaluation rubrics.
- `test.csv`: Scratch test CSV used during initial development.

---

### `api/` Directory (Backend & Chat Service)
- `api/Dockerfile`: Builds the FastAPI container from `python:3.11-slim`. Installs pinned dependencies, copies Python code, creates non-root user `appuser` (`uid 1000`), switches to `appuser`, and runs `uvicorn main:app --host 0.0.0.0 --port 8000`.
- `api/requirements.txt`: Pinned backend dependencies (`fastapi==0.115.0`, `uvicorn[standard]==0.30.6`, `confluent-kafka==2.15.1`, `neo4j==5.24.0`, `python-multipart==0.0.9`, `groq==0.37.1`).
- `api/main.py`: Primary FastAPI application:
  - Initializes CORS middleware for browser access (`http://localhost:3000`).
  - Manages singleton instances for `confluent_kafka.Producer` and `neo4j.GraphDatabase.driver`.
  - `GET /health`: Actively verifies Kafka and Neo4j connectivity via live ping/probes.
  - `POST /ingest`: Hashes file, validates CSV structure, publishes rows to Kafka, and tracks initial job status.
  - `GET /status`: Queries Neo4j `:Dataset` for real-time progress (`rows_loaded`, `rows_failed`, `status`).
  - `POST /chat`: Delegates incoming natural-language questions to the chatbot subsystem.
- `api/chatbot.py`: The two-tier chatbot coordinator:
  - Integrates GroqCloud API with model `openai/gpt-oss-120b` (`reasoning_effort="low"`).
  - Inspects Neo4j schema dynamically (`build_schema_context`) to ground prompts with real column names and sample data.
  - Validates LLM Cypher output using strict regex guards (`validate_read_only`).
  - Executes queries safely via `session.execute_read` with a hard limit of `MAX_RESULT_ROWS = 200`.
  - Phrases conversational answers using Groq based strictly on the query result.
  - Automatically falls back to `chatbot_deterministic.py` on any exception, timeout, or safety violation.
- `api/chatbot_deterministic.py`: Fully independent, zero-LLM deterministic query engine:
  - Implements 12 priority-ordered intent parsers.
  - Programmatically generates safe Cypher queries using sanitized identifiers (`_quote_ident`).
  - Handles row lookups, distinct column values, group-by breakdowns, top-N rankings, numerical comparisons, and aggregates (`sum`, `avg`, `min`, `max`).
  - Features intelligent fallback filter detection (`_FILTER_INTENT_WORDS`) to avoid answering unfiltered queries when a specific filter was requested but not found.

---

### `loader/` Directory (Kafka Consumer & Neo4j Writer)
- `loader/Dockerfile`: Builds the consumer worker from `python:3.11-slim`. Installs dependencies, sets non-root user `appuser`, and runs `python main.py`.
- `loader/requirements.txt`: Pinned dependencies (`confluent-kafka==2.15.1`, `neo4j==5.24.0`).
- `loader/main.py`: The ingestion worker:
  - Implements connection retry loops with backoff for Kafka and Neo4j until both are ready.
  - Consumes messages continuously from `csv-rows`.
  - Executes `MERGE_ROW` Cypher statements to insert nodes idempotently.
  - Executes `MARK_FAILED` Cypher statements if Neo4j rejects a malformed property type, ensuring `rows_failed` increments accurately.

---

### `ui/` Directory (Browser Frontend)
- `ui/index.html`: High-polish, single-page application:
  - Built with React 18 and Babel in-browser compilation (no npm build toolchain needed).
  - Dark-mode aesthetic with Fraunces serif and Inter sans typography.
  - **Live Infrastructure Health Strip:** Polls `/health` every 10 seconds, showing live green/red status dots for API, KAFKA, and NEO4J.
  - **Drag-and-Drop Ingestion Dropzone:** Client-side preview of the first 10 rows.
  - **Real-Time 3-Column Metrics:** Displays Total Rows, Loaded to Graph, and Failed Rows with a progress bar.
  - **Interactive Question Chips:** Pre-built query suggestions ("How many rows in total?", "Show top 5 by amount").
  - **Chat Stream:** Renders the last 5 Q&A pairs, highlighting the phrased answer, a "Grounded / Not Grounded" badge, and an expandable `<details>` drawer revealing the exact Cypher query and raw JSON result.
- `ui/vendor/`: Local fallback bundle containing `react.production.min.js`, `react-dom.production.min.js`, and `babel.min.js` to ensure the UI operates even during complete internet isolation.

---

### `scripts/` Directory (Testing & Verification)
- `scripts/verify_idempotency.sh`: Automated test script that uploads `test_data/small_clean.csv` twice, monitors both jobs to completion, and confirms that `job_id` and loaded row counts are identical.
- `scripts/test_hostile_input.sh`: Tests edge cases against the API (empty CSV, header-only CSV, non-CSV file, queries before upload, ungrounded queries).
- `scripts/seed_neo4j.py`: Direct-to-Neo4j loader script used for testing the chatbot independently without starting Kafka.

---

### `test_data/` Directory (Fixtures & Benchmarks)
- `test_data/small_clean.csv`: 15-row clean fixture with columns `customer`, `group`, `amount`.
- `test_data/large.csv`: 5,000-row stress-test fixture.
- `test_data/broken.csv`: Malformed CSV missing headers and featuring ragged rows.
- `test_data/empty.csv`: 0-byte file to test rejection.
- `test_data/header_only.csv`: Column headers with 0 data rows.
- `test_data/not_a_csv.txt`: Plain text file to test format enforcement.
- `test_data/expected_answers.md`: Benchmark questions and mathematical ground-truth values for `small_clean.csv`.

---

## 6. The Two-Tier Chatbot Architecture

### 6.1 Defense-in-Depth for LLM Cypher Generation
Allowing an LLM to generate database queries introduces severe injection and mutation risks. The system implements a three-layer defense:
1. **Keyword Reject List (`_WRITE_KEYWORDS`):** Regular expression scans for whole-word occurrences of `CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`, `DROP`, `DETACH`, `FOREACH`, `LOAD CSV`, or `CALL db.`. If detected, the query is rejected immediately.
2. **Read-Start Allowlist (`_READ_START`):** The query must explicitly start with `MATCH`, `OPTIONAL MATCH`, `WITH`, `UNWIND`, or `RETURN`.
3. **Database Driver Mode:** The query is executed via `session.execute_read(...)`. Even if an adversarial prompt bypasses regex filters, Neo4j's server-side read transaction rejects any write transaction.

### 6.2 The 12 Deterministic Intents
When running in deterministic mode (or during LLM fallback), `api/chatbot_deterministic.py` evaluates the question against 12 prioritized intent functions:
1. `_row_lookup`: `"show row 5"` → `MATCH (r:Row {row_index: 5}) RETURN r`
2. `_distinct_values_intent`: `"what are the different groups"` → `MATCH (r:Row) RETURN DISTINCT r.group`
3. `_groupby_count_intent`: `"how many rows per group"` → `MATCH (r:Row) RETURN r.group, count(r)`
4. `_topn_intent`: `"top 3 by amount"` → `MATCH (r:Row) RETURN r ORDER BY toFloat(r.amount) DESC LIMIT 3`
5. `_aggregate_intent`: `"average amount for Billing"` → `MATCH (r:Row) WHERE r.group = 'Billing' RETURN avg(toFloat(r.amount))`
6. `_count_intent`: Comparison (`"amount over 500"`), equality (`"belong to Billing"`), or total (`"how many rows"`).
7. `_list_intent`: `"show rows where amount > 100"` → `MATCH (r:Row) WHERE toFloat(r.amount) > 100 RETURN r LIMIT 20`
8. `_schema_intent`: `"what columns exist"` → keys of `:Row` excluding internal bookkeeping (`row_index`, `dataset_id`).
9. `_dataset_status_intent`: `"dataset status"` → queries `:Dataset` for filenames and counts.

---

## 7. Results & Verified Hostile Input Behavior

Live validation results against `test_data/small_clean.csv` (15 rows):

| Question | Query Type | Answer Given | Result | Grounded |
|---|---|---|---|---|
| "How many rows are there?" | Total Count | 15 | Correct | `true` |
| "How many rows belong to the Billing group?" | Equality Filter | 6 | Correct | `true` |
| "How many rows belong to Nonexistent?" | Unmatched Filter | "I don't have that in the data." | Correct refusal | `false` |
| "What is the average amount for Billing?" | Aggregate (`avg`) | 218.0 | Correct | `true` |
| "What is the total amount for Engineering?" | Aggregate (`sum`) | 5140.0 | Correct | `true` |
| "How many rows have amount over 500?" | Comparison (`>`) | 5 | Correct | `true` |
| "How many rows have amount under 100?" | Comparison (`<`) | 4 | Correct (Note: 45, 60, 75, 95) | `true` |
| "What are the different groups?" | Distinct Values | Billing, Engineering, Support | Correct | `true` |
| "How many rows per group?" | Group By | Billing: 6, Engineering: 5, Support: 4 | Correct | `true` |
| "Top 3 rows by amount?" | Top-N (`DESC`) | Crestline Auto (1500), Quantum Labs (1200), Pioneer Foods (990) | Correct | `true` |
| "What is the capital of France?" | Domain Mismatch | "I don't have that in the data." | Correct refusal | `false` |
| "Show row 0?" | Index Lookup | Acme Co, Billing, 128 | Correct | `true` |

### Hostile Input Handling Matrix
- `empty.csv` (0 bytes) → **HTTP 400 Bad Request** (`"empty file"`).
- `header_only.csv` → **HTTP 400 Bad Request** (`"CSV has a header but zero data rows"`).
- `not_a_csv.txt` → **HTTP 400 Bad Request** (`"file must be a .csv"`).
- `broken.csv` (ragged columns, missing headers) → Ingested safely with HTTP 202; rows land without crashing the pipeline.
- Question before any upload → Returned honest message: `"I don't have any data yet — upload a CSV first."` (`grounded: false`).

---

## 8. Operational & Startup Guide

### 8.1 Starting the Project
Run the following commands in the workspace root:

```bash
# 1. Ensure .env is populated with required credentials
cp .env.example .env
# (Optional: Edit .env to supply your GROQ_API_KEY)

# 2. Build and start all five services in detached mode
docker compose up -d --build
```

### 8.2 Verifying Service Health
Once running, verify that all services are operational:

```bash
# Check Docker container status and health
docker compose ps

# Check the API health endpoint directly
curl http://localhost:8000/health
# Expected: {"status":"ok","kafka_connected":true,"neo4j_connected":true}
```

### 8.3 Service Endpoints and Port Mappings
| Service | Internal Port | External Port | URL / Interface | Purpose |
|---|---|---|---|---|
| **ui** | 8080 | 3000 | `http://localhost:3000` | Web Frontend (Upload, Preview, Metrics, Chat) |
| **api** | 8000 | 8000 | `http://localhost:8000` | FastAPI Endpoints (`/ingest`, `/status`, `/chat`, `/health`) |
| **neo4j** | 7474, 7687 | 7474, 7687 | `http://localhost:7474` | Neo4j Browser (User: `neo4j`, Pass: `csvgraphdb`) |
| **kafka** | 9092, 9093 | 9092 | `localhost:9092` | Kafka Broker (PLAINTEXT listener) |
| **loader** | N/A | N/A | Worker Container | Kafka Consumer & Neo4j Ingestion Engine |

### 8.4 Automated Verification Scripts
```bash
# Verify Idempotency (Uploads small_clean.csv twice and compares counts)
scripts/verify_idempotency.sh

# Test Hostile Inputs (Empty file, non-CSV, questions with no data)
scripts/test_hostile_input.sh

# Clean reset of all database and message state
docker compose down -v
```
