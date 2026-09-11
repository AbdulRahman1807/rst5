# Implementation Plan

**STATUS: FROZEN — implementation starting.** Execution-focused, not architecture-focused. See [PRD.md](PRD.md) for what we're building and [PROBLEM_STATEMENT.md](PROBLEM_STATEMENT.md) for full requirements.

## Tech stack
Following the handout's suggested stack (fastest path to finishing inside the time limit):
- **Language:** Python 3.11 — [ASSUMPTION: unless team is clearly stronger in Node.js 18; both have mature Kafka + Neo4j clients]
- **API:** FastAPI (async support helps with Kafka, validates request bodies for free)
- **UI:** [To decide — Nitika] Plain HTML + fetch (fastest, zero build step) or React if she's faster in it. Either is fine per the handout; a working ugly UI beats a broken pretty one, so don't let a React build/tooling setup eat into the 0:35–1:00 window in the timeline below — if React setup stalls, fall back to plain HTML + fetch immediately rather than debugging it further.
- **Broker:** Apache Kafka, single broker, KRaft mode (`apache/kafka:3.7.0`, pinned) — no ZooKeeper needed
- **Graph DB:** Neo4j 5.x Community (`neo4j:5.24-community`, pinned), instance `CSV_Graph_DB`
- **Driver:** Official Neo4j Python driver — never hand-roll Bolt/HTTP calls
- **Chatbot:** GroqCloud API (LLM confirmed allowed) — `groq` Python SDK or plain HTTPS calls to its OpenAI-compatible endpoint. Used for text→Cypher generation and/or answer phrasing; never for answering from general knowledge.
- **Base images:** `python:3.11-slim` for api/loader; all pinned, `pip install --no-cache-dir` to keep images small

## Main components
1. **ui** — upload form, row preview, chat box.
2. **api** — `POST /ingest`, `GET /status`, `POST /chat`, `GET /health`.
3. **kafka** — topic `csv-rows`, one message per row.
4. **loader** — consumes `csv-rows`, `MERGE`s into Neo4j, tracks loaded/failed counts.
5. **neo4j** — graph store, read by api (`/chat`), written by loader.

All five wired together in one `docker-compose.yml`.

## Data & message contracts (locked)

### `dataset_id` / `job_id`
`dataset_id` = **SHA-256 hex digest of the raw uploaded CSV file's bytes**, computed by `api` in `/ingest` before publishing anything. `job_id` in the `/ingest` and `/status` contract **is the same value as `dataset_id`** — no separate ID system. Same file content → same id → re-running the same CSV naturally satisfies the idempotency/reproducibility requirements (#3, #10) without extra bookkeeping.

### Kafka message schema (topic `csv-rows`, key = `dataset_id`, one message per row)
```json
{
  "dataset_id": "9f2b...e7a1",
  "filename": "customers.csv",
  "uploaded_at": "2026-09-11T14:32:00Z",
  "row_index": 42,
  "rows_total": 1000,
  "row": { "name": "Acme Co", "group": "Billing", "amount": "128" }
}
```
- `row` is a flat string-keyed map of column → value (raw CSV strings; the loader does not need to coerce types for the MVP).
- `rows_total` is repeated on every message (not just a "header" message) so the loader can self-sufficiently create/verify the `:Dataset` node from the very first message it sees — no special-casing of message #0.
- Keying by `dataset_id` keeps all of one dataset's rows on the same partition, preserving order per dataset.

### Where job status and row counts are persisted
Two stores, cleanly separated so `api` never writes CSV content to Neo4j directly (requirement #2):
- **`api` in-memory dict** `jobs[job_id] = {rows_received, received_at}` — written synchronously during `/ingest`, before anything is published to Kafka. This is bookkeeping about *the upload itself*, not CSV content, so it doesn't violate requirement #2. Known limitation (document in `REPORT.md`): this is lost on an `api` restart — acceptable for a 3-hour demo since it only covers the brief "queued, nothing consumed yet" window.
- **Neo4j `:Dataset` node** — the durable, authoritative store for everything past "queued." The **loader** (never `api`) creates/updates it as it consumes `csv-rows`:
  ```cypher
  MERGE (d:Dataset {id: $dataset_id})
    ON CREATE SET d.filename = $filename, d.uploaded_at = $uploaded_at,
                  d.rows_total = $rows_total, d.rows_loaded = 0,
                  d.rows_failed = 0, d.status = 'loading'
  MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index})
    ON CREATE SET r += $row
  MERGE (d)-[:HAS_ROW]->(r)
  SET d.rows_loaded = d.rows_loaded + 1,
      d.status = CASE WHEN d.rows_loaded + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
  ```
  On a write failure for a row, instead run:
  ```cypher
  MATCH (d:Dataset {id: $dataset_id})
  SET d.rows_failed = coalesce(d.rows_failed, 0) + 1,
      d.status = CASE WHEN d.rows_loaded + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
  ```
- **`GET /status?job_id=X`** logic: unknown `job_id` (not in `api`'s in-memory dict) → 404. Known but no `:Dataset` node in Neo4j yet → `status: "queued"`, `rows_total` from the in-memory dict, `rows_loaded`/`rows_failed`: 0. `:Dataset` node exists → return its fields directly (Neo4j is authoritative once loading has started).

## Key technical decisions
| Decision | Choice | Why |
|---|---|---|
| Dataset identity | `dataset_id` = SHA-256 hex digest of the raw CSV bytes; `job_id` = same value | Reproducible by construction — same file always maps to the same id, no separate tracking needed |
| Idempotency key | `dataset_id + row_index`, always `MERGE` never `CREATE` | Mandatory per handout Part 5 — duplicate nodes on re-run scores zero, no partial credit |
| Status persistence | `api` holds an in-memory `{job_id: rows_received}` for the pre-load window; the loader owns the durable `:Dataset` node in Neo4j (rows_loaded/rows_failed/status) | Keeps all CSV-derived writes coming only from the loader via Kafka (requirement #2), while `/status` stays cheap to serve |
| Readiness check | Healthchecks on kafka + neo4j, `condition: service_healthy` in compose, plus retry-on-connection-refused in api/loader code | `depends_on` alone only waits for container start, not Kafka leader election or Neo4j accepting Bolt |
| Graph model | Generic `Dataset -[:HAS_ROW]-> Row` with one property per column | Handout explicitly says this is enough to pass; only enrich if time remains |
| Chatbot approach | **Two-tier**: a deterministic keyword/value→Cypher matcher (`chatbot_deterministic.py`) that needs no LLM and works standalone, plus an LLM (GroqCloud) tier used when `GROQ_API_KEY` is configured for handling more question phrasings. LLM tier falls back to the deterministic tier on any failure, which falls back to an honest "I don't have that" | An evaluator questioned "why LLM if you're not generating new data" — the honest answer is the pipeline doesn't need one; template/keyword matching against Kafka-loaded graph data already satisfies the handout's "as simple as matching a question to a Cypher query template" option. The LLM is a flexibility enhancement, not a dependency — demoable by unsetting `GROQ_API_KEY` and showing the system still answers correctly |
| Grounding enforcement | Always execute the LLM-generated Cypher for real; `grounded` is set by **code**, not by the LLM, based on whether the query executed successfully against a real schema — **not** on whether the result set is empty. A valid zero-count aggregate is still `grounded: true` | The handout requires proof, not a confident-sounding claim; zero is a legitimate answer and must not be confused with "couldn't answer" |
| Cypher safety | Validate the LLM's generated query is read-only (allowlist `MATCH`/`RETURN`/`WHERE`/aggregations). If it contains any write clause (`CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`, `DROP`) or fails validation, **reject the query outright** — do not attempt to strip/sanitize and run a mutilated version — and respond `grounded: false` | Rewriting an unsafe query into a "safe" one is itself a correctness risk (silently changes what's being asked); refusing to run it is simpler and honest, in the same spirit as Part 1 |
| LLM outage fallback | If the Groq API call fails/times out/produces an invalid or unsafe query, fall back to the deterministic matcher rather than an apology; only if *that* also can't answer does `/chat` return the honest "I don't have that" | Real degradation path, not just an apology — the deterministic tier is a fully working answer engine on its own, so a Groq hiccup shouldn't cost us a correct, grounded answer |
| Credentials | Neo4j password **and** `GROQ_API_KEY` via env var / `.env`, never hard-coded or baked into image | Explicit mandatory requirement for Neo4j creds; same standard applied to the Groq key |

## Tasks in priority order
Following the handout's own advice: **build the pipe with fake logic before the real logic.**

1. Skeleton compose file: all 5 services start with `docker compose up`.
2. **ui**: uploads any file, shows "uploaded" (no real logic yet).
3. **api** `/ingest`: accepts file, writes one dummy message to Kafka, returns fake `job_id`.
4. **loader**: consumes that one message, MERGEs a single dummy node into Neo4j, logs done.
5. **api** `/status`: hardcoded "complete" for now.
6. **api** `/chat`: hardcoded canned answer with `grounded: false`.
7. ✅ Checkpoint: confirm `docker compose up` runs all five end-to-end with dummies.
8. Replace dummies one at a time, in this order:
   a. Real ui — upload, preview rows, chat box UI.
   b. Real ingest + loader — actual CSV rows published per-row to Kafka, consumed, `MERGE`d into Neo4j.
   c. Real `/status` — true row counts (loaded + failed), real `queued/loading/complete/failed`.
   d. Real `/health` — genuinely checks Kafka + Neo4j reachability.
   e. Real `/chat` — **two-tier** (done): if `GROQ_API_KEY` is configured, send the question + schema/columns to Groq, get back a Cypher query, validate it's read-only (reject outright if not), execute it, phrase the answer from the real result. On any failure in that path, fall back to the deterministic keyword/value matcher, which builds Cypher programmatically (no LLM needed) for count/list/status-shaped questions. `grounded` is set in code from whether a query actually executed — a valid zero-result aggregate is still `grounded: true`. Return `answer` + `cypher` + `result` + `grounded`.
9. Hardening: non-root containers, pin all image tags, handle hostile input (empty CSV, header-only CSV, non-CSV file, question before upload, unanswerable question, Groq API failure/timeout).
10. Verify idempotency: run the same CSV twice, confirm identical counts.
11. Write `REPORT.md` (see handout Part 9 for exact sections).

## Owners
Three-person split. Swap the placeholder names for real ones; the ownership boundaries are the part that matters.

- **You — LLM chatbot + Neo4j (`/chat`)**
  Groq integration, schema-aware prompt design, text→Cypher generation, read-only Cypher validation (reject-not-strip), executing the query, phrasing the answer from the real result, `grounded` logic (incl. the zero-result-aggregate-is-still-grounded rule), Groq-outage fallback, Neo4j graph schema decisions.
- **Beni — API + Kafka + Loader**
  `POST /ingest` (SHA-256 `dataset_id`, publish per-row messages), `GET /status`, `GET /health`, Kafka producer/consumer wiring, the loader's `MERGE` write logic and `:Dataset` node status tracking (rows_loaded/rows_failed), idempotency verification (task 10).
- **Nitika — UI + Docker/Compose + Hardening**
  Upload/preview/chat-box UI (plain HTML+fetch or React — her call, see Tech Stack), `docker-compose.yml` (all 5 services, healthchecks, `condition: service_healthy`), pinned image tags, non-root containers, hostile-input testing (task 9), running the live demo, timekeeping (calling out each checkpoint in the timeline below).

Shared: `REPORT.md` — each person writes the section for the part they owned; one person (suggest whoever finishes their build first) assembles/edits the final file before the freeze.

## Rough timeline
Based on the handout's own schedule (Part 8) — [ASSUMPTION: treat as elapsed time from our actual start, since our literal start time may differ from the handout's example 5:00 PM]:

| Elapsed | Phase | Done when |
|---|---|---|
| 0:00–0:10 | Read handout, agree architecture, split work | Everyone knows what they own |
| 0:10–0:35 | Skeleton — 5 services, dummy logic end to end | `docker compose up` runs ui → api → kafka → loader → neo4j |
| 0:35–1:00 | Real ui — upload, preview, chat box | A real CSV can be dragged in and rows previewed |
| 1:00–1:30 | Real ingest + loader — rows reach Neo4j via Kafka | A `MATCH` query in Neo4j Browser shows real rows |
| 1:30–2:00 | Real api — `/status` true progress, `/health` honest | `curl /status` matches what's actually in the graph |
| 2:00–2:20 | Real chatbot — Groq generates Cypher + phrases answer, code enforces grounding | A real question gets a correct, grounded answer; a nonsense question honestly returns `grounded: false` |
| 2:20–2:25 | Hardening — non-root, pinned tags, hostile input | Pre-freeze checklist (handout Part 10) passes |
| 2:25–2:30 | **BUILD FREEZE** — commit and push | Nothing further edited |
| 2:30–3:00 | Report | `REPORT.md` committed |

Nitika is timekeeper — call out each checkpoint aloud as it's reached.

## What to cut first if we run out of time
In this order (stretch goals only — must-haves are not cuttable):
1. Foreign-key-style column detection / relationship enrichment.
2. Live progress bar in UI (a spinner is fine).
3. p95 latency measurement and image-size optimization.
4. Multi-stage Docker builds.
5. Skipping re-publish of already-MERGEd rows on repeat upload.
6. If truly desperate: drop the ui entirely and prove the pipeline with `curl` — handout explicitly says this beats a broken five-container system with a pretty front end.

## What must work before we polish anything
The full skeleton from tasks 1–7 above: all five services up via one `docker compose up`, dummy data flowing end-to-end through ui → api → kafka → loader → neo4j → status → chat. Nothing gets polished until this is true — this is the single most emphasized point in the handout.
