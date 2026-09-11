# Problem Statement

**STATUS: FROZEN — implementation starting.** Filled in from the RISE @ RST #5 handout ([RISE_at_RST_5_Hackathon_Handout.md](RISE_at_RST_5_Hackathon_Handout.md)). Only edit further for a confirmed decision made during the build — not speculative changes.

**Event:** RISE @ RST #5 — "Data In, Answers Out" — Build a CSV → Kafka → Neo4j Chatbot Pipeline
**Format:** 3 hours total — 2h30m build, 30m report. 5:00 PM – 8:00 PM. Hard build freeze at 7:25 PM.

## What problem are we solving?
Companies constantly receive spreadsheets (CRM exports, partner dumps, nightly support-tool downloads). Someone has to manually load that data somewhere useful, then someone else has to answer questions about it by scrolling/squinting or hand-writing queries.

We must build a pipeline that takes a CSV in the front door and lets a person ask plain-English questions about it out the back door — with nothing manual in between.

## Who has this problem?
Software companies / teams that receive ad-hoc CSV data dumps and need to query relationships in that data (e.g. "which customer is linked to which order") without manual import + query work each time.

## Why does it matter?
- Rows/columns in a spreadsheet are secretly a network (rows reference each other via repeated values) — a graph database fits that shape naturally.
- Manual load-and-query does not scale and is error prone.
- The exercise specifically tests whether we can deliver a **real running service**, not a one-off script — something another team could run themselves with one command, with proof (not a feeling) that the chatbot's answers are actually grounded in the data.

## What exactly are we expected to build?
Five services, orchestrated by one `docker-compose.yml`, with zero manual steps after `docker compose up`:

1. **ui** — browse/upload a CSV, preview received rows, open a chat box once loading starts.
2. **api** — `POST /ingest` (accept upload, publish to Kafka), `GET /status` (load progress), `POST /chat` (answer questions), `GET /health`.
3. **kafka** — single-broker topic `csv-rows`, one message per CSV row. Decouples upload from graph write.
4. **loader** — consumes the topic, `MERGE`s each row into Neo4j as it arrives (never `CREATE` — must be idempotent).
5. **neo4j** — graph DB, instance `CSV_Graph_DB`, read by `/chat`, written by the loader.

Graph model (keep boring/generic unless time allows enrichment):
```
(:Dataset {id, filename, uploaded_at}) -[:HAS_ROW]-> (:Row {row_index, col_1, col_2, ...})
```
`id` (= `dataset_id`) is the **SHA-256 hex digest of the raw CSV file bytes**. Same file content always produces the same `dataset_id`, which is what makes "two clean runs on the same CSV produce identical counts" (requirement #10) hold automatically instead of needing separate bookkeeping. Full message-schema and persistence details are in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

Chatbot: **LLM-powered**, using the GroqCloud API (confirmed allowed for this event). The LLM's job is narrow and constrained by the handout, even though it's a "proper" conversational chatbot: turn the user's English question into a Cypher query, and/or phrase the final answer — it must never answer from general knowledge. Every answer must include the actual Cypher query that was run and the raw result alongside the phrased answer (Part 1). If the graph has no answer, it must say `grounded: false` and admit it doesn't know — a confident sentence with nothing behind it scores zero on the grounding component regardless of how good the LLM sounds.

Exact API contract (request/response shapes) is specified in the handout Part 4 — see the handout file for the full JSON shapes for `/ingest`, `/status`, `/health`, `/chat`.

## Constraints / requirements given
Pulled directly from the handout (Part 7.1 "Must-have"), nothing added:

1. `docker compose up` on a clean machine brings up ui, api, kafka, neo4j and the loader starts consuming — zero manual steps.
2. The CSV reaches Neo4j **only** via the Kafka topic — never written directly from the upload handler.
3. Every row written with `MERGE` on a stable key (`dataset_id` = SHA-256 of the file + `row_index`) — no duplicates on a second load of the same file.
4. `GET /health` reports not-ok until Kafka **and** Neo4j are genuinely reachable (not just "container started").
5. `GET /status` reports real `queued | loading | complete | failed` with real row counts — not hardcoded.
6. `POST /chat` follows the Part 4 contract, including `cypher` and `result` on every answer.
7. A question with no supporting data returns `grounded: false` and says so — never guesses. **Clarification:** `grounded` tracks whether the answer came from a real, successfully-executed query against the graph — not whether the result set was non-empty. A valid aggregate that legitimately returns zero (e.g. "how many rows belong to group Foo" when no row has `group = 'Foo'`) is still `grounded: true` — zero is a real answer. `grounded: false` is for when the query can't meaningfully be answered at all: no dataset loaded yet, the question references data/columns that don't exist in the graph, or the generated Cypher fails to execute.
8. All base images pinned to a version — no `latest` anywhere.
9. Containers run as a non-root user.
10. Two clean runs against the same CSV produce identical row and relationship counts.

Other explicit constraints:
- Neo4j credentials are fixed for the event: `Database Name: CSV_Graph_DB`, `Password: csvgraphdb` — must be wired in as env vars, never hard-coded.
- Kafka: single broker, KRaft mode (no separate ZooKeeper needed).
- Report: one file, `REPORT.md`, committed before the 7:25 PM freeze, with sections specified in handout Part 9 (what we built, data & graph model, methods table, results table of ≥8 test questions, how we worked, limitations, how to run it).
- LLM use is **confirmed allowed** for this event. We're using the **GroqCloud API** (API key required — must be wired in as an env var, never hard-coded or baked into an image, same rule as the Neo4j password).
- Marking weights (100 total): compose-up-clean 15, pipeline architecture 10, healthcheck/startup ordering 8, container hygiene 7, UI end-to-end 5, API correctness 10, idempotent load 10, chatbot groundedness 10, report methods 12, report results 8, report process/honesty 5. **Engineering + report = 90 marks; chatbot cleverness = only 10.**

Suggested (not mandatory) stack from the handout: Python 3.11 or Node.js 18, plain HTML+fetch or React UI, Apache Kafka (KRaft), Neo4j 5.x Community, FastAPI/Flask/Express.

## Assumptions
_Anything below is OUR assumption, not a given requirement — flag and revisit if wrong._

- [ASSUMPTION] "Proper chatbot" means the GroqCloud LLM handles both text→Cypher generation and answer phrasing (full conversational feel), rather than only one of the two — we'll confirm this reading holds once we prototype the prompt.
- [ASSUMPTION] `/chat` queries across the entire currently-loaded graph (all datasets merged together), not scoped to a single `job_id` — the Part 4 contract doesn't take a dataset parameter. Revisit if we end up demoing multiple distinct CSVs side by side.
- [ASSUMPTION] We keep a deterministic fallback (e.g. a couple of hardcoded Cypher templates or a "grounded: false" default) for if the Groq API is unreachable/rate-limited during the demo, so a network hiccup doesn't take down `/chat` entirely.
- [ASSUMPTION] We will follow the suggested stack (Python 3.11 + FastAPI + official Neo4j driver + Kafka KRaft) unless the team is clearly stronger elsewhere.
- [ASSUMPTION] The event's actual start/end clock times may differ from the handout's example "5:00–8:00 PM" — we'll treat the handout's schedule as **relative timing** (elapsed minutes from our actual start) rather than literal clock times, unless told otherwise.
