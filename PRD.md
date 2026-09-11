# PRD

**STATUS: FROZEN — implementation starting.** Filled in from the RISE @ RST #5 handout. Prioritized ruthlessly for a 3-hour build; see [PROBLEM_STATEMENT.md](PROBLEM_STATEMENT.md) for full source detail.

## Main goal
Ship a `docker compose up`-able pipeline where a user uploads any CSV and gets a chatbot that answers plain-English questions **grounded only in what's actually in Neo4j** — with proof (Cypher + raw result) attached to every answer, and an honest "I don't know" when the graph can't answer.

## Who will use it
- The judges, running our `docker compose up` cold on their own machine against a CSV we've never seen (or they run our test files).
- Hypothetical end user: someone at a company who has a CSV and questions about it, no SQL/Cypher knowledge required.

## Main user journey
1. Open the ui in a browser.
2. Drag in a CSV → `POST /ingest` → gets back `job_id` (= SHA-256 hash of the file, see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md)), `rows_received`, `status: queued`.
3. See a preview of the uploaded rows and live-ish progress via `GET /status` (queued → loading → complete).
4. Open the chat box, ask a question (e.g. "How many rows belong to the Billing group?").
5. Get back an answer + the Cypher query + the raw result + `grounded: true/false`.

## Must-have features (MVP)
Directly from the handout's Part 7.1 (this **is** our MVP definition — not up for cutting):
- [ ] `docker compose up` brings up ui, api, kafka, neo4j; loader auto-starts consuming. Zero manual steps.
- [ ] CSV reaches Neo4j only via the Kafka topic (never written directly by the upload handler).
- [ ] Loader writes every row with `MERGE` on a stable key (`dataset_id + row_index`) — no duplicates on re-load.
- [ ] `GET /health` — honest, reports not-ok until Kafka + Neo4j are truly reachable.
- [ ] `GET /status` — real `queued|loading|complete|failed` with real row counts (loaded + failed = received before "complete").
- [ ] `POST /chat` — matches the Part 4 contract exactly (`answer`, `cypher`, `result`, `grounded`).
- [ ] Chatbot is LLM-powered via the GroqCloud API: LLM turns the question into Cypher (and/or phrases the final answer), Cypher is actually executed against Neo4j, and the phrased answer is grounded in the real result — never in the LLM's general knowledge.
- [ ] Ungrounded questions return `grounded: false` and an honest "I don't have that in the data" — never a guess, even if the LLM would otherwise produce a plausible-sounding sentence. (A valid zero-result aggregate, e.g. a count of 0, is still `grounded: true` — see [PROBLEM_STATEMENT.md](PROBLEM_STATEMENT.md) requirement #7 clarification.)
- [ ] All base images pinned to a version (no `latest`).
- [ ] Containers run as non-root.
- [ ] Idempotent: two clean runs on the same CSV → identical row/relationship counts.
- [ ] Basic UI: upload, preview rows, chat box — all functioning in-browser.
- [ ] `REPORT.md` completed per handout Part 9, committed before the freeze.

## Nice-to-have features
Only after every must-have above is solid (handout Part 7.2 "Stretch"):
- [ ] api image under 400 MB.
- [ ] Measured p95 `/ingest` response time under 200ms for a 10k-row file (method stated in report).
- [ ] UI shows live progress (not just a spinner) while a large file loads.
- [ ] Graceful handling of the full hostile-input list (empty CSV, header-only CSV, non-CSV file, question with no data, question before any upload).
- [ ] Foreign-key-style column detection → real relationships instead of flat properties.
- [ ] Re-running `docker compose up` on the same file skips re-publishing already-MERGEd rows.
- [ ] Multi-stage Docker builds (build tools never ship in runtime image).

## Explicitly out of scope
- Any chatbot logic that answers from general knowledge instead of the graph — forbidden, not just deprioritized.
- Multiple Kafka brokers or a separate ZooKeeper container (single broker, KRaft mode, is sufficient per the handout).
- A "beautifully normalized" graph schema — generic `Dataset -[:HAS_ROW]-> Row` is enough to pass; enrichment is stretch-only.
- Letting the LLM answer from its own general knowledge, or skip executing a real Cypher query — forbidden by the handout regardless of how "proper" the conversational experience should feel.
- Multi-turn conversation memory / follow-up question context — each question is handled independently unless we have time left over (not in the handout's contract).
- Performance/scale work beyond the stated stretch goals (this is a 3-hour build, not a production system).

## What counts as "working" for the demo
- `docker compose down -v && docker compose up` succeeds cold, no manual steps.
- A CSV we haven't shown the judges before can be dragged in, previewed, and loaded — visible via `/status` and confirmed by a `MATCH` query in Neo4j Browser.
- At least one real question about the uploaded file gets a correct, grounded answer (answer + cypher + result shown).
- A question with no supporting data honestly returns `grounded: false` instead of a fabricated answer.
- Re-running against the same CSV does not duplicate nodes.
