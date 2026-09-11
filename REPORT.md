# REPORT — RISE @ RST #5: Data In, Answers Out

_Draft — filled in as far as possible before the stack is verified end-to-end. Results table (9.4) and parts of 9.5/9.6 need a live run to finish; everything else below is locked._

## 9.1 What we built

A CSV → Kafka → Neo4j → Chat pipeline: upload any CSV through the UI, it's published row-by-row
to a Kafka topic, a loader consumes that topic and idempotently `MERGE`s each row into Neo4j as
a generic `Dataset -[:HAS_ROW]-> Row` graph, and a chatbot answers plain-English questions about
the loaded data — grounded only in what's actually in the graph, never in general knowledge. The
chatbot is two-tier: a deterministic keyword/value matcher that works with zero LLM dependency,
plus an optional GroqCloud LLM layer for handling more question phrasings, with automatic fallback
to the deterministic tier if the LLM is unavailable.

Architecture: `ui` (browser) → `api` (`/ingest`, `/status`, `/chat`, `/health`) → `kafka`
(topic `csv-rows`, one message per row) → `loader` (consumer, writes to Neo4j) → `neo4j`
(`csv-graph-db` — event's fixed name "CSV_Graph_DB" contains underscores, which Neo4j 5.x
rejects in database names; read by both the loader and `api`'s `/chat`).

_[Fill in once tested: what works, what doesn't, honestly.]_

## 9.2 The data and the graph model

Test CSVs (see `test_data/`): `small_clean.csv` (15 rows, 3 columns: customer/group/amount),
`large.csv` (5,000 rows, same schema, randomly generated), `broken.csv` (ragged columns, stray
commas, missing header), `empty.csv`, `header_only.csv`, `not_a_csv.txt`.

Graph model — generic, one property per CSV column, unchanged regardless of what CSV is uploaded:
```
(:Dataset {id, filename, uploaded_at, rows_total, rows_loaded, rows_failed, status})
  -[:HAS_ROW]->
(:Row {row_index, dataset_id, ...one property per CSV column})
```
`id` (`dataset_id`) is the SHA-256 hex digest of the raw CSV file bytes — same file always maps
to the same id, which is what makes idempotent re-loading work without extra bookkeeping.

_[Fill in once tested: actual row/relationship counts observed for each test file.]_

## 9.3 Methods

| Decision | Chosen | Rejected | Reason |
|---|---|---|---|
| Ingest path | Kafka topic `csv-rows`, one message per row, api never writes to Neo4j directly | Writing straight from the upload handler to Neo4j | Requirement #2; also loses decoupling (upload returns immediately, DB blips don't drop data, topic is replayable) |
| Idempotency key | `dataset_id` (SHA-256 of file) + `row_index`, always `MERGE` | A generated/random job id per upload | Reproducible by construction — re-running the same file naturally produces the same id, no separate tracking table needed |
| Chatbot approach | Two-tier: deterministic keyword/value→Cypher matcher (always available) + optional GroqCloud LLM layer on top | LLM-only chatbot | An evaluator directly questioned "why do you need an LLM if you're not generating new data" — the deterministic tier proves we don't need one, while the LLM adds flexibility for phrasings the matcher misses |
| Cypher safety | LLM-generated queries validated read-only; **rejected outright** if unsafe/invalid (never stripped/rewritten) | Sanitizing/rewriting unsafe queries into safe ones | Rewriting risks silently answering a different question than what was asked; refusing is simpler and honest |
| Groundedness | `grounded` computed in code from whether the query actually executed against the graph — a valid zero-result aggregate is still `grounded: true` | Trusting the LLM's own claim of groundedness | The handout requires proof, not a confident-sounding claim; zero is a legitimate answer, not "couldn't answer" |
| Kafka client | `confluent-kafka` (librdkafka) | `kafka-python` | `kafka-python`'s maintenance/compatibility with newer KRaft-mode brokers is a known risk; `confluent-kafka` is the actively-maintained, battle-tested client |
| Neo4j database name | `csv-graph-db` | Event's literal fixed name `CSV_Graph_DB` | Neo4j 5.x database names may only contain letters, digits, dots, and dashes — no underscores. The literal name is rejected by Neo4j itself at startup; substituting dashes is the minimal necessary deviation, documented here per the handout's request to explain stack deviations |
| Status counter idempotency | `rows_loaded`/`rows_failed` incremented only on genuine first-creation of a `:Row`/`:FailedRow` node (Cypher `FOREACH`-conditional-`SET` idiom, since Cypher has no native conditional `SET`) | Unconditional `SET d.rows_loaded = d.rows_loaded + 1` after every `MERGE` | The handout explicitly supports replaying the Kafka topic to reload the graph, and any loader restart mid-run re-consumes some already-processed messages before the next auto-commit checkpoint — both would silently double-count `rows_loaded` under the naive approach even though the underlying `:Row` nodes stayed correctly idempotent |
| How api knows kafka/neo4j are ready | Docker Compose healthchecks (`condition: service_healthy`) + retry-on-connection-refused in api/loader connection code | `depends_on` alone | `depends_on` only waits for container start, not Kafka leader election or Neo4j accepting Bolt connections |

## 9.4 Results

_[Fill in after a live run — see [test_data/expected_answers.md](test_data/expected_answers.md)
for the ≥8 pre-computed questions/expected answers against `small_clean.csv`, tested against
both chatbot tiers.]_

| Question asked | Answer given | Correct? | Grounded? |
|---|---|---|---|
| | | | |

_[The explanation of any failures carries more marks than the table itself — fill in honestly.]_

## 9.5 How we worked

Owners (see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for the full breakdown):
- **You** — LLM chatbot + Neo4j (`/chat`, both tiers, grounding logic)
- **Beni** — API + Kafka + Loader
- **Nitika** — UI + Docker/Compose + Hardening

Planned vs. actual at each timeline checkpoint: _[fill in as we hit them]_

Two decisions in detail:

**Decision 1: Two-tier chatbot instead of LLM-only.**
- Options considered: LLM-only (simplest to build), template-only (safest, no external dependency), two-tier (both).
- Chosen because: directly answers real evaluator skepticism about LLM necessity, while still getting the LLM's phrasing flexibility.
- Cost accepted: more code to write and maintain, two code paths to keep in sync with the same schema.
- Would revisit if: time ran out before the deterministic tier was robust — would have shipped LLM-only with the pre-existing single-tier fallback (an apology message) instead.

**Decision 2: `confluent-kafka` instead of `kafka-python`.**
- Options considered: `kafka-python` (simpler API, what we started with), `confluent-kafka` (librdkafka-based).
- Chosen because: known compatibility risk between `kafka-python` and modern KRaft-mode brokers, discovered during code review before the stack was even running — not worth risking a live-demo Kafka connection failure over.
- Cost accepted: slightly different API (callback-based produce, poll-based consume) required rewriting the producer/consumer code.
- Would revisit if: `confluent-kafka`'s wheel hadn't installed cleanly on our target image (it did).

One dead end: _[fill in — what we tried, when we abandoned it, what told us to stop]_

## 9.6 Limitations and next steps

- The generic `Dataset -[:HAS_ROW]-> Row` model doesn't detect foreign-key-style columns and turn
  them into real relationships — every column is a flat property regardless of whether it
  logically references another row.
- `api`'s job-status bookkeeping (`rows_received` for the "queued" state) is in-memory and lost on
  an `api` restart — acceptable for a 3-hour demo since Neo4j is authoritative once loading starts,
  but wouldn't survive a real restart mid-upload in production.
- The deterministic chatbot tier only recognizes question shapes we anticipated (count, list,
  aggregate, group-by, top-N, distinct values, row lookup, schema, dataset status) — a question
  phrased outside those shapes falls through to the LLM tier (if configured) or an honest "I don't
  have that."
- We have no way to detect that a re-uploaded file with the same name but different content should
  be treated differently from one with genuinely identical content — `dataset_id` is purely a
  content hash, so two different files that happen to hash-collide (practically impossible) or a
  same-name-different-content re-upload both just work as expected/new datasets respectively; this
  is fine for us but worth stating explicitly.

_[Fill in more as we discover them during testing.]_

## 9.7 How to run it

```bash
git clone https://github.com/AbdulRahman1807/rst5
cd rst5
cp .env.example .env
# edit .env: set GROQ_API_KEY (optional — system works without it, see SOLUTION_ANALYSIS.md)
docker compose up -d --build
```

Then open `http://localhost:3000` (ui), or test directly:
```bash
curl -F "file=@test_data/small_clean.csv" http://localhost:8000/ingest
curl "http://localhost:8000/status?job_id=<job_id from above>"
curl -X POST http://localhost:8000/chat -H "Content-Type: application/json" \
  -d '{"question": "How many rows belong to the Billing group?"}'
```

Neo4j Browser: `http://localhost:7474` (user `neo4j`, password `csvgraphdb`, database `csv-graph-db`).

To verify idempotency: run `scripts/verify_idempotency.sh` (uploads `small_clean.csv` twice,
diffs the resulting row/relationship counts).
