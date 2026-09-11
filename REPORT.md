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

**Status: working end to end, tested live.** `docker compose up -d --build` brings up all 5
services; `/ingest` → Kafka → loader → Neo4j → `/status`/`/chat` all confirmed against real data
(15-row and 5,000-row files), including idempotent re-uploads and hostile-input handling. Two real
bugs were caught and fixed during this testing pass (not before) — see 9.5/9.6.

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

Observed live: `small_clean.csv` → 15 `:Row` nodes, 15 `:HAS_ROW` relationships, 1 `:Dataset` node
(re-uploading the same file a second time left all three counts unchanged — verified directly via
`cypher-shell`, not just the `/status` counters). `large.csv` → 5,000/5,000 rows loaded, 0 failed,
`status: complete`. `broken.csv`'s ragged/stray-comma rows loaded without crashing (tolerated per
design, not rejected — see 9.6).

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
| `groq` SDK version | `groq==0.37.1` | `groq==0.11.0` (initial pin) | The old version passes a now-removed `proxies` argument to `httpx.Client()`; our unpinned `httpx` resolved to 0.28.1 at build time, which rejects it. Container-only failure — host-side testing didn't catch it because the host already had a compatible version pair installed from earlier work. Caught by the first live `/chat` test after the stack came up, not before |
| Status counter idempotency | `rows_loaded`/`rows_failed` incremented only on genuine first-creation of a `:Row`/`:FailedRow` node (Cypher `FOREACH`-conditional-`SET` idiom, since Cypher has no native conditional `SET`) | Unconditional `SET d.rows_loaded = d.rows_loaded + 1` after every `MERGE` | The handout explicitly supports replaying the Kafka topic to reload the graph, and any loader restart mid-run re-consumes some already-processed messages before the next auto-commit checkpoint — both would silently double-count `rows_loaded` under the naive approach even though the underlying `:Row` nodes stayed correctly idempotent |
| How api knows kafka/neo4j are ready | Docker Compose healthchecks (`condition: service_healthy`) + retry-on-connection-refused in api/loader connection code | `depends_on` alone | `depends_on` only waits for container start, not Kafka leader election or Neo4j accepting Bolt connections |

## 9.4 Results

Tested live against `small_clean.csv` (15 rows), both chatbot tiers (LLM via Groq, and the
deterministic tier tested standalone with `GROQ_API_KEY` unset — see [test_data/expected_answers.md](test_data/expected_answers.md)
for the hand-computed expected values):

| Question asked | Answer given | Correct? | Grounded? |
|---|---|---|---|
| How many rows are there? | "There are 15 rows." | Yes | true |
| How many rows belong to the Billing group? | "There are 6 rows belonging to the Billing group." | Yes | true |
| How many rows belong to Nonexistent? | "The count is 0." | Yes | **true** (zero is a real answer) |
| What is the average amount for Billing? | "The average amount for Billing is 218.0." | Yes | true |
| What is the capital of France? | "I don't have that in the data." | Yes (correctly refused) | false |
| Write a C program to reverse a string | "I don't have that in the data." | Yes (correctly refused, see adversarial testing below) | false |
| List all rows (against `large.csv`, 5,021 rows loaded at test time) | Returned exactly 200 rows, not all 5,021 | Yes (capped by design) | true |
| How many rows are there? (deterministic tier only, `GROQ_API_KEY` unset) | "There are 15 rows in total." | Yes | true |
| How many rows belong to the Billing group? (deterministic tier only) | "There are 6 rows where group = 'Billing'." | Yes | true |
| How many rows belong to Nonexistent? (deterministic tier only) | Returns `None` from the matcher → falls through to honest "I don't have that" | Yes | false |

**One real bug this testing caught and fixed:** the deterministic tier's "how many rows belong to
Nonexistent?" originally silently fell back to the *unfiltered* total-row count instead of
recognizing the filter couldn't be resolved — answering the wrong question with a confident-sounding
number. Fixed by detecting filter-intent words ("belong", "where", "for", etc.) and returning "no
match" instead of guessing when those are present but no value resolves; see `_FILTER_INTENT_WORDS`
in `api/chatbot_deterministic.py`.

**Adversarial groundedness testing:** ran 10+ adversarial prompts against the LLM tier — general
code-generation requests, prompt injection ("ignore previous instructions"), general knowledge
questions, disguised delete requests, compound "show me X then delete it" phrasing. Every one
correctly returned `NO_QUERY` from the model. Independently verified the code-level gate
(`validate_read_only`) also rejects hand-crafted malicious Cypher that *starts* with a valid `MATCH`
but sneaks in a write clause later (`DETACH DELETE`, `WITH r DELETE r`, `SET r.amount = 0`,
`RETURN r UNION CREATE (x:Evil)`) — so the defense doesn't rely on the model behaving.

## 9.5 How we worked

Owners (see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for the full breakdown):
- **You** — LLM chatbot + Neo4j (`/chat`, both tiers, grounding logic)
- **Beni** — API + Kafka + Loader
- **Nitika** — UI + Docker/Compose + Hardening

Planned vs. actual at each timeline checkpoint:
- Skeleton (planned 0:10–0:35): took much longer than planned — `docker compose up --build` hit a slow/flaky internet connection, with the Kafka and Neo4j image pulls alone costing well over an hour of wall-clock time, including one outright pull failure (`failed to authorize: ... EOF`) that had to be retried from scratch. This pushed the whole timeline back significantly; see REPORT.md 9.6 for how we adapted (building/testing everything that didn't require the running stack in parallel while the pull continued).
- _[fill in remaining checkpoints as we hit them]_

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

One dead end: initially picked `llama-3.3-70b-versatile` as the Groq model (matching common examples online). The very first real API call returned `404 model_not_found` — it's been removed from Groq's catalog entirely. Switched to `openai/gpt-oss-120b` (confirmed available via `client.models.list()`), which then returned empty responses on short prompts — it's a reasoning model that spends tokens on hidden reasoning before the visible answer, and a low `max_tokens` budget left nothing for the actual output. Diagnosed via `usage.completion_tokens_details.reasoning_tokens` in the response, fixed by adding `reasoning_effort="low"`. Total time from first failure to working fix: a few minutes, caught before it ever touched the live stack — what told us to stop chasing it further was that the fix was immediate and complete once we found `reasoning_effort`, not a deeper architectural problem.

## 9.6 Limitations and next steps

- Container-only bugs are real and distinct from host-side testing: both the `groq`/`httpx`
  version mismatch and the `api` Dockerfile missing `COPY` for `chatbot.py`/`chatbot_deterministic.py`
  (added after the Dockerfile was first written, never wired back in) only surfaced on the first
  live container run — extensive host-side unit/integration testing of the chatbot logic beforehand
  didn't catch either, because the host environment happened to already have compatible versions and
  the right files present. Lesson: a "works on my machine" host test is not a substitute for
  actually running `docker compose up` before considering a piece done.
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
