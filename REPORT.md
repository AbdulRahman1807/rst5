# REPORT — RISE @ RST #5: Data In, Answers Out

_Stack verified end-to-end live (fresh `docker compose down -v && up --build`, all 5 services). Results table (9.4) and 9.1/9.2 below are filled in from that live run. 9.5's timeline/dead-end and any further 9.6 items still need team input — not something a live run alone can honestly fill in._

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

**What works:** the full pipeline end-to-end via a single `docker compose up` — upload through the
UI or `curl`, rows reach Kafka, the loader `MERGE`s them into Neo4j, `/status` reports true
progress, and `/chat` answers questions grounded in the real graph. Idempotency holds under
re-upload at every scale tested (6, 15, and 5,000 rows), including two overlapping uploads racing
each other — final counts always settle to the true total, never double. All of the handout's
hostile-input CSVs (empty, header-only, non-CSV, ragged/broken) are rejected cleanly or ingested
without crashing. 11 of 12 hand-computed test questions in `test_data/expected_answers.md` are
answered correctly and `grounded` matches expectation.

**What doesn't:** the deterministic chatbot tier cannot produce a zero-result answer for a value
that genuinely isn't in the data — see the "Nonexistent group" bug under 9.6. This is a real gap
against the handout's own zero-is-a-legitimate-answer requirement, not a hypothetical edge case:
it's the team's own test case #3.

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

Observed counts from a live run (fresh volumes, one file per `Dataset`):

| File | Rows sent | `rows_loaded` | `rows_failed` | `:Row` nodes | `HAS_ROW` rels | Notes |
|---|---|---|---|---|---|---|
| `small_clean.csv` | 15 | 15 | 0 | 15 | 15 | Re-uploaded twice; counts stayed at 15, not 30 |
| `large.csv` | 5,000 | 5,000 | 0 | 5,000 | 5,000 | Re-uploaded twice, including a second upload issued before the first had fully finished loading; counts still settled to 5,000, not 10,000 |
| `broken.csv` | 6 | 6 | 0 | 6 | 6 | No header row at all, so the CSV parser treats the first data row as column names — every row lands with garbled property keys (e.g. a property literally named `"Acme Co"`) instead of `customer`/`group`/`amount`. Doesn't crash, satisfies "fails politely," but the data itself is junk. See 9.6. |
| `empty.csv` | — | — | — | — | — | Rejected at `/ingest` with 400 `"empty file"` — never reaches Kafka/Neo4j |
| `header_only.csv` | — | — | — | — | — | Rejected at `/ingest` with 400 `"CSV has a header but zero data rows"` |
| `not_a_csv.txt` | — | — | — | — | — | Rejected at `/ingest` with 400 `"file must be a .csv"` |

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

Run live against `small_clean.csv` alone in a freshly-reset graph (`docker compose down -v`), LLM
tier (`GROQ_API_KEY` set), against [test_data/expected_answers.md](test_data/expected_answers.md)'s
12 questions:

| # | Question asked | Answer given | Correct? | Grounded? |
|---|---|---|---|---|
| 1 | How many rows are there? | 15 | ✅ | true |
| 2 | How many rows belong to the Billing group? | 6 | ✅ | true |
| 3 | How many rows belong to Nonexistent? | "There are 15 rows in total." | ❌ **wrong question answered** | true (should be `count: 0`, still true) |
| 4 | What is the average amount for Billing? | 218.0 | ✅ | true |
| 5 | What is the total amount for Engineering? | 5140.0 | ✅ | true |
| 6 | How many rows have amount over 500? | 5 | ✅ | true |
| 7 | How many rows have amount under 100? | 4 | ⚠️ see note | true |
| 8 | What are the different groups? | Billing, Engineering, Support | ✅ (alphabetical, not upload order — fine) | true |
| 9 | How many rows per group? | Billing: 6, Engineering: 5, Support: 4 | ✅ | true |
| 10 | Top 3 rows by amount? | Crestline Auto (1500), Quantum Labs (1200), Pioneer Foods (990) | ✅ | true |
| 11 | What is the capital of France? | "I don't have that in the data." | ✅ | false (correctly refuses) |
| 12 | Show row 0? | Acme Co, Billing, 128 | ✅ | true |

**11/12 correct. One real bug, one fixture error — explained, not glossed over:**

- **#3 is a genuine bug**, not a fluke of this run. `chatbot_deterministic.py`'s filter matcher
  (`_find_equality` / `_find_any_filter`) only recognizes a value as a filter candidate by scanning
  the graph's *actual* distinct column values and checking if one appears in the question — so a
  value that legitimately isn't in the data, like "Nonexistent," can never match, and the question
  silently falls through to an unrelated "count all rows" intent instead of running the equality
  query (which would correctly return `count: 0`). The result is a confidently-stated wrong answer
  (still `grounded: true`) to a question the system never actually attempted — worse than an honest
  "I don't know," and the exact failure mode the handout's grounding rule exists to prevent. This is
  the team's *own* predicted test case (`expected_answers.md` #3 expects `0, true`); the code
  doesn't deliver it. Needs a fix in the LLM/chatbot owner's code before this counts as passing.
- **#7's expected answer in `expected_answers.md` (3) is itself wrong**, independently verified by
  hand: Support's values are 75, 60, 120, 95 — three of those (75, 60, 95) are under 100, plus
  Billing's 45, for **4** total, not 3. The live system's answer of 4 is the mathematically correct
  one; the test fixture's hand-computed expectation has an arithmetic slip. Recommend fixing
  `expected_answers.md` rather than the code here.

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

- **`/chat` is not scoped to a single dataset** (documented as an explicit assumption in
  `PROBLEM_STATEMENT.md`, with the caveat "revisit if we end up demoing multiple distinct CSVs side
  by side" — that caveat is now live). Once more than one CSV has ever been uploaded to a given
  Neo4j instance, aggregate questions like "how many rows are there" answer across the *union* of
  every dataset ever loaded, not just the most recent one. Confirmed live: after uploading
  `small_clean.csv` (15), `broken.csv` (6), and `large.csv` (5,000) into the same instance, "how
  many rows are there" answered 5,021. **Recommend `docker compose down -v` immediately before the
  actual demo/grading run**, so the graph starts empty and matches whatever single CSV gets
  demoed — otherwise every aggregate answer will be inflated by leftover test data.
- **The deterministic chatbot's zero-result bug** (see 9.4 #3 above) — repeated here since it's a
  correctness gap, not just a results-table footnote.
- `broken.csv` has no header row at all (by design, to test "missing header" per the handout), and
  the CSV parser can't distinguish "no header" from "a valid header" — it silently treats the first
  data row as column names. The row loads without crashing (satisfies "fails politely"), but its
  properties are garbage (e.g. a property key literally named `"Acme Co"`), which is visible if that
  row ever surfaces in a `/chat` answer. Not fixed — flagging as a known, inherent limitation of
  using a plain CSV parser for this case rather than something worth engineering around given time
  constraints.

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
