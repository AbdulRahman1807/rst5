# CSV → Kafka → Neo4j → Chat

RISE @ RST #5 hackathon project — upload any CSV, ask plain-English questions about it, get
answers grounded only in what's actually in the graph. See [REPORT.md](REPORT.md) for the full
writeup and [RISE_at_RST_5_Hackathon_Handout.md](RISE_at_RST_5_Hackathon_Handout.md) for the brief.

## Run it

```bash
cp .env.example .env
# edit .env: set GROQ_API_KEY (optional — the chatbot works without it, see SOLUTION_ANALYSIS.md
# and REPORT.md section 9.3 "Chatbot approach")
docker compose up -d --build
```

Then open **http://localhost:3000** and upload a CSV (try `test_data/small_clean.csv` first).

- API: http://localhost:8000 (`/ingest`, `/status`, `/chat`, `/health`)
- Neo4j Browser: http://localhost:7474 (user `neo4j`, password `csvgraphdb`, database `csv-graph-db`)

## Verify it

```bash
scripts/verify_idempotency.sh          # re-uploads the same file twice, checks counts match
scripts/test_hostile_input.sh          # empty/broken/non-CSV files, questions with no data
python3 scripts/seed_neo4j.py test_data/small_clean.csv   # load data directly, bypassing Kafka
```

Test fixtures are in [test_data/](test_data) — `small_clean.csv` (15 rows), `large.csv` (5,000
rows), `broken.csv` / `empty.csv` / `header_only.csv` / `not_a_csv.txt` (hostile input). Expected
chatbot answers for `small_clean.csv` are in [test_data/expected_answers.md](test_data/expected_answers.md).

## Project docs

- [PROBLEM_STATEMENT.md](PROBLEM_STATEMENT.md) — what we're solving and the full requirements
- [PRD.md](PRD.md) — must-haves, nice-to-haves, what's out of scope
- [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) — stack, message/data contracts, task list, owners
- [SOLUTION_ANALYSIS.md](SOLUTION_ANALYSIS.md) — strengths, risks, trade-offs, why this approach
- [REPORT.md](REPORT.md) — the graded submission report

## Structure

```
api/       FastAPI service — /ingest, /status, /health, /chat (two-tier chatbot)
loader/    Kafka consumer — idempotent MERGE into Neo4j
ui/        Upload/preview/chat page
scripts/   Seed data + verification scripts
test_data/ Test CSV fixtures
```
