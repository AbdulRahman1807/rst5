# Expected answers for small_clean.csv

Hand-computed from [small_clean.csv](small_clean.csv) so we can sanity-check the chatbot
(both tiers) the moment the stack is up, and reuse this directly for REPORT.md's results
table (handout requires ≥8 tested questions).

Data: 15 rows — Billing×6 (128, 340, 210, 45, 275, 310), Support×4 (75, 60, 120, 95),
Engineering×5 (990, 1500, 800, 1200, 650).

| # | Question | Expected answer | Grounded? |
|---|---|---|---|
| 1 | How many rows are there? | 15 | true |
| 2 | How many rows belong to the Billing group? | 6 | true |
| 3 | How many rows belong to Nonexistent? | 0 | true (valid zero-result aggregate) |
| 4 | What is the average amount for Billing? | 218 | true |
| 5 | What is the total amount for Engineering? | 5140 | true |
| 6 | How many rows have amount over 500? | 5 | true |
| 7 | How many rows have amount under 100? | 4 (75, 60, 45, 95) | true |
| 8 | What are the different groups? | Billing, Support, Engineering | true |
| 9 | How many rows per group? | Billing: 6, Support: 4, Engineering: 5 | true |
| 10 | Top 3 rows by amount? | Crestline Auto (1500), Quantum Labs (1200), Pioneer Foods (990) | true |
| 11 | What is the capital of France? | "I don't have that in the data" | **false** — out of schema, must not hallucinate |
| 12 | Show row 0? | Acme Co, Billing, 128 | true |

Run each of these against `/chat` once with `GROQ_API_KEY` set (LLM tier) and once with it
unset/blank (deterministic tier only) — both should get the same *correctness*, which is the
demo point for the judge who questioned why we need an LLM at all.
