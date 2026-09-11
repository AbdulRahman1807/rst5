#!/usr/bin/env bash
# Runs every fixture in test_data/ against /ingest and /chat, checking for a clean 4xx
# instead of a crash — the hostile-input checklist (handout Part 6.7 / Part 10).
set -uo pipefail

API="${API:-http://localhost:8000}"

check_ingest() {
  local file="$1" expect_status="$2"
  code=$(curl -s -o /dev/null -w "%{http_code}" -F "file=@${file}" "${API}/ingest")
  if [ "$code" = "$expect_status" ]; then
    echo "PASS: $file -> HTTP $code"
  else
    echo "FAIL: $file -> expected HTTP $expect_status, got $code"
  fi
}

echo "--- /ingest hostile input ---"
check_ingest test_data/empty.csv 400
check_ingest test_data/header_only.csv 400
check_ingest test_data/not_a_csv.txt 400
check_ingest test_data/broken.csv 202   # ragged/stray-comma rows tolerated, not rejected — see REPORT.md limitations
check_ingest test_data/small_clean.csv 202

echo ""
echo "--- /chat hostile input ---"
echo "Question before any upload (only meaningful on a fresh stack):"
curl -s -X POST "${API}/chat" -H "Content-Type: application/json" \
  -d '{"question": "How many rows are there?"}' | python3 -m json.tool

echo ""
echo "Question with no relevant data:"
curl -s -X POST "${API}/chat" -H "Content-Type: application/json" \
  -d '{"question": "What is the capital of France?"}' | python3 -m json.tool
