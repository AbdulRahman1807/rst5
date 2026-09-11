#!/usr/bin/env bash
# Uploads the same CSV twice and confirms row/relationship counts are identical —
# the mandatory, no-partial-credit requirement (handout Part 5 / #10).
set -euo pipefail

API="${API:-http://localhost:8000}"
FILE="${1:-test_data/small_clean.csv}"

echo "Uploading $FILE (first time)..."
JOB_ID=$(curl -sf -F "file=@${FILE}" "${API}/ingest" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")
echo "job_id: $JOB_ID"

echo "Waiting for load to complete..."
for i in $(seq 1 60); do
  STATUS=$(curl -sf "${API}/status?job_id=${JOB_ID}" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
  if [ "$STATUS" = "complete" ]; then break; fi
  sleep 1
done
FIRST=$(curl -sf "${API}/status?job_id=${JOB_ID}")
echo "First load status: $FIRST"

echo "Uploading $FILE (second time)..."
JOB_ID2=$(curl -sf -F "file=@${FILE}" "${API}/ingest" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")

for i in $(seq 1 60); do
  STATUS=$(curl -sf "${API}/status?job_id=${JOB_ID2}" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
  if [ "$STATUS" = "complete" ]; then break; fi
  sleep 1
done
SECOND=$(curl -sf "${API}/status?job_id=${JOB_ID2}")
echo "Second load status: $SECOND"

if [ "$JOB_ID" != "$JOB_ID2" ]; then
  echo "FAIL: job_id changed between uploads of the same file ($JOB_ID vs $JOB_ID2) — dataset_id is not stable"
  exit 1
fi

if [ "$FIRST" != "$SECOND" ]; then
  echo "FAIL: row counts differ between first and second load of the same file"
  echo "  first:  $FIRST"
  echo "  second: $SECOND"
  exit 1
fi

echo "PASS: identical job_id and row counts across two clean loads of the same file."
