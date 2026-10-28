#!/usr/bin/env python3
"""Reproducible Ingestion Benchmark Script

Measures:
- File size (bytes)
- Number of rows
- Ingestion endpoint response time (upload + Kafka publish)
- Time to first row persisted in Neo4j
- Total pipeline completion time (from POST start to status == 'complete')
- Sustained throughput (persisted rows/sec)
- Failed rows
- Output in machine-readable JSON and formatted table.
"""
import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.parse
import mimetypes

API_DEFAULT = "http://localhost:8000"


def post_multipart(url: str, filepath: str) -> tuple[dict, float, int]:
    """Posts file using multipart/form-data with monotonic timing."""
    filename = os.path.basename(filepath)
    with open(filepath, "rb") as f:
        file_bytes = f.read()

    file_size = len(file_bytes)
    boundary = "----BenchmarkBoundary" + str(time.time()).replace(".", "")

    body = []
    body.append(f"--{boundary}".encode("utf-8"))
    body.append(f'Content-Disposition: form-data; name="file"; filename="{filename}"'.encode("utf-8"))
    body.append(b"Content-Type: text/csv")
    body.append(b"")
    body.append(file_bytes)
    body.append(f"--{boundary}--".encode("utf-8"))
    body.append(b"")
    payload = b"\r\n".join(body)

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(len(payload)),
        },
        method="POST"
    )

    t0 = time.monotonic()
    with urllib.request.urlopen(req) as resp:
        res_data = json.loads(resp.read().decode("utf-8"))
    t1 = time.monotonic()

    return res_data, (t1 - t0), file_size


def get_status(api_base: str, job_id: str) -> dict:
    url = f"{api_base}/status?job_id={urllib.parse.quote(job_id)}"
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_benchmark(filepath: str, api_base: str, poll_interval: float = 0.05) -> dict:
    if not os.path.isfile(filepath):
        print(f"Error: file not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        lines = [l for l in f if l.strip()]
        header = lines[0] if lines else ""
        num_rows = max(0, len(lines) - 1)

    t_start = time.monotonic()
    ingest_url = f"{api_base}/ingest"

    try:
        res, t_ingest, file_size = post_multipart(ingest_url, filepath)
    except Exception as e:
        print(f"Error calling /ingest: {e}", file=sys.stderr)
        sys.exit(1)

    job_id = res.get("job_id")
    rows_received = res.get("rows_received", 0)

    t_first_row = None
    t_complete = None
    final_status = None
    last_loaded = 0

    # Poll status until complete or failed (max 300s)
    deadline = time.monotonic() + 300.0
    while time.monotonic() < deadline:
        try:
            st = get_status(api_base, job_id)
            status = st.get("status")
            loaded = st.get("rows_loaded", 0)
            failed = st.get("rows_failed", 0)
            last_loaded = loaded

            now = time.monotonic()
            if loaded > 0 and t_first_row is None:
                t_first_row = now - t_start

            if status in ("complete", "failed"):
                t_complete = now - t_start
                final_status = st
                break
        except Exception:
            pass
        time.sleep(poll_interval)

    if t_complete is None:
        t_complete = time.monotonic() - t_start
        final_status = {"status": "timeout", "rows_loaded": last_loaded, "rows_failed": 0}

    total_duration = t_complete
    rows_loaded = final_status.get("rows_loaded", 0)
    rows_failed = final_status.get("rows_failed", 0)
    throughput = (rows_loaded / total_duration) if total_duration > 0 else 0.0

    return {
        "file": filepath,
        "file_size_bytes": file_size,
        "data_rows": num_rows,
        "job_id": job_id,
        "status": final_status.get("status"),
        "ingest_post_sec": round(t_ingest, 4),
        "time_to_first_row_sec": round(t_first_row, 4) if t_first_row else None,
        "total_duration_sec": round(total_duration, 4),
        "rows_loaded": rows_loaded,
        "rows_failed": rows_failed,
        "rows_per_second": round(throughput, 2),
    }


def main():
    parser = argparse.ArgumentParser(description="Benchmark CSV ingestion pipeline.")
    parser.add_argument("file", help="Path to CSV file to ingest")
    parser.add_argument("--api", default=API_DEFAULT, help=f"API base URL (default: {API_DEFAULT})")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    args = parser.parse_args()

    result = run_benchmark(args.file, args.api)

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print("\n" + "=" * 60)
        print(" INGESTION PIPELINE BENCHMARK RESULT")
        print("=" * 60)
        print(f"File:                   {result['file']} ({result['file_size_bytes']:,} bytes)")
        print(f"Data Rows:              {result['data_rows']:,}")
        print(f"Job ID:                 {result['job_id']}")
        print(f"Status:                 {result['status']}")
        print(f"POST /ingest Duration:  {result['ingest_post_sec']:.4f}s")
        print(f"Time to First Row:      {result['time_to_first_row_sec']}s" if result['time_to_first_row_sec'] else "Time to First Row:      N/A")
        print(f"Total Pipeline Time:    {result['total_duration_sec']:.4f}s")
        print(f"Rows Loaded:            {result['rows_loaded']:,}")
        print(f"Rows Failed:            {result['rows_failed']:,}")
        print(f"Sustained Throughput:   {result['rows_per_second']:.2f} rows/sec")
        print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
