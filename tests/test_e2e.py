"""End-to-End integration tests against the isolated rst5-e2e Docker Compose stack.

Tests:
1. Hostile input checklist ported from scripts/test_hostile_input.sh:
   - 400s on empty, header_only, not_a_csv, all_ragged.
   - Refusal structure on empty stack before upload (grounded is False, cypher is None).
   - Refusal structure on unanswerable question ("What is the capital of France?").
2. Replay & Idempotency test ported from scripts/verify_idempotency.sh:
   - Ingest small_clean.csv and broken.csv 3x each on empty stack.
   - Poll /status until complete before querying /chat.
   - Assert /status counters (15/15/0 and 6/2/4) remain identical across all 3 passes.
   - Query Neo4j directly via driver: assert :Dataset (2), :Row (17), and :FailedRow (4)
     node counts remain strictly identical across all 3 passes.
   - Chat query row count returns grounded=True with matching count.
3. Quoted-newline and tab cell end-to-end:
   - Ingest CSV with quoted newline cell and tab cell.
   - Verify /status completes with rows_loaded=1, rows_failed=0.
   - Verify node property values in Neo4j preserve embedded newlines and tabs exactly.
"""

import os
import time

import httpx
import pytest
from neo4j import GraphDatabase

API_URL = os.getenv("API_URL", "http://localhost:8001")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7688")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "csvgraphdb")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "csv-graph-db")


@pytest.fixture(scope="module")
def neo4j_driver():
    """Module-scoped Neo4j driver connected to the isolated e2e stack."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    # Wait for Neo4j connectivity
    for _ in range(30):
        try:
            driver.verify_connectivity()
            break
        except Exception:
            time.sleep(1)
    yield driver
    driver.close()


def poll_status(client: httpx.Client, job_id: str, timeout: int = 60) -> dict:
    """Poll the /status endpoint until the ingestion job reaches 'complete'."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = client.get(f"{API_URL}/status?job_id={job_id}")
        assert resp.status_code == 200, f"/status returned HTTP {resp.status_code}: {resp.text}"
        data = resp.json()
        if data.get("status") == "complete":
            return data
        time.sleep(0.5)
    pytest.fail(f"Job {job_id} did not reach complete within {timeout}s: last status was {data}")


@pytest.mark.e2e
def test_e2e_hostile_input_checklist():
    """Verify clean 400 responses on hostile inputs and refusal structure on fresh stack."""
    with httpx.Client(timeout=30.0) as client:
        # 1. Hostile inputs -> HTTP 400
        for filename in [
            "test_data/empty.csv",
            "test_data/header_only.csv",
            "test_data/not_a_csv.txt",
            "test_data/all_ragged.csv",
        ]:
            with open(filename, "rb") as f:
                content = f.read()
            resp = client.post(
                f"{API_URL}/ingest",
                files={"file": (os.path.basename(filename), content, "text/csv")},
            )
            assert resp.status_code == 400, f"Expected 400 for {filename}, got {resp.status_code}: {resp.text}"

        # 2. Refusal before any upload (only valid on clean stack)
        resp = client.post(
            f"{API_URL}/chat",
            json={"question": "How many rows are there?"},
        )
        assert resp.status_code == 200
        chat_data = resp.json()
        assert isinstance(chat_data.get("grounded"), bool)
        assert chat_data["grounded"] is False
        assert chat_data.get("cypher") is None
        assert isinstance(chat_data.get("answer"), str)

        # 3. Refusal with unanswerable question
        resp = client.post(
            f"{API_URL}/chat",
            json={"question": "What is the capital of France?"},
        )
        assert resp.status_code == 200
        chat_data = resp.json()
        assert isinstance(chat_data.get("grounded"), bool)
        assert chat_data["grounded"] is False
        assert chat_data.get("cypher") is None
        assert isinstance(chat_data.get("answer"), str)


@pytest.mark.e2e
def test_e2e_replay_idempotency_and_status(neo4j_driver):
    """Ingest small_clean.csv and broken.csv 3 times; verify /status and DB counts stay constant."""
    with httpx.Client(timeout=30.0) as client:
        with open("test_data/small_clean.csv", "rb") as f:
            clean_bytes = f.read()
        with open("test_data/broken.csv", "rb") as f:
            broken_bytes = f.read()

        clean_job_id = None
        broken_job_id = None

        # Replay 3 times
        for pass_num in range(1, 4):
            # Ingest small_clean.csv
            resp1 = client.post(
                f"{API_URL}/ingest",
                files={"file": ("small_clean.csv", clean_bytes, "text/csv")},
            )
            assert resp1.status_code == 202
            data1 = resp1.json()
            if clean_job_id is None:
                clean_job_id = data1["job_id"]
            else:
                assert data1["job_id"] == clean_job_id, f"Pass {pass_num}: job_id changed for small_clean.csv"

            status1 = poll_status(client, clean_job_id)
            assert status1["rows_total"] == 15
            assert status1["rows_loaded"] == 15
            assert status1["rows_failed"] == 0
            assert status1["status"] == "complete"

            # Ingest broken.csv
            resp2 = client.post(
                f"{API_URL}/ingest",
                files={"file": ("broken.csv", broken_bytes, "text/csv")},
            )
            assert resp2.status_code == 202
            data2 = resp2.json()
            if broken_job_id is None:
                broken_job_id = data2["job_id"]
            else:
                assert data2["job_id"] == broken_job_id, f"Pass {pass_num}: job_id changed for broken.csv"

            status2 = poll_status(client, broken_job_id)
            assert status2["rows_total"] == 6
            assert status2["rows_loaded"] == 2
            assert status2["rows_failed"] == 4
            assert status2["status"] == "complete"

            # Query Neo4j directly via driver to verify node counts
            with neo4j_driver.session(database=NEO4J_DATABASE) as session:
                dataset_count = session.run("MATCH (d:Dataset) RETURN count(d) AS c").single()["c"]
                row_count = session.run("MATCH (r:Row) RETURN count(r) AS c").single()["c"]
                failed_count = session.run("MATCH (f:FailedRow) RETURN count(f) AS c").single()["c"]

            assert dataset_count == 2, f"Pass {pass_num}: expected 2 Dataset nodes, got {dataset_count}"
            assert row_count == 17, f"Pass {pass_num}: expected 17 Row nodes (15+2), got {row_count}"
            assert failed_count == 4, f"Pass {pass_num}: expected 4 FailedRow nodes, got {failed_count}"

        # Chat query row count AFTER completion
        chat_resp = client.post(
            f"{API_URL}/chat",
            json={"question": "How many rows are there?"},
        )
        assert chat_resp.status_code == 200
        chat_data = chat_resp.json()
        assert chat_data["grounded"] is True
        assert isinstance(chat_data["cypher"], str)
        assert chat_data["result"] == [{"count": 17}]


@pytest.mark.e2e
def test_e2e_quoted_newline_and_tab_cells_load_cleanly(neo4j_driver):
    """Verify that cells containing quoted newlines and tabs load cleanly as good rows."""
    content = b'company,address,notes\n"Acme Corp","123 Main St\nSuite 400",good\taccount\n'
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            f"{API_URL}/ingest",
            files={"file": ("special_chars.csv", content, "text/csv")},
        )
        assert resp.status_code == 202
        job_id = resp.json()["job_id"]

        status = poll_status(client, job_id)
        assert status["rows_total"] == 1
        assert status["rows_loaded"] == 1
        assert status["rows_failed"] == 0
        assert status["status"] == "complete"

        # Verify property content in Neo4j
        with neo4j_driver.session(database=NEO4J_DATABASE) as session:
            record = session.run(
                "MATCH (r:Row {dataset_id: $dataset_id}) RETURN r.company AS c, r.address AS a, r.notes AS n",
                dataset_id=job_id,
            ).single()

        assert record is not None
        assert record["c"] == "Acme Corp"
        assert record["a"] == "123 Main St\nSuite 400"
        assert record["n"] == "good\taccount"
