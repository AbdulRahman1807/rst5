"""Unit tests for the /ingest API endpoint using FastAPI TestClient and a fake Kafka producer.

Tests:
1. Bare CR in unquoted field -> HTTP 400
2. Reserved header (dataset_id, row_index, _new) -> HTTP 400
3. Duplicate header -> HTTP 400
4. Zero good rows (all ragged or control chars) -> HTTP 400 'no loadable rows'
5. Valid CSV -> HTTP 202, messages produced to Kafka with correct keys and failed=False
6. Ragged CSV (broken.csv) -> HTTP 202, failed rows carry failed=True, reason, row={},
   and NO published JSON message ever contains a None or "null" key.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException
from fastapi.testclient import TestClient
from neo4j.exceptions import ServiceUnavailable

import api.main as api_main
from api.main import app, jobs


class FakeProducer:
    """In-memory fake Kafka producer capturing published messages."""

    def __init__(self):
        self.messages: list[dict] = []
        self.raw_payloads: list[str] = []

    def produce(self, topic, key=None, value=None, callback=None):
        payload_str = value.decode("utf-8") if isinstance(value, bytes) else value
        self.raw_payloads.append(payload_str)
        msg_dict = json.loads(payload_str)
        self.messages.append(msg_dict)
        if callback:
            callback(None, MagicMock())

    def poll(self, timeout=0):
        pass

    def flush(self, timeout=None):
        pass


@pytest.fixture
def fake_producer(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr("api.main.get_producer", lambda: fake)
    return fake


@pytest.fixture
def client():
    return TestClient(app)


def test_ingest_bare_cr_in_unquoted_field_returns_400(client, fake_producer):
    """Bare CR inside an unquoted field must be rejected with HTTP 400."""
    content = b"a,b\n1,foo\rbar\n"
    response = client.post(
        "/ingest",
        files={"file": ("test.csv", content, "text/csv")},
    )
    assert response.status_code == 400
    assert "malformed CSV syntax" in response.json()["detail"]
    assert len(fake_producer.messages) == 0


def test_ingest_reserved_headers_returns_400(client, fake_producer):
    """CSV containing reserved bookkeeping headers must be rejected with HTTP 400."""
    for header in ["dataset_id", "row_index", "_new", "DATASET_ID"]:
        content = f"{header},amount\nval1,100\n".encode("utf-8")
        response = client.post(
            "/ingest",
            files={"file": ("test.csv", content, "text/csv")},
        )
        assert response.status_code == 400
        assert "reserved" in response.json()["detail"]
    assert len(fake_producer.messages) == 0


def test_ingest_duplicate_header_returns_400(client, fake_producer):
    """Duplicate header names must be rejected with HTTP 400."""
    content = b"col1,col2,col1\nval1,val2,val3\n"
    response = client.post(
        "/ingest",
        files={"file": ("test.csv", content, "text/csv")},
    )
    assert response.status_code == 400
    assert "duplicate column name" in response.json()["detail"]
    assert len(fake_producer.messages) == 0


def test_ingest_zero_good_rows_returns_400(client, fake_producer):
    """If every data row is ragged, whole file is rejected with HTTP 400 'no loadable rows'."""
    content = b"col1,col2\nonly_one_col\nextra1,extra2,extra3\n"
    response = client.post(
        "/ingest",
        files={"file": ("test.csv", content, "text/csv")},
    )
    assert response.status_code == 400
    assert "no loadable rows" in response.json()["detail"]
    assert len(fake_producer.messages) == 0


def test_ingest_valid_csv_returns_202_and_publishes_messages(client, fake_producer):
    """Valid CSV returns HTTP 202 and publishes messages whose row keys match header names."""
    content = b"name,group,amount\nAcme Co,Billing,128\nNova,Support,340\n"
    response = client.post(
        "/ingest",
        files={"file": ("clean.csv", content, "text/csv")},
    )
    assert response.status_code == 202
    data = response.json()
    assert data["rows_received"] == 2
    assert data["status"] == "queued"

    assert len(fake_producer.messages) == 2
    for i, msg in enumerate(fake_producer.messages):
        assert msg["row_index"] == i
        assert msg["rows_total"] == 2
        assert msg["failed"] is False
        assert msg["reason"] is None
        assert set(msg["row"].keys()) == {"name", "group", "amount"}
        assert None not in msg["row"]

    # Verify no raw JSON payload contains "null" key
    for raw in fake_producer.raw_payloads:
        assert '"null":' not in raw
        assert "None" not in raw


def test_ingest_broken_csv_returns_202_and_publishes_row_failures(client, fake_producer):
    """broken.csv returns HTTP 202; ragged rows have failed=True, reason, row={}, and no None keys."""
    with open("test_data/broken.csv", "rb") as f:
        content = f.read()

    response = client.post(
        "/ingest",
        files={"file": ("broken.csv", content, "text/csv")},
    )
    assert response.status_code == 202
    data = response.json()
    assert data["rows_received"] == 6

    assert len(fake_producer.messages) == 6

    # Verify exact classifications
    good_msgs = [m for m in fake_producer.messages if not m["failed"]]
    failed_msgs = [m for m in fake_producer.messages if m["failed"]]

    assert len(good_msgs) == 2
    assert len(failed_msgs) == 4

    # Check good rows
    for m in good_msgs:
        assert m["reason"] is None
        assert len(m["row"]) == 3
        assert None not in m["row"]

    # Check failed rows
    for m in failed_msgs:
        assert m["reason"] in ("extra_fields", "short_row", "control_char")
        assert m["row"] == {}  # failed rows carry empty dict
        assert None not in m["row"]

    # Critical check: assert no published raw JSON payload contains a None or "null" key
    for raw in fake_producer.raw_payloads:
        assert '"null":' not in raw
        assert "None:" not in raw


# ==============================================================================
# Ingest Failure Paths & Helper Classes
# ==============================================================================
class ErrorProducer:
    """Fake producer that triggers delivery errors or raises KafkaException."""

    def __init__(self, fail_on_produce: bool = False, fail_on_callback: bool = False):
        self.fail_on_produce = fail_on_produce
        self.fail_on_callback = fail_on_callback

    def produce(self, topic, key=None, value=None, callback=None):
        if self.fail_on_produce:
            raise KafkaException("simulated buffer full or kafka exception")
        if self.fail_on_callback and callback:
            callback("delivery error timeout", MagicMock())

    def poll(self, timeout=0):
        pass

    def flush(self, timeout=None):
        pass

    def list_topics(self, timeout=3):
        return {}


class FakeNeo4jResult:
    def __init__(self, record):
        self._record = record

    def single(self):
        return self._record


class FakeNeo4jSession:
    def __init__(self, record=None):
        self._record = record

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def run(self, query, **params):
        return FakeNeo4jResult(self._record)


class FakeNeo4jDriver:
    def __init__(self, record=None, raise_on_verify: Exception | None = None):
        self._record = record
        self._raise_on_verify = raise_on_verify

    def session(self, database=None):
        return FakeNeo4jSession(self._record)

    def verify_connectivity(self):
        if self._raise_on_verify:
            raise self._raise_on_verify


def test_ingest_non_csv_extension_returns_400(client):
    """Uploading a non-.csv file must return HTTP 400."""
    response = client.post(
        "/ingest",
        files={"file": ("notes.txt", b"some text", "text/plain")},
    )
    assert response.status_code == 400
    assert "file must be a .csv" in response.json()["detail"]


def test_ingest_kafka_exception_returns_503(client, monkeypatch):
    """KafkaException or BufferError on produce must return HTTP 503."""
    monkeypatch.setattr("api.main.get_producer", lambda: ErrorProducer(fail_on_produce=True))
    response = client.post(
        "/ingest",
        files={"file": ("valid.csv", b"a,b\n1,2\n", "text/csv")},
    )
    assert response.status_code == 503
    assert "kafka temporarily unavailable" in response.json()["detail"]


def test_ingest_kafka_delivery_error_returns_503(client, monkeypatch):
    """Callback delivery failures must return HTTP 503."""
    monkeypatch.setattr("api.main.get_producer", lambda: ErrorProducer(fail_on_callback=True))
    response = client.post(
        "/ingest",
        files={"file": ("valid.csv", b"a,b\n1,2\n", "text/csv")},
    )
    assert response.status_code == 503
    assert "kafka temporarily unavailable" in response.json()["detail"]


# ==============================================================================
# /health, /status, /chat & Driver Initialization Unit Tests
# ==============================================================================
def test_health_endpoint_healthy(client, monkeypatch):
    """When Kafka and Neo4j are connected, /health returns ok."""
    monkeypatch.setattr("api.main.get_producer", lambda: ErrorProducer())
    monkeypatch.setattr("api.main.get_driver", lambda: FakeNeo4jDriver())
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data == {"status": "ok", "kafka_connected": True, "neo4j_connected": True}


def test_health_endpoint_kafka_unhealthy(client, monkeypatch):
    """When Kafka probe fails, /health returns not_ok."""

    class BrokenKafka:
        def list_topics(self, timeout=3):
            raise RuntimeError("kafka down")

    monkeypatch.setattr("api.main.get_producer", lambda: BrokenKafka())
    monkeypatch.setattr("api.main.get_driver", lambda: FakeNeo4jDriver())
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "not_ok"
    assert data["kafka_connected"] is False
    assert data["neo4j_connected"] is True


def test_health_endpoint_neo4j_unhealthy(client, monkeypatch):
    """When Neo4j probe fails, /health returns not_ok."""
    monkeypatch.setattr("api.main.get_producer", lambda: ErrorProducer())
    monkeypatch.setattr(
        "api.main.get_driver",
        lambda: FakeNeo4jDriver(raise_on_verify=ServiceUnavailable("db down")),
    )
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "not_ok"
    assert data["kafka_connected"] is True
    assert data["neo4j_connected"] is False


def test_status_unknown_job_id_returns_404(client):
    """Querying /status for an unregistered job_id returns HTTP 404."""
    response = client.get("/status?job_id=unknown_job_id_12345")
    assert response.status_code == 404
    assert response.json()["detail"] == "unknown job_id"


def test_status_queued_when_no_neo4j_record(client, monkeypatch):
    """When job is registered but not yet loaded into Neo4j, status returns 'queued'."""
    jobs["job_queued_test"] = {"rows_received": 10, "received_at": "2026-10-20T10:00:00Z"}
    monkeypatch.setattr("api.main.get_driver", lambda: FakeNeo4jDriver(record=None))

    response = client.get("/status?job_id=job_queued_test")
    assert response.status_code == 200
    assert response.json() == {
        "job_id": "job_queued_test",
        "status": "queued",
        "rows_total": 10,
        "rows_loaded": 0,
        "rows_failed": 0,
    }


def test_status_complete_when_neo4j_record_found(client, monkeypatch):
    """When Neo4j record exists, /status returns current status and counters."""
    jobs["job_complete_test"] = {"rows_received": 6, "received_at": "2026-10-20T10:00:00Z"}
    db_record = {
        "status": "complete",
        "rows_total": 6,
        "rows_loaded": 2,
        "rows_failed": 4,
    }
    monkeypatch.setattr("api.main.get_driver", lambda: FakeNeo4jDriver(record=db_record))

    response = client.get("/status?job_id=job_complete_test")
    assert response.status_code == 200
    assert response.json() == {
        "job_id": "job_complete_test",
        "status": "complete",
        "rows_total": 6,
        "rows_loaded": 2,
        "rows_failed": 4,
    }


def test_chat_endpoint_calls_chatbot(client, monkeypatch):
    """The /chat endpoint passes question to chatbot.answer_question."""
    fake_driver = FakeNeo4jDriver()
    monkeypatch.setattr("api.main.get_driver", lambda: fake_driver)
    mock_answer = MagicMock(return_value={"answer": "42 rows", "cypher": None, "grounded": True})
    monkeypatch.setattr("api.main.chatbot.answer_question", mock_answer)

    # Test with body
    resp = client.post("/chat", json={"question": "How many rows?"})
    assert resp.status_code == 200
    assert resp.json()["answer"] == "42 rows"
    mock_answer.assert_called_with(fake_driver, api_main.NEO4J_DATABASE, "How many rows?")

    # Test direct call with payload=None (exercises line 175)
    res_direct = api_main.chat(payload=None)
    assert res_direct["answer"] == "42 rows"


def test_lazy_driver_and_producer_initialization(monkeypatch):
    """Lines 57-59 and 64-66 initialize _producer and _driver when None."""
    monkeypatch.setattr("api.main._producer", None)
    monkeypatch.setattr("api.main._driver", None)

    with (
        patch("api.main.Producer") as mock_prod_cls,
        patch("api.main.GraphDatabase.driver") as mock_drv_cls,
    ):
        p1 = api_main.get_producer()
        p2 = api_main.get_producer()
        assert p1 is p2
        assert mock_prod_cls.call_count == 1

        d1 = api_main.get_driver()
        d2 = api_main.get_driver()
        assert d1 is d2
        assert mock_drv_cls.call_count == 1
