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
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from api.main import app


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
