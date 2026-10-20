"""Unit tests for loader/main.py.

Tests run entirely in-memory using mock Kafka consumers and mock Neo4j sessions
(zero Docker/network dependencies).

Verifies:
1. Message handling: happy path executes MERGE_ROW with exact parameters.
2. Failure fallback: write exceptions trigger MARK_FAILED with exact parameters.
3. Resilience against Kafka non-message events (poll timeouts, Kafka errors, empty payload).
4. Bug documentation (strict xfail): malformed JSON and missing fields currently crash the
   consumer loop with unhandled exceptions (json.JSONDecodeError / KeyError) because lines 109-111
   in loader/main.py are outside the database try/except block.
5. Connection retry logic for Kafka and Neo4j.

IMPORTANT NOTE ON CYPHERS SEMANTICS:
These fake-session tests verify QUERY AND PARAMS SENT ONLY.
A mock session cannot verify Neo4j Cypher runtime semantics (such as conditional
rows_loaded increment via ON CREATE/ON MATCH + FOREACH or rows_failed counter logic).
Full semantic verification of the Cypher idempotency counters is explicitly deferred to
Phase 1 Step 6 using Testcontainers on a real Neo4j instance.
"""

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException
from neo4j.exceptions import ServiceUnavailable

import loader.main as loader_main


class ExitLoopSentinel(Exception):
    """Sentinel exception to cleanly terminate loader.main's infinite while True loop."""


class MockKafkaMessage:
    def __init__(self, value: bytes | None = None, error: Any = None):
        self._value = value
        self._error = error

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> Any:
        return self._error


class MockConsumer:
    def __init__(self, messages: list[Any]):
        self.messages = list(messages)
        self.poll_count = 0

    def poll(self, timeout: float = 1.0) -> Any:
        self.poll_count += 1
        if self.messages:
            msg = self.messages.pop(0)
            if isinstance(msg, Exception):
                raise msg
            return msg
        raise ExitLoopSentinel()


class MockNeo4jSession:
    def __init__(self, fail_query_pattern: str | None = None):
        self.recorded_runs: list[tuple[str, dict[str, Any]]] = []
        self.fail_query_pattern = fail_query_pattern

    def __enter__(self) -> "MockNeo4jSession":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def run(self, cypher: str, **params: Any) -> None:
        self.recorded_runs.append((cypher, params))
        if self.fail_query_pattern and self.fail_query_pattern in cypher:
            raise RuntimeError(f"Simulated database error for query matching: {self.fail_query_pattern}")


class MockNeo4jDriver:
    def __init__(self, session: MockNeo4jSession):
        self._session = session

    def session(self, database: str | None = None) -> MockNeo4jSession:
        return self._session


# ==============================================================================
# 1. Happy Path & Parameter Verification ("Query and Params Sent Only")
# ==============================================================================
def test_loader_processes_valid_message_sends_merge_row():
    """Verify that a valid Kafka message results in MERGE_ROW dispatched with exact parameters.

    NOTE: Verification is 'query/params sent only'. Cypher counter semantics (rows_loaded conditional
    increment via ON CREATE/ON MATCH) are not evaluated by mock sessions and are deferred to Step 6.
    """
    sample_payload = {
        "dataset_id": "sha256_dataset_001",
        "row_index": 0,
        "filename": "transactions.csv",
        "uploaded_at": "2026-10-20T10:00:00Z",
        "rows_total": 100,
        "row": {"customer_id": "C101", "amount": "45.50", "status": "completed"},
    }
    raw_bytes = json.dumps(sample_payload).encode("utf-8")
    consumer = MockConsumer([MockKafkaMessage(value=raw_bytes)])
    session = MockNeo4jSession()
    driver = MockNeo4jDriver(session)

    with patch("loader.main.connect_kafka", return_value=consumer), patch(
        "loader.main.connect_neo4j", return_value=driver
    ):
        with pytest.raises(ExitLoopSentinel):
            loader_main.main()

    # Verify query and params sent only
    assert len(session.recorded_runs) == 1
    cypher_sent, params_sent = session.recorded_runs[0]
    assert cypher_sent == loader_main.MERGE_ROW
    assert params_sent == {
        "dataset_id": "sha256_dataset_001",
        "filename": "transactions.csv",
        "uploaded_at": "2026-10-20T10:00:00Z",
        "row_index": 0,
        "rows_total": 100,
        "row": {"customer_id": "C101", "amount": "45.50", "status": "completed"},
    }


# ==============================================================================
# 2. Failure Path & Fallback to MARK_FAILED ("Query and Params Sent Only")
# ==============================================================================
def test_loader_merge_row_failure_triggers_mark_failed():
    """Verify that a failure on MERGE_ROW triggers MARK_FAILED with required parameters.

    NOTE: Verification is 'query/params sent only'. Cypher counter semantics (rows_failed increment)
    are deferred to Step 6 on real Neo4j.
    """
    sample_payload = {
        "dataset_id": "sha256_dataset_002",
        "row_index": 5,
        "filename": "corrupted.csv",
        "uploaded_at": "2026-10-20T10:00:00Z",
        "rows_total": 10,
        "row": {"val": "corrupt"},
    }
    raw_bytes = json.dumps(sample_payload).encode("utf-8")
    consumer = MockConsumer([MockKafkaMessage(value=raw_bytes)])
    # Simulate write failure on MERGE_ROW (Row node creation fails)
    session = MockNeo4jSession(fail_query_pattern="MERGE (r:Row")
    driver = MockNeo4jDriver(session)

    with patch("loader.main.connect_kafka", return_value=consumer), patch(
        "loader.main.connect_neo4j", return_value=driver
    ):
        with pytest.raises(ExitLoopSentinel):
            loader_main.main()

    # Must record 2 query executions: 1 failed MERGE_ROW, followed by 1 MARK_FAILED
    assert len(session.recorded_runs) == 2
    failed_cypher, failed_params = session.recorded_runs[0]
    assert failed_cypher == loader_main.MERGE_ROW

    mark_cypher, mark_params = session.recorded_runs[1]
    assert mark_cypher == loader_main.MARK_FAILED
    assert mark_params == {
        "dataset_id": "sha256_dataset_002",
        "row_index": 5,
        "filename": "corrupted.csv",
        "uploaded_at": "2026-10-20T10:00:00Z",
        "rows_total": 10,
    }


def test_loader_both_merge_and_mark_failed_raise_handled_gracefully():
    """Verify that if both MERGE_ROW and MARK_FAILED fail, the consumer does not crash."""
    sample_payload = {
        "dataset_id": "sha256_dataset_003",
        "row_index": 1,
        "filename": "broken.csv",
        "uploaded_at": "2026-10-20T10:00:00Z",
        "rows_total": 5,
        "row": {},
    }
    raw_bytes = json.dumps(sample_payload).encode("utf-8")
    consumer = MockConsumer([MockKafkaMessage(value=raw_bytes)])
    # Always raise error on any run call
    session = MockNeo4jSession(fail_query_pattern="MERGE")
    driver = MockNeo4jDriver(session)

    with patch("loader.main.connect_kafka", return_value=consumer), patch(
        "loader.main.connect_neo4j", return_value=driver
    ):
        # Must catch e2 and not crash the loop before reaching ExitLoopSentinel
        with pytest.raises(ExitLoopSentinel):
            loader_main.main()

    assert len(session.recorded_runs) == 2


# ==============================================================================
# 3. Kafka Non-Message Events Handling (Poll timeout, error, empty payload)
# ==============================================================================
def test_loader_skips_none_messages_and_errors():
    """Verify that poll timeouts (None), Kafka errors, and empty values are skipped without calling DB."""
    events = [
        None,  # Poll timeout
        MockKafkaMessage(value=None, error=None),  # Value is None
        MockKafkaMessage(value=b"some bytes", error="Broker: LeaderNotAvailable"),  # Kafka error
    ]
    consumer = MockConsumer(events)
    session = MockNeo4jSession()
    driver = MockNeo4jDriver(session)

    with patch("loader.main.connect_kafka", return_value=consumer), patch(
        "loader.main.connect_neo4j", return_value=driver
    ):
        with pytest.raises(ExitLoopSentinel):
            loader_main.main()

    # No database calls should have been dispatched
    assert len(session.recorded_runs) == 0


# ==============================================================================
# 4. Known Bugs in Loader (Strict xfail - Do not modify production code in Step 3)
# ==============================================================================
@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 5: DLQ / poison pill: malformed JSON in loader/main.py crashes consumer loop with unhandled JSONDecodeError",
)
def test_loader_survives_malformed_json():
    """Bug reproduction: malformed JSON crashes consumer loop because json.loads is outside try/except.

    A resilient consumer must catch deserialization errors, log, and skip/route to DLQ without crashing.
    """
    consumer = MockConsumer([MockKafkaMessage(value=b"{not valid json")])
    session = MockNeo4jSession()
    driver = MockNeo4jDriver(session)

    with patch("loader.main.connect_kafka", return_value=consumer), patch(
        "loader.main.connect_neo4j", return_value=driver
    ):
        # In current production code, this raises json.JSONDecodeError instead of ExitLoopSentinel
        with pytest.raises(ExitLoopSentinel):
            loader_main.main()


@pytest.mark.xfail(
    strict=True,
    reason="fix in Phase 5: DLQ / poison pill: missing required fields ('dataset_id' or 'row_index') raises unhandled KeyError in consumer loop",
)
def test_loader_survives_missing_required_fields():
    """Bug reproduction: missing dataset_id or row_index crashes consumer loop with unhandled KeyError."""
    # Missing dataset_id and row_index
    invalid_payload = json.dumps({"filename": "no_ids.csv", "row": {}}).encode("utf-8")
    consumer = MockConsumer([MockKafkaMessage(value=invalid_payload)])
    session = MockNeo4jSession()
    driver = MockNeo4jDriver(session)

    with patch("loader.main.connect_kafka", return_value=consumer), patch(
        "loader.main.connect_neo4j", return_value=driver
    ):
        # In current production code, this raises KeyError instead of ExitLoopSentinel
        with pytest.raises(ExitLoopSentinel):
            loader_main.main()


# ==============================================================================
# 5. Connect / Retry Logic Tests
# ==============================================================================
def test_connect_kafka_retry_and_success():
    """Verify that connect_kafka retries on KafkaException and generic Exception, then subscribes upon success."""
    mock_consumer = MagicMock()
    # Fails with KafkaException, then with generic Exception, then succeeds
    mock_consumer.list_topics.side_effect = [
        KafkaException("Broker not ready"),
        RuntimeError("Transient network glitch"),
        MagicMock(),
    ]

    with patch("loader.main.Consumer", return_value=mock_consumer), patch("loader.main.time.sleep") as mock_sleep:
        consumer = loader_main.connect_kafka()
        assert consumer == mock_consumer
        assert mock_consumer.list_topics.call_count == 3
        assert mock_sleep.call_count == 2
        mock_consumer.subscribe.assert_called_once_with([loader_main.KAFKA_TOPIC])



def test_connect_neo4j_retry_and_success():
    """Verify that connect_neo4j retries on ServiceUnavailable and returns driver upon success."""
    mock_driver = MagicMock()
    # Fails once with ServiceUnavailable, then succeeds
    mock_driver.verify_connectivity.side_effect = [
        ServiceUnavailable("Neo4j database unavailable"),
        None,
    ]

    with patch("loader.main.GraphDatabase.driver", return_value=mock_driver), patch("loader.main.time.sleep") as mock_sleep:
        driver = loader_main.connect_neo4j()
        assert driver == mock_driver
        assert mock_driver.verify_connectivity.call_count == 2
        mock_sleep.assert_called_once_with(2)
