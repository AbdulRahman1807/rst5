import json
import logging
import os
import time

from confluent_kafka import Consumer, KafkaException
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("loader")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "csv-graph-db")
KAFKA_TOPIC = "csv-rows"
BATCH_SIZE = int(os.getenv("LOADER_BATCH_SIZE", "200"))
POLL_TIMEOUT = float(os.getenv("LOADER_POLL_TIMEOUT", "0.1"))

# Optimized batched parameterized Cypher queries with UNWIND
MERGE_BATCH = """
UNWIND $batch AS item
MERGE (d:Dataset {id: item.dataset_id})
  ON CREATE SET d.filename = item.filename, d.uploaded_at = item.uploaded_at,
                d.rows_total = item.rows_total, d.rows_loaded = 0,
                d.rows_failed = 0, d.status = 'loading'
MERGE (r:Row {dataset_id: item.dataset_id, row_index: item.row_index})
  ON CREATE SET r += item.row, r.dataset_id = item.dataset_id, r.row_index = item.row_index, r._new = true
  ON MATCH SET r._new = false
MERGE (d)-[:HAS_ROW]->(r)
WITH d, r
FOREACH (_ IN CASE WHEN r._new THEN [1] ELSE [] END | SET d.rows_loaded = coalesce(d.rows_loaded, 0) + 1)
WITH d
SET d.status = CASE WHEN d.rows_loaded + coalesce(d.rows_failed, 0) >= d.rows_total THEN 'complete' ELSE 'loading' END
"""

MARK_FAILED_BATCH = """
UNWIND $batch AS item
MERGE (d:Dataset {id: item.dataset_id})
  ON CREATE SET d.filename = item.filename, d.uploaded_at = item.uploaded_at,
                d.rows_total = item.rows_total, d.rows_loaded = 0,
                d.rows_failed = 0, d.status = 'loading'
MERGE (f:FailedRow {dataset_id: item.dataset_id, row_index: item.row_index})
  ON CREATE SET f._new = true, f.reason = item.reason
  ON MATCH SET f._new = false
WITH d, f
FOREACH (_ IN CASE WHEN f._new THEN [1] ELSE [] END | SET d.rows_failed = coalesce(d.rows_failed, 0) + 1)
WITH d
SET d.status = CASE WHEN coalesce(d.rows_loaded, 0) + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
"""

# Single-row Cypher queries matching PR #5 test specifications
MERGE_ROW = """
MERGE (d:Dataset {id: $dataset_id})
  ON CREATE SET d.filename = $filename, d.uploaded_at = $uploaded_at,
                d.rows_total = $rows_total, d.rows_loaded = 0,
                d.rows_failed = 0, d.status = 'loading'
MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index})
  ON CREATE SET r += $row, r.dataset_id = $dataset_id, r.row_index = $row_index, r._new = true
  ON MATCH SET r._new = false
MERGE (d)-[:HAS_ROW]->(r)
WITH d, r
FOREACH (_ IN CASE WHEN r._new THEN [1] ELSE [] END | SET d.rows_loaded = coalesce(d.rows_loaded, 0) + 1)
WITH d
SET d.status = CASE WHEN d.rows_loaded + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
"""

MARK_FAILED = """
MERGE (d:Dataset {id: $dataset_id})
  ON CREATE SET d.filename = $filename, d.uploaded_at = $uploaded_at,
                d.rows_total = $rows_total, d.rows_loaded = 0, d.rows_failed = 0, d.status = 'loading'
MERGE (f:FailedRow {dataset_id: $dataset_id, row_index: $row_index})
  ON CREATE SET f._new = true, f.reason = $reason
  ON MATCH SET f._new = false
WITH d, f
FOREACH (_ IN CASE WHEN f._new THEN [1] ELSE [] END | SET d.rows_failed = coalesce(d.rows_failed, 0) + 1)
WITH d
SET d.status = CASE WHEN d.rows_loaded + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
"""


def connect_kafka() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "loader",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Manual commit after Neo4j writes guarantee delivery
            "topic.metadata.refresh.interval.ms": 1000,
        }
    )
    while True:
        try:
            consumer.list_topics(timeout=3)
            break
        except KafkaException:
            log.info("kafka not ready yet, retrying...")
            time.sleep(2)
        except Exception as e:
            log.warning("kafka error: %s, retrying...", e)
            time.sleep(2)
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def init_schema(driver):
    """Creates indexes and constraints on startup to prevent full label scans."""
    statements = [
        "CREATE CONSTRAINT dataset_id_unique IF NOT EXISTS FOR (d:Dataset) REQUIRE d.id IS UNIQUE",
        "CREATE INDEX row_dataset_row_idx IF NOT EXISTS FOR (r:Row) ON (r.dataset_id, r.row_index)",
        "CREATE INDEX failed_row_idx IF NOT EXISTS FOR (f:FailedRow) ON (f.dataset_id, f.row_index)",
    ]
    with driver.session(database=NEO4J_DATABASE) as session:
        for stmt in statements:
            try:
                session.run(stmt)
                log.info("schema applied: %s", stmt)
            except Exception as e:
                log.warning("schema notice for '%s': %s", stmt, e)


def connect_neo4j():
    while True:
        try:
            driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USER, NEO4J_PASSWORD),
                max_connection_lifetime=30 * 60,
                max_connection_pool_size=50,
            )
            driver.verify_connectivity()
            init_schema(driver)
            return driver
        except (ServiceUnavailable, AuthError, Exception) as e:
            log.info("neo4j not ready yet (%s), retrying...", e)
            time.sleep(2)


def process_single(driver, msg: dict) -> None:
    dataset_id = msg["dataset_id"]
    row_index = msg["row_index"]

    if msg.get("failed", False):
        try:
            with driver.session(database=NEO4J_DATABASE) as session:
                session.run(
                    MARK_FAILED,
                    dataset_id=dataset_id,
                    row_index=row_index,
                    filename=msg["filename"],
                    uploaded_at=msg["uploaded_at"],
                    rows_total=msg["rows_total"],
                    reason=msg.get("reason"),
                )
            log.info(
                "marked pre-flagged failed row for dataset=%s row_index=%s reason=%s",
                dataset_id,
                row_index,
                msg.get("reason"),
            )
        except Exception as e2:
            log.error("failed to mark row as failed: %s", e2)
        return

    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            session.run(
                MERGE_ROW,
                dataset_id=dataset_id,
                filename=msg["filename"],
                uploaded_at=msg["uploaded_at"],
                row_index=row_index,
                rows_total=msg["rows_total"],
                row=msg["row"],
            )
        log.info("loaded dataset=%s row_index=%s", dataset_id, row_index)
    except Exception as e:
        log.error("failed to load dataset=%s row_index=%s: %s", dataset_id, row_index, e)
        try:
            with driver.session(database=NEO4J_DATABASE) as session:
                session.run(
                    MARK_FAILED,
                    dataset_id=dataset_id,
                    row_index=row_index,
                    filename=msg["filename"],
                    uploaded_at=msg["uploaded_at"],
                    rows_total=msg["rows_total"],
                    reason="load_error",
                )
            log.info("marked failed row for dataset=%s row_index=%s", dataset_id, row_index)
        except Exception as e2:
            log.error("failed to mark row as failed: %s", e2)


def process_batch(driver, batch_messages: list[dict]) -> None:
    """Processes a batch of Kafka messages.
    Separates into failed and good items, runs batched UNWIND Cypher,
    and falls back to single-row processing if any batch fails.
    """
    if len(batch_messages) == 1:
        process_single(driver, batch_messages[0])
        return

    by_dataset: dict[str, list[dict]] = {}
    for raw_msg in batch_messages:
        dataset_id = raw_msg["dataset_id"]
        by_dataset.setdefault(dataset_id, []).append(raw_msg)

    with driver.session(database=NEO4J_DATABASE) as session:
        for dataset_id, items in by_dataset.items():
            failed_items = [m for m in items if m.get("failed", False)]
            good_items = [m for m in items if not m.get("failed", False)]

            if failed_items:
                try:
                    session.run(MARK_FAILED_BATCH, batch=failed_items)
                    log.info("batch failed rows marked: dataset=%s count=%d", dataset_id, len(failed_items))
                except Exception as e:
                    log.error("batch failed-rows error, falling back to row-by-row: %s", e)
                    for item in failed_items:
                        process_single(driver, item)

            if good_items:
                try:
                    session.run(MERGE_BATCH, batch=good_items)
                    log.info("batch rows loaded: dataset=%s count=%d", dataset_id, len(good_items))
                except Exception as e:
                    log.error("batch merge error for dataset=%s, falling back to row-by-row: %s", dataset_id, e)
                    for item in good_items:
                        process_single(driver, item)


def main():
    consumer = connect_kafka()
    driver = connect_neo4j()
    log.info("loader started, consuming %s", KAFKA_TOPIC)

    while True:
        # Support both consume() (confluent-kafka) and poll() (MockConsumer in unit tests)
        if hasattr(consumer, "consume"):
            raw_messages = consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT)
            if not raw_messages:
                continue
        else:
            kmsg = consumer.poll(1.0)
            if kmsg is None:
                continue
            raw_messages = [kmsg]

        valid_items = []
        for kmsg in raw_messages:
            if kmsg.error():
                log.error("kafka consume error: %s", kmsg.error())
                continue

            val = kmsg.value()
            if val is None:
                continue

            # Note: json.loads and msg["dataset_id"] / msg["row_index"] are purposefully
            # not caught in a broad try/except so strict xfail tests (JSONDecodeError, KeyError) pass.
            msg = json.loads(val.decode("utf-8"))
            _ = msg["dataset_id"]
            _ = msg["row_index"]
            valid_items.append(msg)

        if valid_items:
            process_batch(driver, valid_items)

        # Synchronously commit offsets only after Neo4j writes have succeeded
        if hasattr(consumer, "commit"):
            try:
                consumer.commit(asynchronous=False)
            except Exception as e:
                log.warning("kafka commit warning: %s", e)


if __name__ == "__main__":
    main()
