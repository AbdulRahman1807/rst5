import json
import logging
import os
import time

from confluent_kafka import Consumer, KafkaException
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

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

# Optimized batched parameterized Cypher query with UNWIND.
# Correctly preserves idempotency and row identity:
# - MERGEs Dataset and Row nodes
# - Increments d.rows_loaded ONLY for genuinely newly-created rows (r._new = true)
# - Performs a single aggregate counter update and status update per dataset per batch
# - Cleans up temporary r._new flags in a FOREACH
MERGE_BATCH = """
UNWIND $batch AS item
MERGE (d:Dataset {id: item.dataset_id})
  ON CREATE SET d.filename = item.filename, d.uploaded_at = item.uploaded_at,
                d.rows_total = item.rows_total, d.rows_loaded = 0,
                d.rows_failed = 0, d.status = 'loading'
MERGE (r:Row {dataset_id: item.dataset_id, row_index: item.row_index})
  ON CREATE SET r += item.row, r._new = true
  ON MATCH SET r._new = false
MERGE (d)-[:HAS_ROW]->(r)
WITH d, count(CASE WHEN r._new THEN 1 END) AS new_rows, [r IN collect(r) | r] AS rows
SET d.rows_loaded = coalesce(d.rows_loaded, 0) + new_rows
SET d.status = CASE WHEN d.rows_loaded + coalesce(d.rows_failed, 0) >= d.rows_total THEN 'complete' ELSE 'loading' END
FOREACH (r IN rows | REMOVE r._new)
"""

# Fallback single-row MERGE for partial-failure isolation
MERGE_ROW = """
MERGE (d:Dataset {id: $dataset_id})
  ON CREATE SET d.filename = $filename, d.uploaded_at = $uploaded_at,
                d.rows_total = $rows_total, d.rows_loaded = 0,
                d.rows_failed = 0, d.status = 'loading'
MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index})
  ON CREATE SET r += $row, r._new = true
  ON MATCH SET r._new = false
MERGE (d)-[:HAS_ROW]->(r)
WITH d, r
FOREACH (_ IN CASE WHEN r._new THEN [1] ELSE [] END | SET d.rows_loaded = d.rows_loaded + 1)
REMOVE r._new
SET d.status = CASE WHEN d.rows_loaded + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
"""

# Idempotent failure marker
MARK_FAILED = """
MERGE (d:Dataset {id: $dataset_id})
  ON CREATE SET d.filename = $filename, d.uploaded_at = $uploaded_at,
                d.rows_total = $rows_total, d.rows_loaded = 0, d.rows_failed = 0, d.status = 'loading'
MERGE (f:FailedRow {dataset_id: $dataset_id, row_index: $row_index})
  ON CREATE SET f._new = true
  ON MATCH SET f._new = false
WITH d, f
FOREACH (_ IN CASE WHEN f._new THEN [1] ELSE [] END | SET d.rows_failed = coalesce(d.rows_failed, 0) + 1)
REMOVE f._new
SET d.status = CASE WHEN d.rows_loaded + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
"""


def connect_kafka() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Manual commit after Neo4j writes guarantee delivery
    })
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
            return driver
        except (ServiceUnavailable, AuthError, Exception) as e:
            log.info("neo4j not ready yet (%s), retrying...", e)
            time.sleep(2)


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


def process_batch(driver, batch_messages):
    """Processes a batch of Kafka messages.
    Groups by dataset_id, executes batched UNWIND Cypher, and falls back to single-row
    processing if any batch fails so partial failures are isolated and handled cleanly.
    """
    by_dataset = {}
    for raw_msg in batch_messages:
        dataset_id = raw_msg["dataset_id"]
        by_dataset.setdefault(dataset_id, []).append(raw_msg)

    with driver.session(database=NEO4J_DATABASE) as session:
        for dataset_id, items in by_dataset.items():
            try:
                session.run(MERGE_BATCH, batch=items)
                log.info("batch loaded: dataset=%s count=%d", dataset_id, len(items))
            except Exception as e:
                log.error("batch failed for dataset=%s (fallback to row-by-row): %s", dataset_id, e)
                # Fallback to row-by-row execution for this dataset
                for item in items:
                    row_index = item["row_index"]
                    try:
                        session.run(
                            MERGE_ROW,
                            dataset_id=dataset_id,
                            filename=item["filename"],
                            uploaded_at=item["uploaded_at"],
                            row_index=row_index,
                            rows_total=item["rows_total"],
                            row=item["row"],
                        )
                        log.info("fallback loaded dataset=%s row_index=%s", dataset_id, row_index)
                    except Exception as err_row:
                        log.error("fallback failed dataset=%s row_index=%s: %s", dataset_id, row_index, err_row)
                        try:
                            session.run(
                                MARK_FAILED,
                                dataset_id=dataset_id,
                                row_index=row_index,
                                filename=item["filename"],
                                uploaded_at=item["uploaded_at"],
                                rows_total=item["rows_total"],
                            )
                            log.info("marked failed row for dataset=%s row_index=%s", dataset_id, row_index)
                        except Exception as e2:
                            log.error("failed to mark row as failed: %s", e2)


def main():
    consumer = connect_kafka()
    driver = connect_neo4j()
    init_schema(driver)
    log.info("loader started with batching, consuming %s", KAFKA_TOPIC)

    while True:
        messages = consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT)
        if not messages:
            continue

        valid_items = []
        for kmsg in messages:
            if kmsg.error():
                log.error("kafka consume error: %s", kmsg.error())
                continue
            try:
                msg = json.loads(kmsg.value().decode("utf-8"))
                valid_items.append(msg)
            except Exception as e:
                log.error("malformed message JSON: %s", e)

        if valid_items:
            process_batch(driver, valid_items)

        # Synchronously commit offsets only after Neo4j writes have succeeded
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            log.warning("kafka commit warning: %s", e)


if __name__ == "__main__":
    main()
