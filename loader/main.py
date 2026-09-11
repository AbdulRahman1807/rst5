import json
import logging
import os
import time

from confluent_kafka import Consumer, KafkaException
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("loader")

KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
NEO4J_DATABASE = os.environ["NEO4J_DATABASE"]
KAFKA_TOPIC = "csv-rows"

# Idempotent per IMPLEMENTATION_PLAN.md: MERGE keyed on dataset_id (sha256 of file) + row_index, never CREATE.
# rows_loaded/rows_failed must only increment the first time a given row_index is seen for a dataset_id,
# so re-publishing the same CSV (same dataset_id) leaves counts unchanged on repeat runs. A bare
# `SET d.rows_loaded = d.rows_loaded + 1` after a MERGE would double-count on every re-run since it fires
# whether or not the Row node was actually just created — hence the ON CREATE/ON MATCH + FOREACH guard below.
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

# Same idempotency concern as above: a failing row_index must only count once per dataset_id even if the
# same CSV (and thus the same failure) is re-published, so a FailedRow marker node gates the increment.
MARK_FAILED = """
MATCH (d:Dataset {id: $dataset_id})
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
        "enable.auto.commit": True,
    })
    while True:
        try:
            consumer.list_topics(timeout=3)  # forces a broker round-trip; raises if not reachable yet
            break
        except KafkaException:
            log.info("kafka not ready yet, retrying...")
            time.sleep(2)
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def connect_neo4j():
    while True:
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            driver.verify_connectivity()
            return driver
        except ServiceUnavailable:
            log.info("neo4j not ready yet, retrying...")
            time.sleep(2)


def main():
    consumer = connect_kafka()
    driver = connect_neo4j()
    log.info("loader started, consuming %s", KAFKA_TOPIC)

    while True:
        kmsg = consumer.poll(1.0)
        if kmsg is None:
            continue
        if kmsg.error():
            log.error("kafka consume error: %s", kmsg.error())
            continue

        msg = json.loads(kmsg.value().decode("utf-8"))
        dataset_id = msg["dataset_id"]
        row_index = msg["row_index"]
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
                    session.run(MARK_FAILED, dataset_id=dataset_id, row_index=row_index)
            except Exception as e2:
                log.error("failed to mark row as failed: %s", e2)


if __name__ == "__main__":
    main()
