import json
import logging
import os
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
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
MERGE_ROW = """
MERGE (d:Dataset {id: $dataset_id})
  ON CREATE SET d.filename = $filename, d.uploaded_at = $uploaded_at,
                d.rows_total = $rows_total, d.rows_loaded = 0,
                d.rows_failed = 0, d.status = 'loading'
MERGE (r:Row {dataset_id: $dataset_id, row_index: $row_index})
  ON CREATE SET r += $row
MERGE (d)-[:HAS_ROW]->(r)
SET d.rows_loaded = d.rows_loaded + 1,
    d.status = CASE WHEN d.rows_loaded + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
"""

MARK_FAILED = """
MATCH (d:Dataset {id: $dataset_id})
SET d.rows_failed = coalesce(d.rows_failed, 0) + 1,
    d.status = CASE WHEN d.rows_loaded + d.rows_failed >= d.rows_total THEN 'complete' ELSE 'loading' END
"""


def connect_kafka() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id="loader",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        except NoBrokersAvailable:
            log.info("kafka not ready yet, retrying...")
            time.sleep(2)


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

    for message in consumer:
        msg = message.value
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
                    session.run(MARK_FAILED, dataset_id=dataset_id)
            except Exception as e2:
                log.error("failed to mark row as failed: %s", e2)


if __name__ == "__main__":
    main()
