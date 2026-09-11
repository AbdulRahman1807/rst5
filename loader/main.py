import json
import logging
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("loader")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "csvgraphdb")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "CSV_Graph_DB")
KAFKA_TOPIC = "csv-rows"

MERGE_DUMMY_QUERY = """
MERGE (d:Dataset {id: $dataset_id})
  ON CREATE SET d.filename = $filename, d.status = 'complete', d.rows_loaded = 1
MERGE (r:DummyNode {dataset_id: $dataset_id})
  ON CREATE SET r.loaded_at = timestamp()
MERGE (d)-[:HAS_ROW]->(r)
"""


def connect_kafka() -> KafkaConsumer:
    """Connect to Kafka broker with retry loop until available."""
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id="dummy-loader-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            log.info("Successfully connected to Kafka at %s", KAFKA_BROKERS)
            return consumer
        except NoBrokersAvailable:
            log.info("Kafka broker not ready yet, retrying in 2 seconds...")
            time.sleep(2)
        except Exception as e:
            log.warning("Kafka connection error: %s, retrying in 2 seconds...", e)
            time.sleep(2)


def connect_neo4j():
    """Connect to Neo4j database with retry loop until reachable."""
    while True:
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            driver.verify_connectivity()
            log.info("Successfully connected to Neo4j at %s", NEO4J_URI)
            return driver
        except (ServiceUnavailable, AuthError, Exception) as e:
            log.info("Neo4j not ready yet (%s), retrying in 2 seconds...", e)
            time.sleep(2)


def main():
    log.info("Starting Phase 1 dummy loader...")
    consumer = connect_kafka()
    driver = connect_neo4j()
    log.info("Dummy loader active and listening for messages on topic '%s'...", KAFKA_TOPIC)

    for message in consumer:
        msg = message.value
        dataset_id = msg.get("dataset_id", "dummy123")
        filename = msg.get("filename", "dummy.csv")
        log.info("Consumed dummy message from Kafka: %s", msg)

        try:
            with driver.session(database=NEO4J_DATABASE) as session:
                session.run(
                    MERGE_DUMMY_QUERY,
                    dataset_id=dataset_id,
                    filename=filename,
                )
            log.info("done")
        except Exception as e:
            log.error("Failed to MERGE dummy node into Neo4j: %s", e)


if __name__ == "__main__":
    main()
