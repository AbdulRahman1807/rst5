import json
import logging
import os
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("api")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "csvgraphdb")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "CSV_Graph_DB")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
KAFKA_TOPIC = "csv-rows"

app = FastAPI(title="csv-graph-chat api (Phase 1 Skeleton)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_producer = None


def get_producer() -> KafkaProducer | None:
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                request_timeout_ms=5000,
            )
            log.info("Connected to Kafka producer at %s", KAFKA_BROKERS)
        except (NoBrokersAvailable, Exception) as e:
            log.warning("Kafka producer connection failed: %s", e)
            return None
    return _producer


@app.get("/health")
def health():
    """Dummy health endpoint for Phase 1 skeleton."""
    return {
        "status": "ok",
        "kafka_connected": True,
        "neo4j_connected": True,
    }


@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    """Accepts any file upload, writes ONE dummy message to Kafka, returns fake job response."""
    # Read file content to accept upload
    _ = await file.read()
    filename = file.filename or "uploaded.csv"

    dummy_message = {
        "dataset_id": "dummy123",
        "filename": filename,
        "row_index": 0,
        "rows_total": 1,
        "row": {"column_1": "dummy_value", "status": "skeleton_demo"},
    }

    try:
        producer = get_producer()
        if producer is not None:
            future = producer.send(KAFKA_TOPIC, key="dummy123", value=dummy_message)
            producer.flush(timeout=5)
            log.info("Successfully published 1 dummy message to topic '%s'", KAFKA_TOPIC)
        else:
            log.warning("Kafka producer not ready; skipped message send in dummy mode")
    except Exception as e:
        log.warning("Error publishing dummy message to Kafka: %s", e)

    return {
        "job_id": "dummy123",
        "rows_received": 1,
        "status": "queued",
    }


@app.get("/status")
def status(job_id: str = "dummy123"):
    """Hardcoded complete response for Phase 1 skeleton."""
    return {
        "job_id": job_id,
        "status": "complete",
        "rows_total": 1,
        "rows_loaded": 1,
        "rows_failed": 0,
    }


@app.post("/chat")
def chat(payload: dict = None):
    """Hardcoded canned answer with grounded: False for Phase 1 skeleton."""
    return {
        "answer": "This is a Phase 1 skeleton canned response. Pipeline plumbing active.",
        "cypher": "MATCH (n:DummyNode) RETURN count(n);",
        "result": [{"count(n)": 1}],
        "grounded": False,
    }
