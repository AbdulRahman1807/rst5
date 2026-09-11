import hashlib
import io
import json
import logging
import os
import time

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

import chatbot

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("api")

KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
NEO4J_DATABASE = os.environ["NEO4J_DATABASE"]
KAFKA_TOPIC = "csv-rows"

app = FastAPI(title="csv-graph-chat api")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# job_id -> {rows_received, received_at} — bookkeeping about the upload itself only.
# Lost on restart; acceptable per IMPLEMENTATION_PLAN.md (Neo4j is authoritative once loading starts).
jobs: dict[str, dict] = {}

_producer: KafkaProducer | None = None
_driver = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
    return _producer


def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return _driver


@app.get("/health")
def health():
    kafka_connected = False
    neo4j_connected = False

    try:
        get_producer().bootstrap_connected()
        kafka_connected = True
    except Exception as e:
        log.warning("kafka health check failed: %s", e)

    try:
        get_driver().verify_connectivity()
        neo4j_connected = True
    except (ServiceUnavailable, AuthError, Exception) as e:
        log.warning("neo4j health check failed: %s", e)

    status = "ok" if kafka_connected and neo4j_connected else "not_ok"
    return {"status": status, "kafka_connected": kafka_connected, "neo4j_connected": neo4j_connected}


@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty file")
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="file must be a .csv")

    dataset_id = hashlib.sha256(raw).hexdigest()

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="file is not valid UTF-8 text/CSV")

    import csv
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if reader.fieldnames is None:
        raise HTTPException(status_code=400, detail="not a valid CSV (no header row)")
    if any(f is None or not f.strip() for f in reader.fieldnames):
        raise HTTPException(status_code=400, detail="not a valid CSV (malformed header row)")
    if not rows:
        raise HTTPException(status_code=400, detail="CSV has a header but zero data rows")

    rows_total = len(rows)
    uploaded_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    jobs[dataset_id] = {"rows_received": rows_total, "received_at": uploaded_at}

    producer = get_producer()
    for i, row in enumerate(rows):
        message = {
            "dataset_id": dataset_id,
            "filename": file.filename,
            "uploaded_at": uploaded_at,
            "row_index": i,
            "rows_total": rows_total,
            "row": row,
        }
        producer.send(KAFKA_TOPIC, key=dataset_id, value=message)
    producer.flush()

    return {"job_id": dataset_id, "rows_received": rows_total, "status": "queued"}


@app.get("/status")
def status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="unknown job_id")

    rows_total = jobs[job_id]["rows_received"]

    with get_driver().session(database=NEO4J_DATABASE) as session:
        record = session.run(
            "MATCH (d:Dataset {id: $id}) RETURN d.status AS status, "
            "d.rows_total AS rows_total, d.rows_loaded AS rows_loaded, d.rows_failed AS rows_failed",
            id=job_id,
        ).single()

    if record is None:
        return {"job_id": job_id, "status": "queued", "rows_total": rows_total, "rows_loaded": 0, "rows_failed": 0}

    return {
        "job_id": job_id,
        "status": record["status"],
        "rows_total": record["rows_total"],
        "rows_loaded": record["rows_loaded"],
        "rows_failed": record["rows_failed"],
    }


@app.post("/chat")
def chat(payload: dict):
    question = payload.get("question", "")
    return chatbot.answer_question(get_driver(), NEO4J_DATABASE, question)
