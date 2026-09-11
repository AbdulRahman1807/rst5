"""Load a CSV directly into Neo4j, bypassing api/Kafka/loader entirely.

For testing the chatbot (LLM + deterministic tiers) against real data the moment the neo4j
container is healthy, without waiting on kafka/api/loader to also be up and wired correctly.
Uses the exact same dataset_id derivation and MERGE pattern as the real pipeline
(api/main.py + loader/main.py) so results are representative of the real thing.

Usage:
    python3 scripts/seed_neo4j.py test_data/small_clean.csv
"""
import csv
import hashlib
import os
import sys
import time

from neo4j import GraphDatabase

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


def main():
    if len(sys.argv) != 2:
        print("usage: python3 scripts/seed_neo4j.py <path-to-csv>")
        sys.exit(1)

    path = sys.argv[1]
    with open(path, "rb") as f:
        raw = f.read()
    dataset_id = hashlib.sha256(raw).hexdigest()

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    rows_total = len(rows)
    uploaded_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    filename = os.path.basename(path)

    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "csvgraphdb")
    database = os.environ.get("NEO4J_DATABASE", "csv-graph-db")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()

    with driver.session(database=database) as session:
        for i, row in enumerate(rows):
            session.run(
                MERGE_ROW,
                dataset_id=dataset_id,
                filename=filename,
                uploaded_at=uploaded_at,
                row_index=i,
                rows_total=rows_total,
                row=row,
            )

    print(f"Seeded {rows_total} rows from {filename} as dataset_id={dataset_id}")


if __name__ == "__main__":
    main()
