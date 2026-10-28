import os

# Set test environment variables so importing api/loader modules does not fail on missing env
os.environ.setdefault("NEO4J_PASSWORD", "csvgraphdb")
os.environ.setdefault("NEO4J_DATABASE", "csv-graph-db")
os.environ.setdefault("NEO4J_URI", "bolt://localhost:7687")
os.environ.setdefault("NEO4J_USER", "neo4j")
os.environ.setdefault("KAFKA_BROKERS", "localhost:9092")
