# CSV → Kafka → Neo4j → Analytics & Chat Workbench

**RISE @ RST #5 Hackathon Project** — A high-throughput, demo-ready streaming analytics platform that ingests arbitrary CSV spreadsheets through Apache Kafka into a Neo4j knowledge graph, profiles data automatically, detects statistical anomalies, and answers natural-language questions with inline interactive charts.

---

## 🚀 Live Demo & Quick Start

```bash
cp .env.example .env
# Edit .env: set GROQ_API_KEY (optional — chatbot has a deterministic fallback, works 100% offline)
docker compose up -d --build
```

Access the services:
- **Analytics Workbench UI**: [http://localhost:3001](http://localhost:3001) (or `http://localhost:3000` if `UI_PORT` is unset)
- **FastAPI Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Neo4j Browser**: [http://localhost:7474](http://localhost:7474) (`neo4j` / `csvgraphdb`, database: `csv-graph-db`)

---

## 🌟 Key Upgrades & Features

### 1. Automated Visual Analytics & Dataset Profiling
- **Zero-Configuration Type Inference**: Detects `numeric`, `categorical`, `date_like`, and `text` columns dynamically from arbitrary CSV schemas.
- **Descriptive Statistics**: Real-time computation of min, max, mean, sum, stddev, distinct counts, and null percentages.
- **Smart Chart Recommendations**: Generates distribution bar charts, doughnut breakdown charts, and group-by aggregate charts (`/analytics/profile`) rendered with Chart.js.
- **Interactive Column Explorer**: Live column and chart type switcher (Bar, Line, Doughnut).

### 2. Natural-Language Visualizations in Chat
- **Inline Interactive Charts**: When queries involve distributions, group-by aggregates, or top-N rankings, the chatbot returns structured chart configurations rendered directly inside chat cards.
- **Architectural Grounding**: Every answer and chart is derived strictly from executed Cypher query results (`grounded: true`).
- **Expandable Evidence**: Inspect executed Cypher queries and raw graph result tables with one-click copy.
- **Zero-Dependency Fallback**: Works with Groq LLM when configured, and falls back seamlessly to the deterministic Cypher generator when offline.

### 3. Data Quality & Statistical Anomaly Profiling
- **Holistic Health Score (0–100%)**: Evaluates missing cell rates, failed row penalties, outlier density, and duplicates.
- **Tukey’s IQR Outlier Detection**: Identifies statistical outliers using the non-parametric $[Q1 - 1.5 \times IQR, Q3 + 1.5 \times IQR]$ rule without assuming normal distribution.
- **Completeness Auditing**: Column-by-column missingness breakdown with actionable quality warnings.

### 4. 5.1× Ingestion Throughput Optimization
- **Schema Uniqueness Constraints & Indexes**: Added startup indexes on `:Dataset(id)` and `:Row(dataset_id, row_index)` to eliminate full-table label scans on MERGE.
- **Batched Parameterized `UNWIND` Cypher**: Replaced per-row transactions with batched Cypher UNWIND transactions (up to 200 rows/batch).
- **Manual Kafka Commit Safety**: Offsets are committed synchronously only *after* Neo4j transactions commit, guaranteeing zero data loss.
- **Benchmark Results**:
  - Baseline (`large.csv`, 5,000 rows): **63.16 rows/sec** (79.2s total pipeline time).
  - Optimized (`benchmark_5k.csv`, 5,000 rows): **322.70 rows/sec** (15.5s total pipeline time, **652.7 rows/sec** pure graph persistence rate).

---

## 🧪 Verification & Benchmarks

Run the complete automated test suite:

```bash
# 1. Platform Feature & Endpoint Test Suite (24 test assertions)
python3 scripts/test_platform_features.py

# 2. Idempotency Verification (double load produces identical row counts)
scripts/verify_idempotency.sh

# 3. Hostile Input Verification (empty, header-only, non-CSV, broken/ragged files)
scripts/test_hostile_input.sh

# 4. Reproducible Monotonic Ingestion Benchmark
python3 scripts/benchmark_ingest.py test_data/benchmark_5k.csv
```

---

## 📁 Repository Structure

```
api/
  ├── main.py                    FastAPI app, /ingest, /status, /datasets, /analytics/*, /chat
  ├── analytics.py               Dataset profiling, chart recommendations, Tukey's IQR quality engine
  ├── chatbot.py                 Two-tier LLM/deterministic chat coordinator with chart inference
  └── chatbot_deterministic.py   Read-only Cypher generator with structured chart payloads
loader/
  └── main.py                    Kafka consumer with batched UNWIND Cypher & schema indexing
ui/
  ├── index.html                 React 18 & Chart.js data workbench application
  └── vendor/                    Offline dependencies (React, ReactDOM, Babel, Chart.js)
scripts/
  ├── benchmark_ingest.py        Monotonic clock ingestion benchmark script
  ├── test_platform_features.py  End-to-end verification test suite
  ├── verify_idempotency.sh      Re-upload deduplication verification
  ├── test_hostile_input.sh      Hostile input and negative testing
  └── seed_neo4j.py              Direct Neo4j seeding utility
test_data/
  ├── small_clean.csv            15-row clean baseline fixture
  ├── large.csv                  5,000-row stress testing fixture
  ├── benchmark_5k.csv           5,000-row benchmark dataset
  └── broken.csv / empty.csv ... Hostile input test fixtures
```
