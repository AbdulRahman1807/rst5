# REMEDIATION REPORT: GraphSense Spatial Data Studio
## CSV → Kafka → Neo4j → Interactive Graph Visualizer & Cross-Dataset Reasoning

---

## 1. Verified Current State Before Remediation

An audit of the initial hackathon code before remediation revealed the following baseline state:

- **Pipeline Architecture:**
  - `FastAPI` backend (`api/main.py`) accepted CSV uploads, computed SHA-256 content hashes as logical dataset IDs, and published row payloads to the `csv-rows` Apache Kafka topic.
  - `Kafka Consumer` (`loader/main.py`) consumed rows sequentially and executed individual `MERGE` Cypher transactions into Neo4j (`neo4j:5.24-community`).
  - Throughput baseline for a 5,000-row file (`large.csv`) was **63.16 rows/sec** (total pipeline time ~79.2s).
- **Frontend Architecture:**
  - Single-file `ui/index.html` served via unprivileged Nginx (`nginxinc/nginx-unprivileged:1.27-alpine`), powered by React 18 UMD and in-browser Babel compilation (`@babel/standalone`).
  - UI was structured into separated top tabs with a navy-blue/cyan card visual identity.
- **Chatbot Architecture:**
  - Two-tier question answering system: optional Groq LLM with a fallback deterministic Cypher generator (`api/chatbot_deterministic.py`).
  - Questions were either scoped to a single `dataset_id` or unscoped across all rows in the graph.

---

## 2. Cross-CSV Audit & Architectural Findings

| Audit Question | Verified Reality | Remediation Implemented |
| :--- | :--- | :--- |
| **Can multiple distinct CSVs coexist in Neo4j?** | **Yes.** Each row stored a `dataset_id` property pointing to its parent `:Dataset` node. | Preserved multi-dataset coexistence and added index on `(r:Row {dataset_id})`. |
| **How are datasets identified?** | SHA-256 content hash: `hashlib.sha256(raw).hexdigest()`. | Preserved deterministic SHA-256 dataset identification. |
| **Does the same file uploaded twice map to one logical dataset?** | **Yes.** Neo4j MERGE ensures idempotency. | Verified by `scripts/verify_idempotency.sh`. |
| **How are row identities scoped?** | Scoped via composite `(dataset_id, row_index)` attributes. | Added composite index for instant lookups. |
| **Are duplicate column names or dynamic headers handled?** | Handled dynamically via python dictionary property mapping. | Maintained dynamic Cypher property mapping with safe string coercions. |
| **Can the chatbot query rows from multiple datasets?** | **No.** Queries were either scoped to one dataset or scanned all rows indiscriminately. No join concept existed. | Added explicit scopes: `single`, `cross_dataset`, `workspace`. |
| **Were there any inter-dataset relationships in the graph?** | **Zero.** The graph contained only `(:Dataset)-[:HAS_ROW]->(:Row)`. | Introduced `:DatasetRelation` nodes with confidence scores, overlap counts, and status tracking. |
| **Does `/datasets` list all distinct datasets accurately?** | **Yes.** Cypher queries `MATCH (d:Dataset)` accurately. | Preserved and enriched with column and relation counts. |

---

## 3. Visual Identity: Monochromatic Experimental Data Studio

The frontend was overhauled into an uncompromising **monochromatic digital instrument**:

- **Color Foundation:**
  - True black canvas (`#09090b` / `#000000`), obsidian panels (`#111114`), elevated dark zinc surfaces (`#16161a`), and crisp 1px borders (`#27272a` / `#3f3f46`).
  - Elimination of navy-blue gradients, glowing neon boxes, and generic SaaS dashboard cards.
  - Semantic accents are strictly functional: stark white `#ffffff` for confirmed relationships and primary data, muted amber `#f59e0b` for candidate links, and emerald `#10b981` for verified health metrics.
- **Typography-Led Editorial Hierarchy:**
  - Display face: `Outfit` (bold editorial titles, uppercase tracked section indices such as `01 // WORKSPACE OVERVIEW`, `02 // NODE VISUALIZER`).
  - Technical metadata: `JetBrains Mono` for dataset hashes, Cypher queries, data types, and join definitions.
  - Body text: `Inter` for crisp readability.
- **Collapsible Vertically Structured Workspace:**
  - Eliminated isolated tabs. The application behaves as a unified, vertical workspace with collapsible accordion sections and smooth anchor navigation.

---

## 4. Signature Interaction: Liquid Morphing Sidebar

A persistent navigation rail anchors the left side of the studio:
- **Fluid Morphing Transitions:** Smoothly expands from compact `64px` to `250px` on hover or click (`transition: width 0.3s cubic-bezier(0.16, 1, 0.3, 1)`).
- **Coordinated Reveals:** Section indices (`01` through `06`) remain visible in compact mode; labels and badges fade and slide in with coordinated opacity and transform transitions.
- **Active State Indicator:** An active indicator tracks the user's position as they scroll or click to jump directly to any workspace section.
- **Persistent Metadata:** Bottom badge displays live counts of loaded datasets and confirmed joins.

---

## 5. Node-Based CSV Visualizer

A custom interactive SVG-based spatial data model:

- **Entities Represented:**
  - **Dataset Nodes:** Circular hubs with dataset filename, row count, status dot, and an interactive `+`/`−` toggle for column blossom.
  - **Column Nodes:** Clicking `+` expands a dataset's columns radiating in an orbit around the parent hub with connection lines.
  - **Candidate Edges:** Rendered as dashed zinc lines (`stroke-dasharray="5 4"`) with interactive percentage badges (`87%`).
  - **Confirmed Edges:** Rendered as bold solid white lines with key names.
- **Interactions Supported:**
  - Pan & zoom (mouse drag, zoom buttons, layout reset).
  - Filtering by status (Show All vs Confirmed Only).
  - Side Inspector Drawer: Clicking any node or edge opens the slide-out inspector displaying detailed column stats, join definitions, and live tables of joined records.

---

## 6. Cross-Dataset Relationship Discovery & Join Reasoning

### 6.1 Discovery Algorithm (`POST /relationships/discover`)
1. **Name Similarity Score ($S_{name} \in [0, 1]$):**
   - Normalizes column names (removes `_id`, `id_`, `_key`, `_num`).
   - Exact match: `1.0`. Clean token equality: `0.9`. Substring: `0.75`. Fuzzy ratio via `difflib.SequenceMatcher`.
2. **Sample Value Overlap ($S_{overlap} \in [0, 1]$):**
   - Queries Neo4j for sample distinct values (up to 100 distinct from each dataset).
   - Computes overlap: $\text{coverage} = \frac{|\text{overlap}|}{\min(|S_1|, |S_2|)}$.
3. **Composite Confidence:**
   $$\text{Confidence} = 0.4 \times S_{name} + 0.6 \times \text{coverage}$$
4. **Cardinality Inference:** Checks uniqueness on both sides to classify as `1:1`, `1:N`, or `N:M`.
5. **Persistence in Neo4j:** Stored idempotently as `(:DatasetRelation {id, source_id, target_id, source_col, target_col, confidence, cardinality, status: 'candidate' | 'confirmed' | 'rejected', sample_matches})`.

### 6.2 Chatbot Reasoning Across Scopes
- **Scope = `single`:** Query is strictly isolated to `MATCH (d:Dataset {id: $dataset_id})-[:HAS_ROW]->(r:Row)`. Cannot leak rows from other datasets.
- **Scope = `cross_dataset` / `workspace`:**
  - If a confirmed relationship exists: Generates and executes a parameterized Cypher join matching rows where `r1[source_col] = r2[target_col]`. Returns grounded answers with join metadata and inspectable evidence.
  - If an unconfirmed candidate exists: Refuses to invent a join key; returns an honest message detailing the candidate link and instructing the user to review and confirm it in the visualizer first.
  - If no relationship exists: States plainly that no relationship exists.

---

## 7. Files Changed & Architectural Boundary Compliance

- **Untouched Artifacts:**
  - `REPORT.md`: **0 lines modified** (strictly preserved).
- **Single-File Frontend:**
  - `ui/index.html`: Completely rewritten to house the monochrome design system, liquid sidebar, SVG node visualizer, inspector drawer, relationship manager, and graph analyst in React 18 UMD + Babel.
- **Backend & Database:**
  - `api/analytics.py`: Added relationship discovery engine, edge persistence, join matching, workspace graph compiler, and sampled caching.
  - `api/chatbot_deterministic.py`: Added cross-dataset join intent and scoped Cypher queries.
  - `api/chatbot.py`: Exposed scope and dataset_ids parameters.
  - `api/main.py`: Added endpoints (`/relationships/discover`, `/relationships`, `/relationships/{id}/confirm`, `/relationships/{id}/reject`, `/relationships/{id}/matches`, `/workspace/graph`).
  - `docker-compose.yml`: Added `./api:/app` volume mount and `--reload` command for development velocity.
- **Test Suite:**
  - `scripts/test_platform_features.py`: Extended from 24 to 39 automated assertions covering relationships, joins, and scoping.
  - `test_data/customer_orders.csv`: Added companion test dataset with matching `customer` keys.

---

## 8. Test Evidence & Benchmark Results

### 8.1 Platform Feature Test Suite (39 Assertions)
```bash
python3 scripts/test_platform_features.py
```
```
==================================================
 1. HEALTH & CONNECTIVITY
==================================================
  [PASS] GET /health returns HTTP 200
  [PASS] Kafka broker is connected
  [PASS] Neo4j graph database is connected

==================================================
 2. DATASET REGISTRY
==================================================
  [PASS] GET /datasets returns HTTP 200
  [PASS] Datasets is a list
  [PASS] Found 6 loaded datasets in graph

==================================================
 3. AUTOMATED VISUAL ANALYTICS & PROFILING (Candidate A)
==================================================
  [PASS] GET /analytics/profile returns HTTP 200
  [PASS] Profile contains dataset overview
  [PASS] Columns profiled with type inference
  [PASS] Chart recommendations generated
  [PASS] Recommendation has valid chart schema
  [PASS] Recommendation contains real data rows

==================================================
 4. DATA QUALITY & ANOMALY PROFILING (Candidate E)
==================================================
  [PASS] GET /analytics/quality returns HTTP 200
  [PASS] Data health score calculated: 100%
  [PASS] Column missingness analyzed
  [PASS] Tukey's IQR outlier analysis present

==================================================
 5. NATURAL-LANGUAGE CHAT WITH CHARTS (Candidate B)
==================================================
  [PASS] Single-dataset scoped count question returns HTTP 200
  [PASS] Answer is grounded in real graph data
  [PASS] Executed Cypher evidence provided
  [PASS] Single dataset scope isolates rows correctly
  [PASS] Group breakdown returns HTTP 200
  [PASS] Irrelevant query returns HTTP 200
  [PASS] Honest grounded: false on irrelevant questions
  [PASS] Hostile query returned HTTP 200 (no crash)
  [PASS] Unsafe query rejected / ungrounded

==================================================
 6. CROSS-DATASET DISCOVERY, GRAPH & REASONING
==================================================
  [PASS] POST /relationships/discover returns HTTP 200
  [PASS] Discovered relationships returned as list
  [PASS] GET /relationships returns HTTP 200
  [PASS] Found 4 candidate relationships across datasets
  [PASS] Confirmed relationship 58c7a833bc83b85e
  [PASS] GET /relationships/{id}/matches returns HTTP 200
  [PASS] Retrieved 10 joined cross-dataset rows
  [PASS] GET /workspace/graph returns HTTP 200
  [PASS] Graph model has nodes and edges
  [PASS] Confirmed relationships tracked in graph stats
  [PASS] Cross-dataset join chat query returns HTTP 200
  [PASS] Cross-dataset answer is grounded
  [PASS] Join metadata exposed in answer
  [PASS] Joined records returned in evidence result

==================================================
 TEST SUMMARY
==================================================
Passed: 39 | Failed: 0
All platform feature tests passed successfully!
```

### 8.2 Idempotency & Hostile Input Tests
```bash
./scripts/verify_idempotency.sh
# PASS: identical job_id and row counts across two clean loads of the same file.

./scripts/test_hostile_input.sh
# PASS: test_data/empty.csv -> HTTP 400
# PASS: test_data/header_only.csv -> HTTP 400
# PASS: test_data/not_a_csv.txt -> HTTP 400
# PASS: test_data/broken.csv -> HTTP 202
# PASS: test_data/small_clean.csv -> HTTP 202
```

---

## 9. Demo Walkthrough

1. **Upload Companion CSV:** Upload `test_data/customer_orders.csv` via the dropzone. Kafka asynchronously streams the 12 rows into Neo4j.
2. **Discover Candidate Relationship:** Click **"Run Discovery"** in the Relationship Manager or visualizer toolbar. The engine discovers `small_clean.csv` (`customer`) ↔ `customer_orders.csv` (`customer`) with **87% confidence** and **7 overlapping sample keys** ("Acme Co", "Nova Retail", "Redwood Inc", etc.).
3. **Inspect Evidence & Confirm:** Click the candidate edge in the Node Visualizer to open the Inspector Drawer. Review the sample matching values, then click **"Confirm Relationship"**. The edge turns solid stark-white.
4. **Cross-Dataset Chat Query:** In the Natural-Language Analyst, switch scope to **Cross-Dataset Joins** and ask:
   *"Show matching records between small_clean.csv and customer_orders.csv"*
   - **Answer:** *"Found 10 joined record(s) between small_clean.csv and customer_orders.csv linked on confirmed key 'customer' = 'customer'."*
   - **Grounded Cypher Evidence:**
     ```cypher
     MATCH (d1:Dataset {id: $s_id})-[:HAS_ROW]->(r1:Row),
           (d2:Dataset {id: $t_id})-[:HAS_ROW]->(r2:Row)
     WHERE r1.`customer` IS NOT NULL AND toString(r1.`customer`) = toString(r2.`customer`)
     RETURN toString(r1.`customer`) AS join_key,
            properties(r1) AS `small_clean.csv`,
            properties(r2) AS `customer_orders.csv`
     LIMIT 25
     ```
   - **Results Table:** Shows joined attributes (e.g. Acme Co with budget `128` from `small_clean.csv` joined to orders `ORD-1001` and `ORD-1002` with totals `128.50` and `450.00` from `customer_orders.csv`).

---

## 10. Remaining Limitations & Edge Cases

1. **Ragged CSV Headers:** CSVs with missing headers (like `broken.csv`) have data values treated as column names (e.g. `Acme Co`). While safely escaped in Cypher (`_quote_ident`), candidate generation can infer semantic similarity between data values and columns.
2. **Sampling Boundary:** For datasets with $> 3,000$ rows, discovery evaluates up to 1,000 sample rows per column to maintain sub-second response times. Low-cardinality keys with rare matches outside the sample window may yield lower candidate confidence scores.
3. **Deterministic Cypher Grammar:** The deterministic chatbot engine currently supports pairwise joins across two connected datasets. Multi-hop joins across three or more datasets (e.g. $A \to B \to C$) require explicit chaining.
