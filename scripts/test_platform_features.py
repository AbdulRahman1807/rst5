#!/usr/bin/env python3
"""Comprehensive Platform Verification Test Suite

Tests:
1. Healthcheck & Connectivity
2. Dataset Registry & Ingestion Status
3. Automated Profiling & Visual Analytics (Candidate A)
4. Chatbot Grounding & Inline Chart Generation (Candidate B)
5. Data Quality, Missingness & Tukey's IQR Outlier Detection (Candidate E)
6. Cypher Read-Only Security Guardrails
7. Hostile / Malformed Input Handling
"""
import json
import os
import sys
import time
import urllib.request
import urllib.parse

API_BASE = os.getenv("API_BASE", "http://localhost:8000")


def req(path: str, method: str = "GET", data: dict = None) -> tuple[int, dict | str]:
    url = f"{API_BASE}{path}"
    headers = {"Content-Type": "application/json"} if data else {}
    body = json.dumps(data).encode("utf-8") if data else None
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request) as resp:
            content = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(content)
            except Exception:
                return resp.status, content
    except urllib.error.HTTPError as e:
        content = e.read().decode("utf-8")
        try:
            return e.code, json.loads(content)
        except Exception:
            return e.code, content


def run_tests():
    passed = 0
    failed = 0

    def assert_test(cond: bool, name: str):
        nonlocal passed, failed
        if cond:
            print(f"  [PASS] {name}")
            passed += 1
        else:
            print(f"  [FAIL] {name}", file=sys.stderr)
            failed += 1

    print("==================================================")
    print(" 1. HEALTH & CONNECTIVITY")
    print("==================================================")
    code, health = req("/health")
    assert_test(code == 200, "GET /health returns HTTP 200")
    assert_test(health.get("kafka_connected") is True, "Kafka broker is connected")
    assert_test(health.get("neo4j_connected") is True, "Neo4j graph database is connected")

    print("\n==================================================")
    print(" 2. DATASET REGISTRY")
    print("==================================================")
    code, datasets = req("/datasets")
    assert_test(code == 200, "GET /datasets returns HTTP 200")
    assert_test(isinstance(datasets, list), "Datasets is a list")
    assert_test(len(datasets) > 0, f"Found {len(datasets)} loaded datasets in graph")

    active_job_id = next((d["id"] for d in datasets if d.get("filename") == "small_clean.csv"), datasets[0]["id"] if datasets else None)

    print("\n==================================================")
    print(" 3. AUTOMATED VISUAL ANALYTICS & PROFILING (Candidate A)")
    print("==================================================")
    if active_job_id:
        code, profile = req(f"/analytics/profile?job_id={active_job_id}")
        assert_test(code == 200, "GET /analytics/profile returns HTTP 200")
        assert_test("overview" in profile, "Profile contains dataset overview")
        assert_test("columns" in profile and len(profile["columns"]) > 0, "Columns profiled with type inference")
        assert_test("recommendations" in profile, "Chart recommendations generated")

        recs = profile.get("recommendations", [])
        if recs:
            r = recs[0]
            assert_test("type" in r and "data" in r and "x_key" in r and "y_key" in r, "Recommendation has valid chart schema")
            assert_test(len(r["data"]) > 0, "Recommendation contains real data rows")

    print("\n==================================================")
    print(" 4. DATA QUALITY & ANOMALY PROFILING (Candidate E)")
    print("==================================================")
    if active_job_id:
        code, quality = req(f"/analytics/quality?job_id={active_job_id}")
        assert_test(code == 200, "GET /analytics/quality returns HTTP 200")
        assert_test("health_score" in quality, f"Data health score calculated: {quality.get('health_score')}%")
        assert_test("missingness" in quality, "Column missingness analyzed")
        assert_test("outliers" in quality, "Tukey's IQR outlier analysis present")

    print("\n==================================================")
    print(" 5. NATURAL-LANGUAGE CHAT WITH CHARTS (Candidate B)")
    print("==================================================")
    code, chat1 = req("/chat", "POST", {"question": "How many rows in total?", "dataset_id": active_job_id, "scope": "single"})
    assert_test(code == 200, "Single-dataset scoped count question returns HTTP 200")
    assert_test(chat1.get("grounded") is True, "Answer is grounded in real graph data")
    assert_test(chat1.get("cypher") is not None, "Executed Cypher evidence provided")
    assert_test("15 rows in this dataset" in chat1.get("answer", ""), "Single dataset scope isolates rows correctly")

    code, chat2 = req("/chat", "POST", {"question": "Show row counts per group", "dataset_id": active_job_id})
    assert_test(code == 200, "Group breakdown returns HTTP 200")
    if chat2.get("grounded") and "chart" in chat2 and chat2["chart"]:
        c = chat2["chart"]
        assert_test(c.get("type") == "bar" and len(c.get("data", [])) > 0, "Chat returned structured inline chart")

    code, chat3 = req("/chat", "POST", {"question": "What is the capital of France?"})
    assert_test(code == 200, "Irrelevant query returns HTTP 200")
    assert_test(chat3.get("grounded") is False, "Honest grounded: false on irrelevant questions")

    code, chat4 = req("/chat", "POST", {"question": "DROP DATABASE neo4j"})
    assert_test(code == 200, "Hostile query returned HTTP 200 (no crash)")
    assert_test(chat4.get("grounded") is False, "Unsafe query rejected / ungrounded")

    print("\n==================================================")
    print(" 6. CROSS-DATASET DISCOVERY, GRAPH & REASONING")
    print("==================================================")
    code, disc = req("/relationships/discover", "POST", {})
    assert_test(code == 200, "POST /relationships/discover returns HTTP 200")
    assert_test(isinstance(disc, list), "Discovered relationships returned as list")

    code, rels = req("/relationships", "GET")
    assert_test(code == 200, "GET /relationships returns HTTP 200")
    assert_test(isinstance(rels, list) and len(rels) > 0, f"Found {len(rels)} candidate relationships across datasets")

    confirmed_rel = next((r for r in rels if r.get("source_col") == "customer" and r.get("target_col") == "customer"), None)
    if confirmed_rel:
        rel_id = confirmed_rel["id"]
        code, conf_res = req(f"/relationships/{rel_id}/confirm", "POST")
        assert_test(code == 200 and conf_res.get("status") == "confirmed", f"Confirmed relationship {rel_id}")

        code, matches_res = req(f"/relationships/{rel_id}/matches", "GET")
        assert_test(code == 200, "GET /relationships/{id}/matches returns HTTP 200")
        assert_test(matches_res.get("total_joined_rows", 0) > 0, f"Retrieved {matches_res.get('total_joined_rows')} joined cross-dataset rows")

    code, graph_model = req("/workspace/graph", "GET")
    assert_test(code == 200, "GET /workspace/graph returns HTTP 200")
    assert_test("nodes" in graph_model and "edges" in graph_model, "Graph model has nodes and edges")
    assert_test(graph_model["stats"]["confirmed_relationships"] > 0, "Confirmed relationships tracked in graph stats")

    code, cross_chat = req("/chat", "POST", {
        "question": "show matching records between small_clean.csv and customer_orders.csv",
        "scope": "cross_dataset"
    })
    assert_test(code == 200, "Cross-dataset join chat query returns HTTP 200")
    assert_test(cross_chat.get("grounded") is True, "Cross-dataset answer is grounded")
    assert_test("join_used" in cross_chat and cross_chat["join_used"].get("status") == "confirmed", "Join metadata exposed in answer")
    assert_test(len(cross_chat.get("result", [])) > 0, "Joined records returned in evidence result")

    print("\n==================================================")
    print(" TEST SUMMARY")
    print("==================================================")
    print(f"Passed: {passed} | Failed: {failed}")
    if failed > 0:
        sys.exit(1)
    print("All platform feature tests passed successfully!\n")


if __name__ == "__main__":
    run_tests()
