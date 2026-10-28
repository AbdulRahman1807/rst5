"""Dataset Profiling, Exploratory Visual Analytics, and Data Quality Engine.

Provides deep automated analysis of uploaded CSV datasets stored in Neo4j:
- Type inference (numeric, categorical, date_like, text)
- Comprehensive statistics (min, max, mean, sum, stddev, distinct counts, null counts)
- Top category distributions & aggregations
- Automated chart recommendations
- Data quality health scoring & Tukey's IQR statistical outlier detection
- Cross-dataset candidate relationship discovery, scoring, and graph generation
"""
import hashlib
import logging
import math
import re
from difflib import SequenceMatcher

log = logging.getLogger("analytics")

_profile_cache: dict[str, dict] = {}
_quality_cache: dict[str, dict] = {}


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _run_read(driver, database: str, cypher: str, params: dict | None = None) -> list[dict]:
    params = params or {}
    def _tx(tx):
        return [r.data() for r in tx.run(cypher, **params)]

    with driver.session(database=database) as session:
        return session.execute_read(_tx)


def _run_write(driver, database: str, cypher: str, params: dict | None = None) -> list[dict]:
    params = params or {}
    def _tx(tx):
        return [r.data() for r in tx.run(cypher, **params)]

    with driver.session(database=database) as session:
        return session.execute_write(_tx)


def get_all_datasets(driver, database: str) -> list[dict]:
    """Returns a list of all ingested datasets with summary metrics."""
    cypher = """
    MATCH (d:Dataset)
    RETURN d.id AS id,
           d.filename AS filename,
           d.uploaded_at AS uploaded_at,
           d.rows_total AS rows_total,
           d.rows_loaded AS rows_loaded,
           d.rows_failed AS rows_failed,
           d.status AS status
    ORDER BY d.uploaded_at DESC
    """
    return _run_read(driver, database, cypher)


def _get_dataset_columns_and_samples(driver, database: str, job_id: str) -> tuple[list[str], list[dict]]:
    """Retrieves column names and a sample of rows for schema inspection."""
    cypher = """
    MATCH (d:Dataset {id: $job_id})-[:HAS_ROW]->(r:Row)
    RETURN keys(r) AS keys, r AS row
    LIMIT 100
    """
    records = _run_read(driver, database, cypher, {"job_id": job_id})
    if not records:
        return [], []

    columns = sorted(list({
        str(k) for rec in records for k in rec["keys"]
        if k and k not in ("row_index", "dataset_id", "None", "")
    }))
    samples = [rec["row"] for rec in records]
    return columns, samples


def _safe_str_expr(ident: str) -> str:
    """Cypher expression that safely converts any property (scalar or array) to string without TypeError."""
    return f"(CASE WHEN {ident} IS NULL THEN '' WHEN {ident} IS :: LIST<ANY> THEN reduce(s = '', x IN {ident} | s + (CASE WHEN s = '' THEN '' ELSE ', ' END) + toString(x)) ELSE toString({ident}) END)"


def _is_numeric(val) -> bool:
    if val is None or val == "":
        return False
    try:
        float(str(val).replace(",", "").replace("$", ""))
        return True
    except (ValueError, TypeError):
        return False


def _is_date_like(val) -> bool:
    if not val or not isinstance(val, str):
        return False
    val_str = val.strip()
    return bool(re.match(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?", val_str))


def get_dataset_profile(driver, database: str, job_id: str) -> dict:
    """Computes automated exploratory profile for a dataset with dynamic columns."""
    if job_id in _profile_cache:
        return _profile_cache[job_id]

    meta_cypher = """
    MATCH (d:Dataset {id: $job_id})
    RETURN d.id AS id, d.filename AS filename, d.uploaded_at AS uploaded_at,
           d.rows_total AS rows_total, d.rows_loaded AS rows_loaded,
           d.rows_failed AS rows_failed, d.status AS status
    """
    meta_res = _run_read(driver, database, meta_cypher, {"job_id": job_id})
    if not meta_res:
        return {"error": f"Dataset {job_id} not found", "columns": [], "recommendations": []}

    meta = meta_res[0]
    total_rows = meta.get("rows_loaded") or meta.get("rows_total") or 0

    columns, samples = _get_dataset_columns_and_samples(driver, database, job_id)
    if not columns or total_rows == 0:
        res = {
            "metadata": meta,
            "columns": [],
            "recommendations": [],
            "overview": {
                "total_rows": total_rows,
                "column_count": len(columns),
                "total_cells": total_rows * len(columns),
            },
        }
        return res

    column_profiles = []
    numeric_cols = []
    categorical_cols = []
    date_cols = []

    # Sampling strategy for datasets > 3000 rows to ensure sub-second response times
    is_sampled = total_rows > 3000
    sample_size = 2000 if is_sampled else total_rows
    sample_prefix = f"WITH r LIMIT {sample_size} " if is_sampled else ""

    for col in columns:
        q_col = _quote_ident(col)
        col_samples = [s.get(col) for s in samples if s.get(col) is not None and str(s.get(col)).strip() != ""]
        num_non_empty = len(col_samples)

        numeric_sample_count = sum(1 for v in col_samples if _is_numeric(v))
        date_sample_count = sum(1 for v in col_samples if _is_date_like(v))

        inferred_type = "string"
        if num_non_empty > 0 and (numeric_sample_count / num_non_empty) >= 0.75:
            inferred_type = "numeric"
            numeric_cols.append(col)
        elif num_non_empty > 0 and (date_sample_count / num_non_empty) >= 0.75:
            inferred_type = "date"
            date_cols.append(col)
        else:
            inferred_type = "categorical"
            categorical_cols.append(col)

        count_cypher = f"""
        MATCH (d:Dataset {{id: $job_id}})-[:HAS_ROW]->(r:Row)
        {sample_prefix}
        WITH r.{q_col} AS val
        RETURN count(CASE WHEN val IS NULL OR val = '' OR (val IS :: LIST<ANY> AND size(val) = 0) THEN 1 END) AS null_count,
               count(DISTINCT val) AS distinct_count
        """
        counts = _run_read(driver, database, count_cypher, {"job_id": job_id})[0]
        null_count = counts["null_count"]
        distinct_count = counts["distinct_count"]

        if is_sampled:
            null_count = int(round(null_count * (total_rows / sample_size)))
            distinct_count = min(total_rows, int(round(distinct_count * math.sqrt(total_rows / sample_size))))

        profile = {
            "name": col,
            "type": inferred_type,
            "total_count": total_rows,
            "null_count": null_count,
            "null_percentage": round((null_count / total_rows) * 100, 2) if total_rows else 0.0,
            "distinct_count": distinct_count,
            "statistics": None,
            "top_values": [],
        }

        # Numeric statistics
        if inferred_type == "numeric":
            stat_cypher = f"""
            MATCH (d:Dataset {{id: $job_id}})-[:HAS_ROW]->(r:Row)
            {sample_prefix}
            WHERE r.{q_col} IS NOT NULL AND toFloat(r.{q_col}) IS NOT NULL
            WITH toFloat(r.{q_col}) AS v
            RETURN min(v) AS min_val, max(v) AS max_val,
                   avg(v) AS avg_val, sum(v) AS sum_val,
                   stDev(v) AS std_dev
            """
            stats = _run_read(driver, database, stat_cypher, {"job_id": job_id})
            if stats and stats[0]["min_val"] is not None:
                st = stats[0]
                sum_val = st["sum_val"]
                if is_sampled and sum_val is not None:
                    sum_val = sum_val * (total_rows / sample_size)
                profile["statistics"] = {
                    "min": round(st["min_val"], 4) if st["min_val"] is not None else None,
                    "max": round(st["max_val"], 4) if st["max_val"] is not None else None,
                    "avg": round(st["avg_val"], 4) if st["avg_val"] is not None else None,
                    "sum": round(sum_val, 4) if sum_val is not None else None,
                    "std_dev": round(st["std_dev"], 4) if st["std_dev"] is not None else None,
                }

        # Top values for categoricals
        if inferred_type in ("categorical", "string") or (inferred_type == "numeric" and distinct_count <= 10):
            safe_str = _safe_str_expr(f"r.{q_col}")
            top_cypher = f"""
            MATCH (d:Dataset {{id: $job_id}})-[:HAS_ROW]->(r:Row)
            {sample_prefix}
            WHERE r.{q_col} IS NOT NULL
            WITH {safe_str} AS value, r
            WHERE value <> ''
            RETURN value, count(r) AS count
            ORDER BY count DESC
            LIMIT 10
            """
            top_res = _run_read(driver, database, top_cypher, {"job_id": job_id})
            profile["top_values"] = top_res

        column_profiles.append(profile)

    # Generate smart chart recommendations based on real discovered columns
    recommendations = []

    # Rec 1: Top categorical distribution
    if categorical_cols:
        cat = categorical_cols[0]
        prof = next((p for p in column_profiles if p["name"] == cat), None)
        if prof and prof["top_values"]:
            recommendations.append({
                "id": "rec_cat_distribution",
                "title": f"Distribution of {cat}",
                "type": "bar",
                "x_key": "value",
                "y_key": "count",
                "data": prof["top_values"],
                "description": f"Frequency distribution across top unique {cat} values.",
            })

    # Rec 2: Categorical + Numeric aggregation (Group-by sum/avg)
    if categorical_cols and numeric_cols:
        cat = categorical_cols[0]
        num = numeric_cols[0]
        q_cat = _quote_ident(cat)
        q_num = _quote_ident(num)
        safe_cat = _safe_str_expr(f"r.{q_cat}")

        agg_cypher = f"""
        MATCH (d:Dataset {{id: $job_id}})-[:HAS_ROW]->(r:Row)
        {sample_prefix}
        WHERE r.{q_cat} IS NOT NULL AND toFloat(r.{q_num}) IS NOT NULL
        WITH {safe_cat} AS category, toFloat(r.{q_num}) AS num_val
        WHERE category <> ''
        RETURN category,
               round(avg(num_val), 2) AS avg_value,
               round(sum(num_val), 2) AS sum_value
        ORDER BY avg_value DESC
        LIMIT 10
        """
        agg_res = _run_read(driver, database, agg_cypher, {"job_id": job_id})
        if agg_res:
            recommendations.append({
                "id": "rec_cat_num_avg",
                "title": f"Average {num} by {cat}",
                "type": "bar",
                "x_key": "category",
                "y_key": "avg_value",
                "data": agg_res,
                "description": f"Breakdown of average {num} across different {cat} groups.",
            })

    # Rec 3: Top-N records by primary numeric column
    if numeric_cols:
        num = numeric_cols[0]
        label_col = categorical_cols[0] if categorical_cols else "row_index"
        q_num = _quote_ident(num)
        q_lbl = _quote_ident(label_col)

        top_cypher = f"""
        MATCH (d:Dataset {{id: $job_id}})-[:HAS_ROW]->(r:Row)
        {sample_prefix}
        WHERE toFloat(r.{q_num}) IS NOT NULL
        RETURN toString(r.{q_lbl}) AS label, toFloat(r.{q_num}) AS value
        ORDER BY value DESC
        LIMIT 10
        """
        top_res = _run_read(driver, database, top_cypher, {"job_id": job_id})
        if top_res:
            recommendations.append({
                "id": "rec_topn_numeric",
                "title": f"Top 10 Records by {num}",
                "type": "bar",
                "x_key": "label",
                "y_key": "value",
                "data": top_res,
                "description": f"Highest values recorded for {num}.",
            })

    # Rec 4: Secondary categorical breakdown as Doughnut/Pie
    if len(categorical_cols) > 1:
        cat2 = categorical_cols[1]
        prof2 = next((p for p in column_profiles if p["name"] == cat2), None)
        if prof2 and prof2["top_values"]:
            recommendations.append({
                "id": "rec_cat2_doughnut",
                "title": f"{cat2} Proportions",
                "type": "doughnut",
                "x_key": "value",
                "y_key": "count",
                "data": prof2["top_values"][:8],
                "description": f"Proportional share of top {cat2} categories.",
            })

    result = {
        "metadata": meta,
        "overview": {
            "total_rows": total_rows,
            "column_count": len(columns),
            "numeric_columns": len(numeric_cols),
            "categorical_columns": len(categorical_cols),
            "date_columns": len(date_cols),
            "total_cells": total_rows * len(columns),
            "is_sampled": is_sampled,
            "sample_size": sample_size if is_sampled else total_rows,
        },
        "columns": column_profiles,
        "recommendations": recommendations,
    }
    if meta.get("status") == "complete":
        _profile_cache[job_id] = result
    return result


def get_data_quality(driver, database: str, job_id: str) -> dict:
    """Computes comprehensive data-quality metrics and statistical anomaly detection."""
    if job_id in _quality_cache:
        return _quality_cache[job_id]

    meta_cypher = """
    MATCH (d:Dataset {id: $job_id})
    RETURN d.id AS id, d.filename AS filename, d.rows_total AS rows_total,
           d.rows_loaded AS rows_loaded, d.rows_failed AS rows_failed, d.status AS status
    """
    meta_res = _run_read(driver, database, meta_cypher, {"job_id": job_id})
    if not meta_res:
        return {"error": f"Dataset {job_id} not found"}

    meta = meta_res[0]
    total_rows = meta.get("rows_loaded") or meta.get("rows_total") or 0
    failed_rows = meta.get("rows_failed") or 0

    columns, samples = _get_dataset_columns_and_samples(driver, database, job_id)
    if not columns or total_rows == 0:
        return {
            "health_score": 100,
            "total_rows": total_rows,
            "failed_rows": failed_rows,
            "missingness": [],
            "outliers": [],
            "duplicates": 0,
            "warnings": [],
        }

    is_sampled = total_rows > 3000
    sample_size = 2000 if is_sampled else total_rows
    sample_prefix = f"WITH r LIMIT {sample_size} " if is_sampled else ""

    missing_reports = []
    total_missing_cells = 0
    total_cells = total_rows * len(columns)
    warnings = []

    # Check missingness per column
    for col in columns:
        q_col = _quote_ident(col)
        m_cypher = f"""
        MATCH (d:Dataset {{id: $job_id}})-[:HAS_ROW]->(r:Row)
        {sample_prefix}
        WITH r.{q_col} AS val
        RETURN count(CASE WHEN val IS NULL OR val = '' OR (val IS :: LIST<ANY> AND size(val) = 0) THEN 1 END) AS null_count
        """
        null_count = _run_read(driver, database, m_cypher, {"job_id": job_id})[0]["null_count"]
        if is_sampled:
            null_count = int(round(null_count * (total_rows / sample_size)))
        pct = round((null_count / total_rows) * 100, 2) if total_rows else 0.0
        total_missing_cells += null_count
        missing_reports.append({
            "column": col,
            "missing_count": null_count,
            "missing_percentage": pct,
        })
        if pct > 20.0:
            warnings.append({
                "severity": "warning",
                "type": "high_missingness",
                "message": f"Column '{col}' is missing {pct}% of its values ({null_count}/{total_rows} rows).",
            })

    # Statistical outlier detection using Tukey's IQR rule on numeric columns
    numeric_cols = []
    for col in columns:
        col_samples = [s.get(col) for s in samples if s.get(col) is not None and str(s.get(col)).strip() != ""]
        if col_samples and sum(1 for v in col_samples if _is_numeric(v)) / len(col_samples) >= 0.75:
            numeric_cols.append(col)

    outlier_reports = []
    total_outlier_count = 0

    for col in numeric_cols:
        q_col = _quote_ident(col)
        # Compute Q1 (25th percentile) and Q3 (75th percentile)
        p_cypher = f"""
        MATCH (d:Dataset {{id: $job_id}})-[:HAS_ROW]->(r:Row)
        {sample_prefix}
        WHERE r.{q_col} IS NOT NULL AND toFloat(r.{q_col}) IS NOT NULL
        RETURN percentileCont(toFloat(r.{q_col}), 0.25) AS q1,
               percentileCont(toFloat(r.{q_col}), 0.75) AS q3
        """
        p_res = _run_read(driver, database, p_cypher, {"job_id": job_id})
        if not p_res or p_res[0]["q1"] is None or p_res[0]["q3"] is None:
            continue

        q1 = p_res[0]["q1"]
        q3 = p_res[0]["q3"]
        iqr = q3 - q1
        if iqr <= 0:
            continue

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outlier_cypher = f"""
        MATCH (d:Dataset {{id: $job_id}})-[:HAS_ROW]->(r:Row)
        {sample_prefix}
        WHERE r.{q_col} IS NOT NULL AND toFloat(r.{q_col}) IS NOT NULL
        WITH r, toFloat(r.{q_col}) AS val
        WHERE val < $lower OR val > $upper
        RETURN count(r) AS outlier_count,
               collect({{row_index: r.row_index, value: val}})[..5] AS sample_records
        """
        out_res = _run_read(driver, database, outlier_cypher, {
            "job_id": job_id,
            "lower": lower_bound,
            "upper": upper_bound,
        })
        if out_res and out_res[0]["outlier_count"] > 0:
            count = out_res[0]["outlier_count"]
            if is_sampled:
                count = int(round(count * (total_rows / sample_size)))
            total_outlier_count += count
            pct = round((count / total_rows) * 100, 2)
            outlier_reports.append({
                "column": col,
                "outlier_count": count,
                "outlier_percentage": pct,
                "q1": round(q1, 2),
                "q3": round(q3, 2),
                "iqr": round(iqr, 2),
                "lower_bound": round(lower_bound, 2),
                "upper_bound": round(upper_bound, 2),
                "sample_anomalies": out_res[0]["sample_records"],
                "rule": "Tukey's IQR 1.5x rule (values < Q1 - 1.5*IQR or > Q3 + 1.5*IQR)",
            })
            warnings.append({
                "severity": "info",
                "type": "statistical_outlier",
                "message": f"Column '{col}' has {count} statistical outliers ({pct}%) outside [{round(lower_bound, 1)}, {round(upper_bound, 1)}].",
            })

    # Duplicate rows check (matching all property values)
    dup_cypher = f"""
    MATCH (d:Dataset {{id: $job_id}})-[:HAS_ROW]->(r:Row)
    {sample_prefix}
    WITH [k IN keys(r) WHERE NOT k IN ['row_index', 'dataset_id'] | r[k]] AS signature, count(r) AS dup_count
    WHERE dup_count > 1
    RETURN sum(dup_count - 1) AS duplicate_rows
    """
    dup_res = _run_read(driver, database, dup_cypher, {"job_id": job_id})
    duplicate_rows = dup_res[0]["duplicate_rows"] or 0
    if is_sampled and duplicate_rows > 0:
        duplicate_rows = int(round(duplicate_rows * (total_rows / sample_size)))
    if duplicate_rows > 0:
        warnings.append({
            "severity": "warning",
            "type": "duplicate_rows",
            "message": f"Found {duplicate_rows} duplicate record(s) with identical values across all columns.",
        })

    missing_pct = (total_missing_cells / total_cells) * 100 if total_cells else 0
    missing_penalty = min(30.0, missing_pct * 1.5)

    failed_pct = (failed_rows / total_rows) * 100 if total_rows else 0
    failed_penalty = min(30.0, failed_pct * 3.0)

    outlier_pct = (total_outlier_count / total_rows) * 100 if total_rows else 0
    outlier_penalty = min(20.0, outlier_pct * 1.0)

    dup_pct = (duplicate_rows / total_rows) * 100 if total_rows else 0
    dup_penalty = min(20.0, dup_pct * 2.0)

    raw_score = 100.0 - (missing_penalty + failed_penalty + outlier_penalty + dup_penalty)
    health_score = max(0, min(100, round(raw_score, 1)))

    result = {
        "health_score": health_score,
        "total_rows": total_rows,
        "failed_rows": failed_rows,
        "total_columns": len(columns),
        "total_missing_cells": total_missing_cells,
        "missing_cell_percentage": round(missing_pct, 2),
        "missingness": missing_reports,
        "outliers": outlier_reports,
        "duplicate_rows": duplicate_rows,
        "warnings": warnings,
        "is_sampled": is_sampled,
        "scoring_breakdown": {
            "missing_penalty": round(missing_penalty, 1),
            "failed_penalty": round(failed_penalty, 1),
            "outlier_penalty": round(outlier_penalty, 1),
            "duplicate_penalty": round(dup_penalty, 1),
        },
    }
    if meta.get("status") == "complete":
        _quality_cache[job_id] = result
    return result


# ---------------------------------------------------------------------------
# Cross-Dataset Candidate Relationship Discovery & Graph Persistence
# ---------------------------------------------------------------------------

def _clean_token(s: str) -> str:
    s = s.lower().replace("-", "_").replace(" ", "_")
    for aff in ("_id", "id_", "_key", "key_", "_num", "_no", "_code", "_name"):
        if s.endswith(aff) and len(s) > len(aff):
            s = s[:-len(aff)]
        elif s.startswith(aff) and len(s) > len(aff):
            s = s[len(aff):]
    return s.strip("_")


def _compute_name_similarity(c1: str, c2: str) -> float:
    c1_low, c2_low = c1.lower(), c2.lower()
    if c1_low == c2_low:
        return 1.0
    t1, t2 = _clean_token(c1), _clean_token(c2)
    if t1 and t2 and t1 == t2:
        return 0.9
    if t1 and t2 and (t1 in t2 or t2 in t1):
        return 0.75
    return round(SequenceMatcher(None, c1_low, c2_low).ratio(), 3)


def discover_relationships(driver, database: str, dataset_ids: list[str] | None = None) -> list[dict]:
    """Scans datasets in Neo4j to infer candidate relationships between compatible columns.

    Evaluates:
    - Column name normalized similarity (fuzzy token / prefix / suffix match)
    - Distinct value overlap ratio between sample records
    - Distinctness & candidate-key cardinality (1:1, 1:N, N:M)
    - Persists candidates idempotently as (:DatasetRelation) nodes in Neo4j.
    """
    datasets = get_all_datasets(driver, database)
    if dataset_ids:
        datasets = [d for d in datasets if d["id"] in dataset_ids]

    # Constraint to guarantee idempotent uniqueness
    try:
        _run_write(driver, database, "CREATE CONSTRAINT dataset_rel_id IF NOT EXISTS FOR (r:DatasetRelation) REQUIRE r.id IS UNIQUE")
    except Exception as e:
        log.debug("Constraint on DatasetRelation already exists or failed: %s", e)

    discovered = []

    for i in range(len(datasets)):
        for j in range(i + 1, len(datasets)):
            d1 = datasets[i]
            d2 = datasets[j]
            if d1["id"] == d2["id"]:
                continue

            cols1, _ = _get_dataset_columns_and_samples(driver, database, d1["id"])
            cols2, _ = _get_dataset_columns_and_samples(driver, database, d2["id"])

            for c1 in cols1:
                for c2 in cols2:
                    name_sim = _compute_name_similarity(c1, c2)
                    if name_sim < 0.4:
                        continue

                    q_c1 = _quote_ident(c1)
                    q_c2 = _quote_ident(c2)

                    # Distinct value overlap query with early limit on traversal
                    overlap_cypher = f"""
                    MATCH (d1:Dataset {{id: $id1}})-[:HAS_ROW]->(r1:Row)
                    WITH r1 LIMIT 500
                    WHERE r1.{q_c1} IS NOT NULL AND toString(r1.{q_c1}) <> ''
                    WITH collect(DISTINCT toString(r1.{q_c1}))[..100] AS s1
                    MATCH (d2:Dataset {{id: $id2}})-[:HAS_ROW]->(r2:Row)
                    WITH s1, r2 LIMIT 500
                    WHERE r2.{q_c2} IS NOT NULL AND toString(r2.{q_c2}) <> ''
                    WITH s1, collect(DISTINCT toString(r2.{q_c2}))[..100] AS s2
                    WITH s1, s2, [x IN s1 WHERE x IN s2] AS overlap
                    RETURN size(s1) AS count1, size(s2) AS count2, size(overlap) AS overlap_count, overlap[..10] AS sample_matches
                    """
                    res = _run_read(driver, database, overlap_cypher, {"id1": d1["id"], "id2": d2["id"]})
                    if not res:
                        continue

                    row = res[0]
                    count1 = row["count1"] or 0
                    count2 = row["count2"] or 0
                    overlap_count = row["overlap_count"] or 0
                    sample_matches = row["sample_matches"] or []

                    if overlap_count > 0 and min(count1, count2) > 0:
                        coverage = overlap_count / min(count1, count2)
                        confidence = round(0.4 * name_sim + 0.6 * coverage, 2)
                        if confidence < 0.35:
                            continue

                        cardinality = "1:1" if (count1 == count2 and overlap_count == count1) else "1:N"

                        # Canonical directional sort to prevent duplicate reversed edges
                        canon = sorted([(d1["id"], c1, d1["filename"]), (d2["id"], c2, d2["filename"])])
                        s_id, s_col, s_fn = canon[0]
                        t_id, t_col, t_fn = canon[1]
                        rel_id = hashlib.sha256(f"{s_id}:{s_col}->{t_id}:{t_col}".encode()).hexdigest()[:16]

                        upsert_cypher = """
                        MERGE (r:DatasetRelation {id: $rel_id})
                        ON CREATE SET
                            r.source_id = $s_id,
                            r.source_filename = $s_fn,
                            r.source_col = $s_col,
                            r.target_id = $t_id,
                            r.target_filename = $t_fn,
                            r.target_col = $t_col,
                            r.confidence = $confidence,
                            r.overlap_count = $overlap_count,
                            r.sample_count_source = $count1,
                            r.sample_count_target = $count2,
                            r.coverage = $coverage,
                            r.cardinality = $cardinality,
                            r.sample_matches = $sample_matches,
                            r.status = 'candidate',
                            r.created_at = datetime()
                        ON MATCH SET
                            r.confidence = $confidence,
                            r.overlap_count = $overlap_count,
                            r.sample_count_source = $count1,
                            r.sample_count_target = $count2,
                            r.coverage = $coverage,
                            r.sample_matches = $sample_matches,
                            r.updated_at = datetime()
                        RETURN r.id AS id, r.source_id AS source_id, r.source_filename AS source_filename,
                               r.source_col AS source_col, r.target_id AS target_id, r.target_filename AS target_filename,
                               r.target_col AS target_col, r.confidence AS confidence, r.overlap_count AS overlap_count,
                               r.cardinality AS cardinality, r.status AS status, r.sample_matches AS sample_matches,
                               r.coverage AS coverage
                        """
                        rel_res = _run_write(driver, database, upsert_cypher, {
                            "rel_id": rel_id,
                            "s_id": s_id,
                            "s_fn": s_fn,
                            "s_col": s_col,
                            "t_id": t_id,
                            "t_fn": t_fn,
                            "t_col": t_col,
                            "confidence": confidence,
                            "overlap_count": overlap_count,
                            "count1": count1,
                            "count2": count2,
                            "coverage": round(coverage, 3),
                            "cardinality": cardinality,
                            "sample_matches": sample_matches,
                        })
                        if rel_res:
                            discovered.append(rel_res[0])
    return discovered


def get_relationships(driver, database: str, status_filter: str | None = None) -> list[dict]:
    """Retrieves all persisted dataset relationships with optional status filtering."""
    where_clause = ""
    params = {}
    if status_filter and status_filter.lower() != "all":
        where_clause = "WHERE r.status = $status"
        params["status"] = status_filter.lower()

    cypher = f"""
    MATCH (r:DatasetRelation)
    {where_clause}
    RETURN r.id AS id,
           r.source_id AS source_id,
           r.source_filename AS source_filename,
           r.source_col AS source_col,
           r.target_id AS target_id,
           r.target_filename AS target_filename,
           r.target_col AS target_col,
           r.confidence AS confidence,
           r.overlap_count AS overlap_count,
           r.sample_count_source AS sample_count_source,
           r.sample_count_target AS sample_count_target,
           r.coverage AS coverage,
           r.cardinality AS cardinality,
           r.status AS status,
           r.sample_matches AS sample_matches
    ORDER BY r.confidence DESC, r.overlap_count DESC
    """
    return _run_read(driver, database, cypher, params)


def set_relationship_status(driver, database: str, rel_id: str, new_status: str) -> dict:
    """Updates a relationship status to 'confirmed', 'rejected', or 'candidate'."""
    if new_status not in ("candidate", "confirmed", "rejected"):
        raise ValueError(f"Invalid status: {new_status}")
    cypher = """
    MATCH (r:DatasetRelation {id: $rel_id})
    SET r.status = $new_status, r.updated_at = datetime()
    RETURN r.id AS id, r.status AS status, r.source_col AS source_col, r.target_col AS target_col,
           r.source_filename AS source_filename, r.target_filename AS target_filename
    """
    res = _run_write(driver, database, cypher, {"rel_id": rel_id, "new_status": new_status})
    if not res:
        return {"error": f"Relationship {rel_id} not found"}
    return res[0]


def create_manual_relationship(driver, database: str, source_id: str, source_col: str, target_id: str, target_col: str) -> dict:
    """Explicitly defines and confirms a relationship between two columns across datasets."""
    datasets = {d["id"]: d.get("filename", d["id"][:8]) for d in get_all_datasets(driver, database)}
    s_fn = datasets.get(source_id, source_id[:8])
    t_fn = datasets.get(target_id, target_id[:8])

    q_scol = _quote_ident(source_col)
    q_tcol = _quote_ident(target_col)

    overlap_cypher = f"""
    MATCH (d1:Dataset {{id: $id1}})-[:HAS_ROW]->(r1:Row)
    WHERE r1.{q_scol} IS NOT NULL AND toString(r1.{q_scol}) <> ''
    WITH collect(DISTINCT toString(r1.{q_scol}))[..200] AS s1
    MATCH (d2:Dataset {{id: $id2}})-[:HAS_ROW]->(r2:Row)
    WHERE r2.{q_tcol} IS NOT NULL AND toString(r2.{q_tcol}) <> ''
    WITH s1, collect(DISTINCT toString(r2.{q_tcol}))[..200] AS s2
    WITH s1, s2, [x IN s1 WHERE x IN s2] AS overlap
    RETURN size(s1) AS count1, size(s2) AS count2, size(overlap) AS overlap_count, overlap[..10] AS sample_matches
    """
    res = _run_read(driver, database, overlap_cypher, {"id1": source_id, "id2": target_id})
    count1 = res[0]["count1"] if res else 0
    count2 = res[0]["count2"] if res else 0
    overlap_count = res[0]["overlap_count"] if res else 0
    sample_matches = res[0]["sample_matches"] if res else []
    coverage = round(overlap_count / min(count1, count2), 3) if min(count1, count2) > 0 else 0.0

    canon = sorted([(source_id, source_col, s_fn), (target_id, target_col, t_fn)])
    s_id, s_c, s_f = canon[0]
    t_id, t_c, t_f = canon[1]
    rel_id = hashlib.sha256(f"{s_id}:{s_c}->{t_id}:{t_c}".encode()).hexdigest()[:16]

    cypher = """
    MERGE (r:DatasetRelation {id: $rel_id})
    SET r.source_id = $s_id,
        r.source_filename = $s_fn,
        r.source_col = $s_col,
        r.target_id = $t_id,
        r.target_filename = $t_fn,
        r.target_col = $t_col,
        r.confidence = 1.0,
        r.overlap_count = $overlap_count,
        r.sample_count_source = $count1,
        r.sample_count_target = $count2,
        r.coverage = $coverage,
        r.cardinality = '1:N',
        r.sample_matches = $sample_matches,
        r.status = 'confirmed',
        r.updated_at = datetime()
    RETURN r.id AS id, r.status AS status, r.confidence AS confidence, r.source_col AS source_col,
           r.target_col AS target_col, r.source_filename AS source_filename, r.target_filename AS target_filename
    """
    res = _run_write(driver, database, cypher, {
        "rel_id": rel_id,
        "s_id": s_id,
        "s_fn": s_f,
        "s_col": s_c,
        "t_id": t_id,
        "t_fn": t_f,
        "t_col": t_c,
        "overlap_count": overlap_count,
        "count1": count1,
        "count2": count2,
        "coverage": coverage,
        "sample_matches": sample_matches,
    })
    return res[0] if res else {"error": "Failed to create relation"}


def get_relationship_matches(driver, database: str, rel_id: str, limit: int = 25) -> dict:
    """Returns sample joined records and join statistics for a given relationship."""
    rels = _run_read(driver, database, "MATCH (r:DatasetRelation {id: $rel_id}) RETURN r", {"rel_id": rel_id})
    if not rels:
        return {"error": f"Relationship {rel_id} not found"}
    rel = dict(rels[0]["r"])

    s_id = rel["source_id"]
    t_id = rel["target_id"]
    s_col = rel["source_col"]
    t_col = rel["target_col"]
    q_scol = _quote_ident(s_col)
    q_tcol = _quote_ident(t_col)

    match_cypher = f"""
    MATCH (d1:Dataset {{id: $s_id}})-[:HAS_ROW]->(r1:Row),
          (d2:Dataset {{id: $t_id}})-[:HAS_ROW]->(r2:Row)
    WHERE r1.{q_scol} IS NOT NULL AND toString(r1.{q_scol}) = toString(r2.{q_tcol})
    RETURN toString(r1.{q_scol}) AS join_key,
           properties(r1) AS source_row,
           properties(r2) AS target_row
    LIMIT $limit
    """
    matches = _run_read(driver, database, match_cypher, {"s_id": s_id, "t_id": t_id, "limit": limit})

    for m in matches:
        for k in ("row_index", "dataset_id"):
            m["source_row"].pop(k, None)
            m["target_row"].pop(k, None)

    count_cypher = f"""
    MATCH (d1:Dataset {{id: $s_id}})-[:HAS_ROW]->(r1:Row),
          (d2:Dataset {{id: $t_id}})-[:HAS_ROW]->(r2:Row)
    WHERE r1.{q_scol} IS NOT NULL AND toString(r1.{q_scol}) = toString(r2.{q_tcol})
    RETURN count(r1) AS total_joined_rows
    """
    count_res = _run_read(driver, database, count_cypher, {"s_id": s_id, "t_id": t_id})
    total_joined_rows = count_res[0]["total_joined_rows"] if count_res else 0

    return {
        "relationship": rel,
        "total_joined_rows": total_joined_rows,
        "matches": matches,
    }


def get_workspace_graph(driver, database: str) -> dict:
    """Returns the full spatial graph model: dataset nodes, column nodes, candidate and confirmed edges."""
    datasets = get_all_datasets(driver, database)
    relationships = get_relationships(driver, database)

    nodes = []
    total_rows = 0

    for d in datasets:
        d_id = d["id"]
        rows = d.get("rows_loaded") or d.get("rows_total") or 0
        total_rows += rows
        cols, _ = _get_dataset_columns_and_samples(driver, database, d_id)

        nodes.append({
            "id": d_id,
            "type": "dataset",
            "label": d.get("filename") or d_id[:8],
            "filename": d.get("filename"),
            "uploaded_at": d.get("uploaded_at"),
            "rows_loaded": rows,
            "status": d.get("status"),
            "columns": cols,
        })

        for c in cols:
            nodes.append({
                "id": f"{d_id}:{c}",
                "type": "column",
                "dataset_id": d_id,
                "dataset_name": d.get("filename"),
                "label": c,
                "name": c,
            })

    edges = []
    candidate_count = 0
    confirmed_count = 0
    rejected_count = 0

    for r in relationships:
        status = r.get("status", "candidate")
        if status == "candidate":
            candidate_count += 1
        elif status == "confirmed":
            confirmed_count += 1
        elif status == "rejected":
            rejected_count += 1

        edges.append({
            "id": r["id"],
            "source": r["source_id"],
            "target": r["target_id"],
            "source_col": r["source_col"],
            "target_col": r["target_col"],
            "source_col_id": f"{r['source_id']}:{r['source_col']}",
            "target_col_id": f"{r['target_id']}:{r['target_col']}",
            "source_filename": r["source_filename"],
            "target_filename": r["target_filename"],
            "confidence": r["confidence"],
            "overlap_count": r["overlap_count"],
            "coverage": r.get("coverage"),
            "cardinality": r.get("cardinality", "1:N"),
            "status": status,
            "sample_matches": r.get("sample_matches", []),
        })

    return {
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "total_datasets": len(datasets),
            "total_rows": total_rows,
            "total_columns": sum(len(n.get("columns", [])) for n in nodes if n["type"] == "dataset"),
            "candidate_relationships": candidate_count,
            "confirmed_relationships": confirmed_count,
            "rejected_relationships": rejected_count,
        }
    }
