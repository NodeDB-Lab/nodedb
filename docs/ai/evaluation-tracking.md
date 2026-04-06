# Evaluation Tracking

Track ML experiment metrics, compare retrievers, and detect model drift — all in SQL. NodeDB's columnar engine handles analytical queries over experiment data, and the timeseries profile tracks performance over time with automatic retention and continuous aggregation.

## Experiment Metrics Storage

Store per-query evaluation metrics in a columnar collection. Each row is one evaluation data point: a query, a retriever, and its measured metrics.

```sql
CREATE COLLECTION eval_metrics (
    ts          TIMESTAMP TIME_KEY,
    experiment  VARCHAR,
    retriever   VARCHAR,
    query_id    VARCHAR,
    precision_5 FLOAT,
    recall_10   FLOAT,
    mrr         FLOAT,
    ndcg_10     FLOAT,
    latency_ms  FLOAT,
    doc_count   INT
) WITH (storage = 'columnar', profile = 'timeseries', partition_by = '1d', retention = '365d');

-- Log evaluation results (your eval harness inserts these)
INSERT INTO eval_metrics VALUES (
    NOW(), 'exp-042', 'hybrid-rrf-v2', 'q-001',
    0.82, 0.91, 0.78, 0.85, 12.3, 1500000
);
```

## Compare Retrievers

Use standard SQL aggregation to compare retriever configurations side by side.

```sql
-- Compare average metrics across retrievers in one experiment
SELECT
    retriever,
    COUNT(*) AS queries,
    AVG(precision_5) AS avg_p5,
    AVG(recall_10) AS avg_r10,
    AVG(mrr) AS avg_mrr,
    AVG(ndcg_10) AS avg_ndcg,
    AVG(latency_ms) AS avg_latency_ms
FROM eval_metrics
WHERE experiment = 'exp-042'
GROUP BY retriever
ORDER BY avg_mrr DESC;

-- Results:
-- retriever          | queries | avg_p5 | avg_r10 | avg_mrr | avg_ndcg | avg_latency_ms
-- hybrid-rrf-v2      |    500  |  0.82  |  0.91   |  0.78   |  0.85    |  12.3
-- vector-only        |    500  |  0.74  |  0.85   |  0.71   |  0.79    |   8.1
-- bm25-only          |    500  |  0.68  |  0.79   |  0.65   |  0.72    |   5.2

-- Statistical comparison: percentile distribution of MRR per retriever
SELECT
    retriever,
    AVG(mrr) AS mean_mrr,
    MIN(mrr) AS min_mrr,
    MAX(mrr) AS max_mrr
FROM eval_metrics
WHERE experiment = 'exp-042'
GROUP BY retriever;

-- Per-query comparison: find queries where one retriever beats another
SELECT
    a.query_id,
    a.mrr AS rrf_mrr,
    b.mrr AS vector_mrr,
    a.mrr - b.mrr AS mrr_delta
FROM eval_metrics a
JOIN eval_metrics b ON a.query_id = b.query_id
WHERE a.experiment = 'exp-042'
  AND a.retriever = 'hybrid-rrf-v2'
  AND b.retriever = 'vector-only'
  AND a.mrr - b.mrr < -0.1
ORDER BY mrr_delta ASC
LIMIT 20;
-- Shows queries where hybrid is significantly worse — investigate these for tuning.
```

## Time-Series Model Performance Tracking (Drift Detection)

Track production model metrics over time to detect performance drift. Use continuous aggregation for real-time dashboards.

```sql
-- Production inference metrics (timeseries)
CREATE COLLECTION model_perf (
    ts          TIMESTAMP TIME_KEY,
    model_name  VARCHAR,
    endpoint    VARCHAR,
    latency_ms  FLOAT,
    token_count INT,
    score       FLOAT,
    error       BOOLEAN
) WITH (storage = 'columnar', profile = 'timeseries', partition_by = '1h', retention = '90d');

-- Continuous aggregation: 5-minute rollups for dashboards
CREATE CONTINUOUS AGGREGATE model_perf_5m
ON model_perf
AS
    SELECT time_bucket('5 minutes', ts) AS bucket,
           model_name,
           endpoint,
           AVG(latency_ms) AS avg_latency,
           AVG(score) AS avg_score,
           SUM(CASE WHEN error THEN 1 ELSE 0 END) AS error_count,
           COUNT(*) AS request_count
    FROM model_perf
    GROUP BY bucket, model_name, endpoint;

-- Dashboard query: latency trend for the last 24 hours
SELECT ts, model_name, avg_latency, avg_score, error_count, request_count
FROM model_perf_5m
WHERE model_name = 'retriever-v3'
  AND ts > NOW() - INTERVAL '24 hours'
ORDER BY ts;

-- Drift detection: compare this week's average to last week's
SELECT
    model_name,
    AVG(CASE WHEN ts > NOW() - INTERVAL '7 days' THEN score END) AS this_week_score,
    AVG(CASE WHEN ts BETWEEN NOW() - INTERVAL '14 days'
                       AND NOW() - INTERVAL '7 days' THEN score END) AS last_week_score
FROM model_perf
WHERE ts > NOW() - INTERVAL '14 days'
GROUP BY model_name;
-- Alert if this_week_score < last_week_score * 0.95 (5% degradation threshold)
```

## Approximate Aggregation for Large-Scale Evaluation

For datasets with billions of evaluation points, use approximate aggregation functions that run in constant memory.

```sql
-- Approximate distinct query count (HyperLogLog)
SELECT
    retriever,
    APPROX_COUNT_DISTINCT(query_id) AS unique_queries
FROM eval_metrics
WHERE experiment = 'exp-042'
GROUP BY retriever;

-- Latency percentiles (t-digest)
SELECT
    model_name,
    APPROX_PERCENTILE(latency_ms, 0.50) AS p50,
    APPROX_PERCENTILE(latency_ms, 0.95) AS p95,
    APPROX_PERCENTILE(latency_ms, 0.99) AS p99
FROM model_perf
WHERE ts > NOW() - INTERVAL '1 hour'
GROUP BY model_name;

-- Top-K most frequent error endpoints (SpaceSaving)
SELECT
    endpoint,
    APPROX_TOP_K(endpoint, 10) AS top_errors
FROM model_perf
WHERE error = true AND ts > NOW() - INTERVAL '24 hours';
```

These approximate functions are mergeable across shards in distributed mode — the same query works on single-node and clustered deployments.
