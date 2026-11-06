# Timeseries

Timeseries is a **columnar profile**. It extends the [Columnar engine](columnar.md) with retention policies, continuous aggregation, ILP ingest, and 12 dedicated SQL functions. It does not have a separate storage layer — timeseries collections store data in the same `columnar_memtables` as plain columnar collections, with a `TIME_KEY` column driving partition-by-time and block-level skip.

## When to Use

- Application and infrastructure metrics
- IoT sensor telemetry
- Financial market data
- Log analytics
- Any time-ordered append-mostly workload

## Key Features

- **Cascading compression** — ALP + FastLanes + FSST + Pcodec + rANS + LZ4. Achieves 20-40x compression on typical telemetry, compared to 3-5x from single-pass approaches.
- **ILP ingest** — InfluxDB Line Protocol for high-throughput metric ingestion with adaptive batching and per-series core routing.
- **Continuous aggregation** — Incrementally maintained per-bucket state, folded from flushed partitions. No full re-scan on refresh.
- **Retention policies** — Automatic expiry of old data by time partition. A pass drops the partitions older than `decided time - max age`.
  - A partition with an interval expires once its whole interval ended at or before the cutoff. Its rows leave at one moment on every replica.
  - A partition with no interval expires once its newest row is older than the cutoff.
  - A bitemporal collection expires a partition once its newest write is older than the cutoff.
  - On a bitemporal collection, a row past the cutoff can leave one replica while another holds it beside a newer row.
  - Every row newer than the cutoff stays on every replica.
  - The held row leaves at the first pass past its partition's newest write.
- **Approximate aggregation** — HyperLogLog (cardinality), t-digest (percentiles), SpaceSaving (top-K), Count-Min Sketch (frequency). All mergeable across shards and usable in continuous aggregation.
- **Block-level skip** — Sparse primary index with min/max statistics per block. Queries skip irrelevant time ranges without decompression.
- **PromQL** — Full Prometheus query engine (Tier 1+2+3 functions) at `/obsv/api`. Point Grafana at NodeDB directly.
- **Prometheus remote write/read** — Use NodeDB as a long-term storage backend for Prometheus.
- **OTLP ingest** — OpenTelemetry metrics, traces, and logs via HTTP (port 4318) and gRPC (port 4317).

## Examples

```sql
-- Unified DDL syntax (preferred)
-- TIME_KEY designates the primary time column; enables partition-by-time and block-level skip
CREATE COLLECTION cpu_metrics (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    region VARCHAR,
    cpu_usage FLOAT,
    mem_usage FLOAT
) WITH (engine='timeseries', partition_by='1d', retention='90d');

-- CREATE TIMESERIES is a convenience alias equivalent to engine='timeseries'
CREATE TIMESERIES cpu_metrics;

-- Insert metrics
INSERT INTO cpu_metrics (host, region, cpu_usage, mem_usage, ts)
VALUES ('web-01', 'us-east', 72.5, 84.2, now());

-- Time-bucketed aggregation
SELECT time_bucket('5 minutes', ts) AS bucket,
       host,
       AVG(cpu_usage) AS avg_cpu,
       MAX(mem_usage) AS max_mem
FROM cpu_metrics
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY bucket, host
ORDER BY bucket DESC;

-- Continuous aggregation (incremental, no full re-scan)
CREATE CONTINUOUS AGGREGATE cpu_hourly ON cpu_metrics
BUCKET '1h'
AGGREGATE avg(cpu_usage) AS avg_cpu, ts_percentile(cpu_usage, 0.99) AS p99_cpu
GROUP BY host
WITH (refresh_interval = '1m');

-- Approximate aggregation
SELECT approx_count_distinct(host) AS unique_hosts,
       approx_percentile(cpu_usage, 0.95) AS p95_cpu,
       approx_topk(host, 5) AS busiest_hosts
FROM cpu_metrics
WHERE ts > now() - INTERVAL '24 hours';
```

## Timeseries SQL Functions

| Function               | What it does                              |
| ---------------------- | ----------------------------------------- |
| `ts_rate`              | Per-second rate of change                 |
| `ts_delta`             | Difference between consecutive values     |
| `ts_moving_avg`        | Moving average over a window              |
| `ts_ema`               | Exponential moving average                |
| `ts_interpolate`       | Fill gaps with interpolated values        |
| `ts_percentile`        | Percentile calculation                    |
| `ts_correlate`         | Correlation between two series            |
| `ts_lag`               | Previous value in a series                |
| `ts_lead`              | Next value in a series                    |
| `ts_rank`              | Rank within a series                      |
| `ts_stddev`            | Standard deviation                        |
| `ts_derivative`        | Rate of change (alias for ts_rate)        |
| `ts_zscore`            | Z-score anomaly detection                 |
| `ts_bollinger_upper`   | Bollinger Band upper bound                |
| `ts_bollinger_lower`   | Bollinger Band lower bound                |
| `ts_bollinger_mid`     | Bollinger Band midline (moving average)   |
| `ts_bollinger_width`   | Bollinger Band width (volatility measure) |
| `ts_moving_percentile` | Rolling percentile over a window          |

## time_bucket() UDF

`time_bucket(interval, ts)` truncates a timestamp to the nearest interval boundary. It is registered as a DataFusion ScalarUDF and available on all SQL-capable protocols.

```sql
-- Truncate to 5-minute buckets
SELECT time_bucket('5 minutes', ts) AS bucket, AVG(cpu_usage)
FROM cpu_metrics
GROUP BY bucket;

-- Supported interval literals: '1s', '5m', '15m', '1h', '6h', '1d', '1w'
-- ISO 8601 duration format also accepted: 'PT5M', 'PT1H', 'P1D'
```

## Continuous Aggregation DDL

A continuous aggregate folds new rows of a timeseries collection into per-bucket state. It never re-scans the source.

```sql
-- Create a continuous aggregate
CREATE CONTINUOUS AGGREGATE cpu_hourly ON cpu_metrics
BUCKET '1h'
AGGREGATE avg(cpu_usage) AS avg_cpu, ts_percentile(cpu_usage, 0.99) AS p99_cpu
GROUP BY host
WITH (refresh_interval = '1m', retention = '365d');

-- Manually trigger a refresh
REFRESH CONTINUOUS AGGREGATE cpu_hourly;

-- List all continuous aggregates
SHOW CONTINUOUS AGGREGATES;

-- Drop
DROP CONTINUOUS AGGREGATE cpu_hourly;
```

`BUCKET` sets the bucket width. Do not use `time_bucket(...)` in the definition.

| Clause | Meaning |
| ------ | ------- |
| `AGGREGATE f(col) [AS alias], ...` | Outputs. Functions: `count`, `sum`, `avg`, `min`, `max`, `first`, `last`, `count_distinct`, `approx_count_distinct`, `percentile(col, q)` (also `ts_percentile`, `approx_percentile`), `topk(col, k)` (also `approx_topk`). `q` is in `[0, 1]`. `k` is at least 1. Use `count(*)` to count rows. |
| `GROUP BY col, ...` | Optional. Adds group columns to each bucket. |
| `refresh_policy` | `on_flush` (default), `on_seal`, `manual`, or an interval such as `'5m'` (folds periodically). |
| `refresh_interval` | Interval such as `'1m'`. Folds periodically on the maintenance tick. |
| `retention` | Interval after which the aggregate drops old buckets. |

Refresh behavior:

- `on_flush` and `on_seal` aggregates fold each flushed partition once.
- `refresh_interval` makes the aggregate periodic. It folds on the maintenance tick at that interval.
- `manual` aggregates fold only on `REFRESH CONTINUOUS AGGREGATE`. A read serves the last refresh.

Aggregate state survives restart. It keeps its history after raw retention removes the source rows.

`SELECT * FROM cpu_hourly` returns one row per bucket and group. Columns are `bucket`, the `GROUP BY` columns, then the outputs. The aggregate is read-only.

Continuous aggregate refreshes can also be triggered from the cron scheduler:

```sql
CREATE SCHEDULE refresh_cpu_hourly
CRON '*/5 * * * *'
AS BEGIN
    REFRESH CONTINUOUS AGGREGATE cpu_hourly;
END;
```

This is useful when you want deterministic refresh timing rather than relying on the `refresh_interval` maintenance tick or the flush-time fold. See [Real-Time Features](real-time.md#cron-scheduler) for the full cron scheduler reference.

## ILP Ingest (InfluxDB Line Protocol)

NodeDB accepts metrics via the InfluxDB Line Protocol over TCP. ILP is **disabled by default** — enable it by setting a port:

```toml
# nodedb.toml
[server.ports]
ilp = 8086
```

Or via environment variable: `NODEDB_PORT_ILP=8086`

Once enabled, any ILP-compatible client (telegraf, vector, InfluxDB client libraries) can push metrics directly:

```bash
# Raw ILP over TCP
echo "cpu,host=web-01,region=us-east usage=72.5,mem=84.2 1609459200000000000" | nc localhost 8086
```

```toml
# telegraf.conf
[[outputs.socket_writer]]
address = "tcp://localhost:8086"
data_format = "influx"
```

NodeDB uses adaptive batching (auto-tuned by ingest rate) and per-series core routing to eliminate cross-core contention. No configuration needed — it self-tunes.

## Grafana Integration

NodeDB works as a native Grafana data source:

1. Add a Prometheus data source in Grafana
2. Point it at `http://nodedb-host:6480/obsv/api`
3. Use PromQL queries directly in Grafana dashboards

NodeDB also supports Grafana's health check, metadata, annotations, and label discovery endpoints.

## Bitemporal Support

Timeseries data can be backdated or corrected using bitemporal queries — distinguishing between system time (when the metric was recorded) and valid time (when it represents).

```sql
-- Query metrics as recorded yesterday (system time)
SELECT host, AVG(cpu_usage) FROM cpu_metrics
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY host
AS OF SYSTEM TIME '2026-06-06T00:00:00Z';

-- Correct historical metric (recorded with wrong timestamp, corrected later)
INSERT INTO cpu_metrics (ts, host, cpu_usage) VALUES
  ('2026-04-01T10:00:00Z', 'web-01', 65.0);  -- backdated

-- Query what was valid at the original time
SELECT * FROM cpu_metrics
WHERE ts BETWEEN '2026-04-01' AND '2026-04-02'
AS OF VALID TIME 1712000000000;  -- April 1st
```

This is useful for metric corrections, forecast updates, and audit trails. See [Bitemporal](bitemporal.md) for detailed examples.

## Related

- [Bitemporal](bitemporal.md) — Cross-engine temporal queries and audit trails
- [Columnar](columnar.md) — Timeseries is a columnar profile
- [Spatial](spatial.md) — Combine time and location (fleet tracking, IoT)
- [Architecture](architecture.md) — How storage tiers handle hot/warm/cold data

[Back to docs](README.md)
