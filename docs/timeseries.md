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
- **Continuous aggregation** — Incrementally maintained materialized views with watermarks, multi-tier chaining, and out-of-order handling. No full re-scan on refresh.
- **Retention policies** — Automatic expiry of old data by time partition.
- **Approximate aggregation** — HyperLogLog (cardinality), t-digest (percentiles), SpaceSaving (top-K), Count-Min Sketch (frequency). All mergeable across shards and usable in continuous aggregation.
- **Block-level skip** — Sparse primary index with min/max statistics per block. Queries skip irrelevant time ranges without decompression.
- **PromQL** — Full Prometheus query engine (Tier 1+2+3 functions) at `/obsv/api`. Point Grafana at NodeDB directly.
- **Prometheus remote write/read** — Use NodeDB as a long-term storage backend for Prometheus.
- **OTLP ingest** — OpenTelemetry metrics, traces, and logs via HTTP (port 4318) and gRPC (port 4317).

## Examples

```sql
-- Unified DDL syntax (preferred)
-- TIME_KEY designates the primary time column; enables partition-by-time and block-level skip
CREATE COLLECTION cpu_metrics TYPE COLUMNAR (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    region VARCHAR,
    cpu_usage FLOAT,
    mem_usage FLOAT
) WITH profile = 'timeseries', partition_by = '1d', retention = '90d';

-- CREATE TIMESERIES is a convenience alias for the above form
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
CREATE MATERIALIZED VIEW cpu_hourly AS
SELECT time_bucket('1 hour', ts) AS hour,
       host,
       AVG(cpu_usage) AS avg_cpu,
       ts_percentile(cpu_usage, 0.99) AS p99_cpu
FROM cpu_metrics
GROUP BY hour, host
WITH (refresh = 'incremental');

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

Continuous aggregates are incrementally maintained views over a timeseries collection. They refresh automatically as new data arrives — no full re-scan on each refresh.

```sql
-- Create a continuous aggregate
CREATE CONTINUOUS AGGREGATE cpu_hourly
ON cpu_metrics
AS
    SELECT time_bucket('1 hour', ts) AS hour,
           host,
           AVG(cpu_usage) AS avg_cpu,
           ts_percentile(cpu_usage, 0.99) AS p99_cpu
    FROM cpu_metrics
    GROUP BY hour, host
WITH (refresh_interval = '1m');

-- Manually trigger a refresh
REFRESH CONTINUOUS AGGREGATE cpu_hourly;

-- List all continuous aggregates
SHOW CONTINUOUS AGGREGATES;

-- Drop
DROP CONTINUOUS AGGREGATE cpu_hourly;
```

The `refresh_interval` controls how often the engine checks for new data since the last watermark. Out-of-order data within the watermark window is handled automatically.

Continuous aggregate refreshes can also be triggered from the cron scheduler:

```sql
CREATE SCHEDULE refresh_cpu_hourly
CRON '*/5 * * * *'
AS BEGIN
    REFRESH CONTINUOUS AGGREGATE cpu_hourly;
END;
```

This is useful when you want deterministic refresh timing rather than relying on the `refresh_interval` background timer. See [Real-Time Features](real-time.md#cron-scheduler) for the full cron scheduler reference.

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

## Related

- [Columnar](columnar.md) — Timeseries is a columnar profile
- [Spatial](spatial.md) — Combine time and location (fleet tracking, IoT)
- [Architecture](architecture.md) — How storage tiers handle hot/warm/cold data

[Back to docs](README.md)
