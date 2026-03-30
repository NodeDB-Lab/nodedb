# Architecture

NodeDB splits work between two runtimes connected by a lock-free bridge. This is the core design decision — it's what makes the database fast on modern hardware.

## Hybrid Execution Model

```
┌───────────────────────────────────────────┐
│           Control Plane (Tokio)           │
│  SQL parsing, query planning, connections │
│           Send + Sync, async              │
└─────────────────┬─────────────────────────┘
                  │ SPSC Ring Buffers
                  │ (bounded, lock-free)
┌─────────────────▼─────────────────────────┐
│         Data Plane (Thread-per-Core)      │
│  Physical execution, storage I/O, SIMD    │
│        !Send, io_uring, no locks          │
└───────────────────────────────────────────┘
```

**Control Plane** — Runs on Tokio. Handles connections (pgwire, HTTP, WebSocket), parses SQL via DataFusion, builds logical query plans, and dispatches work to the Data Plane. All types here are `Send + Sync`.

**Data Plane** — One thread per CPU core, each an isolated shard. Reads from NVMe via io_uring, runs SIMD vector math, executes physical query plans. No locks, no atomics, no cross-core sharing. Types are `!Send` by design.

**SPSC Bridge** — Bounded lock-free ring buffers are the only communication path between the two planes. Backpressure is automatic: at 85% queue utilization the Data Plane reduces read depth, at 95% it suspends new reads.

This separation means the Control Plane never touches storage directly, and the Data Plane never handles network I/O. Each does what it's best at.

## Storage Tiers

NodeDB uses tiered storage to match data temperature to the right medium:

| Tier      | Medium | What lives here                                 | I/O                   |
| --------- | ------ | ----------------------------------------------- | --------------------- |
| L0 (hot)  | RAM    | Memtables, active CRDT states, incoming metrics | None (in-memory)      |
| L1 (warm) | NVMe   | HNSW graphs, metadata indexes, segment files    | mmap with madvise     |
| L2 (cold) | S3     | Historical logs, compressed vector layers       | Parquet + HTTP range  |
| WAL       | NVMe   | Write-ahead log                                 | O_DIRECT via io_uring |

The WAL uses O_DIRECT (bypasses page cache) for deterministic write latency. L1 indexes use mmap for zero-copy reads. These never share page cache.

## Per-Collection Storage Models

Unlike most databases that lock you into one storage model, NodeDB lets you choose per collection:

- **Document (schemaless)** — MessagePack blobs, flexible schema, CRDT sync. Best for evolving data.
- **Document (strict)** — Binary Tuples with fixed schema, O(1) field extraction. Best for OLTP.
- **Columnar** — Per-column compression, block statistics, predicate pushdown. Best for analytics. Timeseries and Spatial are profiles that extend it.
- **Key-Value** — Hash-indexed O(1) point lookups. Best for key-dominant access patterns.

**Columnar-first architecture.** Columnar is the base storage engine for all analytics workloads. Timeseries and Spatial are profiles layered on top of it — they do not have separate storage layers. All three share the same `columnar_memtables` (the in-memory L0 write buffer). Profile-specific behavior (partition-by-time, R*-tree indexing) is implemented as extensions to the base `ColumnarOp` physical plan node:

- `ColumnarOp` — base plan for plain columnar collections
- `TimeseriesOp` — extends `ColumnarOp` with `time_range` bounds, time bucketing, and retention
- `SpatialOp` — extends `ColumnarOp` with R*-tree candidate lookup

A `TIME_KEY` column modifier on a `TIMESTAMP` or `DATETIME` column designates the primary time dimension. A `SPATIAL_INDEX` modifier on a `GEOMETRY` column triggers automatic R*-tree maintenance.

Collections can be converted between modes at any time with `CONVERT COLLECTION <name> TO <mode>`.

## HTAP Bridge

Strict (OLTP) and Columnar (OLAP) collections can work together through materialized views. A `CREATE MATERIALIZED VIEW` on a strict collection automatically replicates changes to a columnar representation via CDC. The query planner routes point lookups to the strict engine and analytical scans to the columnar engine — no ETL pipeline needed.

## Edge-to-Cloud Sync

NodeDB-Lite (the embedded variant) writes CRDT deltas locally. When connectivity returns, deltas sync to Origin over WebSocket. Multiple devices converge to the same state regardless of operation order. SQL constraints (UNIQUE, FK, CHECK) are enforced on Origin at sync time, with typed compensation hints sent back to devices on conflict.

See [NodeDB-Lite](lite.md) for details on the embedded database.

## Cross-Engine Queries

All engines share the same snapshot, transaction context, and memory budget. A query that combines vector similarity, graph traversal, spatial filtering, and document field access executes inside one process — no network hops between engines, no application-level joins.

[Back to docs](README.md)
