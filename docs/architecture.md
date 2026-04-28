# Architecture

NodeDB splits work across three planes connected by lock-free ring buffers. This separation is the core design decision — each plane does exactly what it is best at and nothing else.

## Three-Plane Execution Model

```
┌───────────────────────────────────────────┐
│           Control Plane (Tokio)           │
│  SQL parsing, query planning, connections │
│           Send + Sync, async              │
└─────────────┬──────────────┬──────────────┘
              │ SPSC Bridge  │ Event subscriptions
              │              │
┌─────────────▼──────────┐  ┌▼────────────────────────────────────┐
│  Data Plane (TPC)      │  │  Event Plane (Tokio)                │
│  Physical execution    ├─►│  AFTER trigger dispatch             │
│  Storage I/O, SIMD     │  │  CDC change streams                 │
│  !Send, io_uring       │  │  Cron scheduler                     │
│  Emits WriteEvent      │  │  Durable pub/sub, webhook delivery  │
│  (Insert/Update/Delete)│  │  Retry, DLQ, backpressure           │
└────────────────────────┘  └─────────────────────────────────────┘
```

**Control Plane** — Runs on Tokio. Handles connections (pgwire, HTTP, WebSocket), parses SQL via DataFusion, builds logical query plans, and dispatches work to the Data Plane. All types here are `Send + Sync`.

**Data Plane** — One thread per CPU core, each an isolated shard. Reads from NVMe via io_uring, runs SIMD vector math, executes physical query plans. No locks, no atomics, no cross-core sharing. Types are `!Send` by design. Emits `WriteEvent` records (covering inserts, updates, and deletes via `WriteOp`) to the Event Plane via per-core bounded ring buffers after each WAL commit.

**Event Plane** — Runs on Tokio. Consumes the event stream from the Data Plane and handles all asynchronous, event-driven work: AFTER trigger dispatch, CDC change stream delivery, cron job evaluation, durable pub/sub topics, and webhook HTTP delivery. Side effects (trigger bodies, scheduled SQL) are dispatched back through the normal Control Plane → Data Plane path — the Event Plane handles routing and delivery, not compute. WAL-backed crash recovery ensures no events are lost across restarts.

**SPSC Bridge** — Bounded lock-free ring buffers are the only communication path between the planes. Backpressure is automatic: at 85% queue utilization the Data Plane reduces read depth, at 95% it suspends new reads.

### Plane Boundaries

| Plane         | Does                                                                 | Does not do                                              |
| ------------- | -------------------------------------------------------------------- | -------------------------------------------------------- |
| Control Plane | SQL parsing, query planning, connection handling                     | Event processing, trigger execution, storage I/O         |
| Data Plane    | Physical I/O, SIMD math, WAL append, BEFORE triggers                 | Event delivery, cross-shard coordination, AFTER triggers |
| Event Plane   | AFTER trigger dispatch, CDC, cron, webhook delivery, durable pub/sub | Query planning, storage I/O, spawning TPC tasks          |

**Mixing planes is a correctness bug.** If code needs to cross a plane boundary, it goes through the SPSC bridge.

## Query Entry Paths

There are two ways a query reaches the Data Plane. Both produce the same `PhysicalPlan` and execute identically from that point on.

**SQL path (user-facing)** — All user-visible interfaces use SQL. `psql`, the `ndb` CLI, and the HTTP `/query` endpoint all accept SQL text. The Control Plane runs it through DataFusion (parse → logical plan → optimize → `PhysicalPlan`):

```
psql / ndb CLI / HTTP /query
         │
         ▼
   DataFusion parser
         │
         ▼
   Logical plan + optimizer
         │
         ▼
   PhysicalPlan ──► SPSC Bridge ──► Data Plane
```

**Native opcode path (SDK optimization)** — The Rust SDK (`nodedb-client`), FFI bindings (`nodedb-lite-ffi`), and WASM bindings (`nodedb-lite-wasm`) dispatch typed opcode messages over the NDB protocol instead of SQL text. The Control Plane converts them directly to a `PhysicalPlan` via `build_plan()`, skipping SQL parsing and serialization:

```
nodedb-client / nodedb-lite-ffi / nodedb-lite-wasm
         │
         ▼
   Native opcode + typed fields
         │
         ▼
   build_plan()
         │
         ▼
   PhysicalPlan ──► SPSC Bridge ──► Data Plane
```

SDKs support **both modes** on the same connection. Use SQL for complex queries and rapid prototyping (`client.sql("SELECT ...")`). Use native methods for hot-path CRUD and high-throughput ingest (`client.get()`, `client.put()`, `client.vector_search()`) where parsing overhead matters.

## Storage Tiers

NodeDB uses tiered storage to match data temperature to the right medium:

| Tier      | Medium | What lives here                                 | I/O                   |
| --------- | ------ | ----------------------------------------------- | --------------------- |
| L0 (hot)  | RAM    | Memtables, active CRDT states, incoming metrics | None (in-memory)      |
| L1 (warm) | NVMe   | HNSW graphs, metadata indexes, segment files    | mmap with madvise     |
| L2 (cold) | S3     | Historical logs, compressed vector layers       | Parquet + HTTP range  |
| WAL       | NVMe   | Write-ahead log                                 | O_DIRECT via io_uring |

The WAL uses O_DIRECT (bypasses page cache) for deterministic write latency. L1 indexes use mmap for zero-copy reads. These never share page cache.

**Checkpoints and tombstones.** Each checkpoint garbage-collects the WAL rows for collections that have been hard-deleted (tombstoned), so tombstone records do not accumulate across restarts. On replay, the startup path merges persisted WAL tombstones with tombstones extracted from the WAL itself, so a crash mid-purge cannot resurrect a dropped collection.

**Backups.** The backup envelope embeds catalog rows and the source tombstone set alongside segment data, so a restored snapshot reconstructs the catalog deterministically and refuses to resurrect collections tombstoned before the backup was taken. Each `StoredCollection` row carries a `size_bytes_estimate` field, surfaced through `_system.dropped_collections` so operators can size the L2 cleanup queue before issuing `PURGE`.

## Per-Collection Storage Models

Unlike most databases that lock you into one storage model, NodeDB lets you choose per collection:

- **Document (schemaless)** — MessagePack blobs, flexible schema, CRDT sync. Best for evolving data.
- **Document (strict)** — Binary Tuples with fixed schema, O(1) field extraction. Best for OLTP.
- **Columnar** — Per-column compression, block statistics, predicate pushdown. Best for analytics. Timeseries and Spatial are profiles that extend it.
- **Key-Value** — Hash-indexed O(1) point lookups. Best for key-dominant access patterns.

**Columnar-first architecture.** Columnar is the base storage engine for all analytics workloads. Timeseries and Spatial are profiles layered on top of it — they do not have separate storage layers. All three share the same `columnar_memtables` (the in-memory L0 write buffer). Profile-specific behavior (partition-by-time, R\*-tree indexing) is implemented as extensions to the base `ColumnarOp` physical plan node:

- `ColumnarOp` — base plan for plain columnar collections
- `TimeseriesOp` — extends `ColumnarOp` with `time_range` bounds, time bucketing, and retention
- `SpatialOp` — extends `ColumnarOp` with R\*-tree candidate lookup

A `TIME_KEY` column modifier on a `TIMESTAMP` or `DATETIME` column designates the primary time dimension. A `SPATIAL_INDEX` modifier on a `GEOMETRY` column triggers automatic R\*-tree maintenance.

Collections can be converted between modes at any time with `CONVERT COLLECTION <name> TO <mode>`.

## HTAP Bridge

Strict (OLTP) and Columnar (OLAP) collections can work together through materialized views. A `CREATE MATERIALIZED VIEW` on a strict collection automatically replicates changes to a columnar representation via CDC. The query planner routes point lookups to the strict engine and analytical scans to the columnar engine — no ETL pipeline needed.

## Edge-to-Cloud Sync

NodeDB-Lite (the embedded variant) writes CRDT deltas locally. When connectivity returns, deltas sync to Origin over WebSocket. Multiple devices converge to the same state regardless of operation order. SQL constraints (UNIQUE, FK, CHECK) are enforced on Origin at sync time, with typed compensation hints sent back to devices on conflict.

See [NodeDB-Lite](lite.md) for details on the embedded database.

## Cross-Engine Queries

All engines share the same snapshot, transaction context, and memory budget. A query that combines vector similarity, graph traversal, spatial filtering, and document field access executes inside one process — no network hops between engines, no application-level joins.

## Cross-Engine Identity

All engines use a unified, distributed identity space called **surrogate identity**. Each row, cell, node, and document has a surrogate ID (u64) that is globally unique within a database. Surrogates enable fused queries that combine filtering across all engines in a single bitmap.

**Why surrogates matter:**

- **Bitmap fusion** — A predicate like "documents matching this full-text query AND vectors similar to this query AND graph nodes reachable in 2 hops" compiles to a single Roaring Bitmap of candidate surrogates. No three-way JOIN needed.
- **Fast intersection** — Combine constraints across engines in microseconds using bitwise operations.
- **Distributed execution** — Surrogates map to vShards; the planner scatters the fused predicate to the correct shard cores in parallel.

### How It Works

When you insert a row into any engine, the Control Plane assigns a unique `surrogate_id`. This ID is embedded in:

- Vector index payloads (allows pre-filtering vector search by document properties, graph reachability, etc.)
- Graph node attributes (allows querying nodes by document properties)
- Document metadata (allows filtering documents by vector similarity, graph membership)
- Array cell metadata (allows filtering cells by spatial properties, text search)
- Full-text posting lists (allows narrowing FTS results by vector similarity, graph traversal)

### Example: Multimodal RAG Query

```sql
-- Vector + Graph + Text fusion in one query
SELECT docs.id, docs.title, rrf_score() AS score
FROM documents AS docs
WHERE docs.id IN (
    SEARCH vectors USING VECTOR(docs.embedding, query_vec, 1000)
  )
  AND docs.id IN (
    GRAPH TRAVERSE FROM 'topic:ml' DEPTH 2
  )
  AND text_match(docs.body, 'machine learning transformers')
LIMIT 10;
```

Internally:

1. Vector search returns a Roaring Bitmap of surrogate IDs (documents similar to the query)
2. Graph traverse returns a Roaring Bitmap of surrogate IDs (documents reachable from the topic node)
3. Full-text search returns a Roaring Bitmap of surrogate IDs (documents matching the text query)
4. The planner intersects the three bitmaps in parallel: `vector_bitmap & graph_bitmap & fts_bitmap`
5. The result is a single bitmap of surrogates that satisfy all three constraints
6. Final fetch pulls documents, re-ranks by RRF score, and returns top 10

All bitmap operations happen on the Data Plane in microseconds. No network hops, no intermediate result sets.

### Distributed Surrogate Routing

On a cluster, surrogates map to vShards using a consistent hash:

```
surrogate_id = hash(tenant_id, global_row_id) % num_vshards
```

All operations on a surrogate route to the same vShard core. Fused bitmap queries scatter-gather across shards: each shard computes its local bitmap intersection, results merge at the Control Plane.

### Performance

- **Bitmap intersection** — O(log n) bitwise operations on compressed Roaring Bitmaps
- **No intermediate sets** — Predicates fuse before any documents are fetched
- **Cache-friendly** — Bitmaps are small (kilobytes for millions of rows) and fit in L1/L2 cache

[Back to docs](README.md)
