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

**Control Plane** — Runs on Tokio. Handles connections (pgwire, HTTP, WebSocket), parses SQL via nodedb-sql (sqlparser-rs based), builds logical query plans, and dispatches work to the Data Plane. All types here are `Send + Sync`.

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

**SQL path (user-facing)** — All user-visible interfaces use SQL. `psql`, the `ndb` CLI, and the HTTP `/v1/query` endpoint all accept SQL text. The Control Plane runs it through nodedb-sql (parse → logical plan → optimize → `PhysicalPlan`):

```
psql / ndb CLI / HTTP /v1/query
         │
         ▼
   nodedb-sql parser
         │
         ▼
   Logical plan + optimizer
         │
         ▼
   PhysicalPlan ──► SPSC Bridge ──► Data Plane
```

**Native opcode path (SDK optimization)** — The Rust SDK (`nodedb-client`), FFI bindings (`nodedb-lite-ffi`), and WASM bindings (`nodedb-lite-wasm`) dispatch typed opcode messages over the NDB protocol instead of SQL text. The Control Plane converts them directly to a `PhysicalPlan` via engine-specific handlers, skipping SQL parsing and serialization:

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

## Multi-Raft Consensus

NodeDB uses Multi-Raft — each vShard is its own independent Raft group. Three kinds run simultaneously:

| Kind          | Purpose                                                       |
| ------------- | ------------------------------------------------------------- |
| **Data**      | One per vShard — replicates WAL entries for that shard's data |
| **Meta**      | Cluster membership, catalog, schema                           |
| **Sequencer** | Cross-shard transaction ordering (Calvin epoch log)           |

Each kind has independent leader election. A sequencer leader failure does not affect data-group leaders.

**Placement and rebalancing.** Each Raft group's voter set is derived deterministically by rendezvous (highest-random-weight) hashing over active nodes, taking the top `min(replication_factor, node_count)` nodes per group. A reconcile loop on the metadata-group leader converges membership toward placement on every tick: it proposes learners for placement nodes not yet in membership, promotes them when caught up, evicts out-of-placement learners, removes leaving voters, and transfers leadership away from placement-excluded leaders (via TimeoutNow). Node joins kick an immediate reconcile. `REBALANCE` triggers it manually.

**Log compaction and snapshots.** Raft log auto-compaction is opt-in (`log_compaction_threshold`; disabled by default) and gated on the durable Data-Plane applied watermark — entries are only truncated once their effects are applied, not merely committed. Lagging followers catch up via per-group snapshots that carry the full Data-Plane state (rows, CRDT state, graph edges, PK→surrogate identity map) using exact clear-then-install semantics; persisted `.snap` files re-install on follower boot.

## Cross-Shard Transactions

Write transactions that touch multiple vShards go through the **Calvin sequencer** rather than two-phase commit. The sequencer Raft group produces a globally-ordered log of transaction batches (epochs, default 20 ms). Within each epoch, the scheduler derives a deterministic lock order and the executor runs all writes concurrently without cross-shard coordination.

**Interactive transactions are atomic across shards.** A `BEGIN ... COMMIT` block whose statements span multiple vShards (or nodes) flushes at COMMIT as one Calvin transaction bound by a durable **Vote/Verdict barrier**: each participant replicates a commit/abort vote through the sequencer log; when the tally completes, a replicated verdict commits or drops every participant's staged writes together. Expected-participant counts are seeded from the replicated epoch batch, so the barrier survives sequencer failover. Read-only participants count too — a transaction that wrote shard X but read shard Y validates and votes on both.

**Serializable OCC.** Reads recorded during the transaction (point reads, predicate scans, index equality/range probes, both sides of gathered cross-node JOINs) are validated at COMMIT against per-key, per-collection, and per-index-value write LSNs. A stale read aborts the transaction with SQLSTATE `40001` (retryable) instead of committing silently.

**Durability.** A committed transaction's writes are persisted as a single atomically-replayable `TransactionRedo` WAL record appended before the commit is acknowledged. WAL-only recovery replays it in LSN order, rebuilding even in-memory secondary indexes (vector HNSW, FTS postings) that base storage cannot reconstruct alone.

For value-dependent predicates (e.g., `WHERE balance > 0`), the executor uses **OLLP** (Optimistic Lock Location Prediction): optimistically proceed, then re-validate and retry on mismatch. A circuit breaker opens when the retry ratio exceeds 50% for a predicate class.

```sql
-- Require atomic cross-shard writes (default)
SET cross_shard_txn = 'strict';

-- Opt out of atomicity for bulk loads: writes are grouped per vShard and each
-- group commits as an independent single-vShard transaction; a failure does NOT
-- roll back vShards that already committed
SET cross_shard_txn = 'best_effort_non_atomic';
```

(Bare `best_effort` is deliberately rejected; invalid values return SQLSTATE `22023`.)

**Single-node deployments run Calvin by default** (`[server] single_node_calvin = true`): a standalone node synthesizes a one-node sequencer group so transactions spanning multiple cores (vShards) commit atomically instead of being rejected. Set it `false` to force the legacy fast path. Uncontended single-shard point writes bypass the sequencer entirely and go directly through the relevant data-group Raft; contended or predicate/bulk writes route through the deterministic scheduler.

**Overlay hygiene.** Per-transaction staging overlays are kept alive by every staged write/read; overlays orphaned by vanished clients are reaped after a 6-hour lease. The `nodedb_active_txn_overlays` Prometheus gauge tracks live overlays. Data-Plane resource rejection surfaces as SQLSTATE `53200` (backpressure — retry when pressure subsides).

## Distributed Query Execution

Multi-node SELECTs pick their execution strategy from `ANALYZE` statistics — run `ANALYZE <collection>` to collect per-column `row_count`, `distinct_count`, and `avg_value_len` into the catalog (auto-ANALYZE re-runs after ~10% of rows change).

**Joins** — The cost model estimates each side's size from statistics and chooses between **broadcast join** (small side shipped to every node) and **shuffle join** (both sides hash-partitioned across nodes over the QUIC streaming transport). Unanalyzed or empty sides fall back to broadcast. Overrides: `SET nodedb.force_shuffle_join = true`, `SET nodedb.broadcast_threshold_bytes = ...` (default from `[tuning.cluster_transport] broadcast_threshold_bytes`, 8 MiB).

**GROUP BY** — High-cardinality aggregations shuffle groups across nodes instead of gathering all rows at the coordinator. Selection is driven by `distinct_count`; overrides: `SET nodedb.shuffle_agg_threshold`, `nodedb.force_shuffle_agg`, `nodedb.shuffle_agg_num_parts`.

**Spill** — Hash joins and aggregations that exceed memory spill to disk via grace-hash partitioning (recursive re-partitioning, io_uring sequential writers). Knobs in `[tuning.query]`: `groupby_max_groups_in_mem` (default 1M), `aggregate_chunk_size` (default 10K), `max_scan_result_bytes` (default 512 MiB — the memory-budget bound on unbounded SELECTs; exceeding it returns a typed `ResourcesExhausted` error, never a silent truncation).

**Streaming** — Eligible unordered SELECT results stream from Data Plane cores over QUIC to the coordinator and on to the client (pgwire, native, HTTP NDJSON) without materializing the full result set on the coordinator.

## Cross-Engine Queries

All engines share the same snapshot, transaction context, and memory budget. A query that combines vector similarity, graph traversal, spatial filtering, and document field access executes inside one process — no network hops between engines, no application-level joins.

## Resource Governance

NodeDB enforces multi-tier resource isolation to prevent a single tenant or database from starving others. Resource governance encompasses memory, connections, request rate, storage I/O scheduling, WAL commit priority, document cache allocation, background task CPU budgets, and compute bounds.

### Three-Tier Resource Hierarchy

All request-path resource decisions follow a **global ceiling → database budget → tenant budget → engine internal usage** hierarchy:

- **Global ceiling**: Cluster-wide configuration (e.g., `cluster.max_memory_bytes`). Applies to all databases.
- **Database budget**: Per-database quotas set via `ALTER DATABASE name SET QUOTA (...)`. Applies to all tenants within that database.
- **Tenant budget**: Per-tenant quotas within a database set via `ALTER TENANT name IN DATABASE db SET QUOTA (...)`. Applies within the narrowest scope.
- **Engine internal usage**: Governed by `MemoryGovernor`; descends the hierarchy to reserve bytes.

Admission ordering for requests:

1. Identity & database access (established during authentication)
2. **Tenant** rate/concurrency check → `TENANT_QUOTA_EXCEEDED`
3. **Database** rate/concurrency check → `DATABASE_QUOTA_EXCEEDED`
4. **Global** pressure check → `SERVER_OVERLOAD`

Memory reservation flips the order (largest scope first to fail fast): global → database → tenant → engine. Failure at any layer aborts before the next is consulted.

### Memory Governor

Located in `nodedb-mem/src/governor.rs`, the `MemoryGovernor` enforces hierarchical memory reservations:

```rust
pub struct MemoryGovernor {
    global_ceiling: usize,
    database_budgets: HashMap<DatabaseId, Budget>,
    tenant_budgets: HashMap<(DatabaseId, TenantId), Budget>,
    engine_budgets: HashMap<EngineId, Budget>,
}

impl MemoryGovernor {
    pub fn try_reserve(
        &self,
        db: DatabaseId,
        tenant: TenantId,
        engine: EngineId,
        size: usize,
    ) -> Result<ReservationToken, BudgetError>
    // Reserves across all four levels: global → database → tenant → engine
}
```

`ReservationToken` is RAII — dropping it releases against all four levels atomically. This prevents leaks across plane boundaries. Every allocation site walks all four levels before touching memory.

### Connection Semaphore

Defined in `nodedb/src/control/server/listener.rs`, connection admission occurs in two phases:

**Phase 1 (at TLS accept):** Acquire a temporary global permit from `cluster.max_connections`.

**Phase 2 (post-auth):** After SCRAM/Argon2 handshake succeeds and `database_id`/`tenant_id` are known, convert the temporary permit into a database-bucketed + tenant-bucketed permit (3-level ref-counted). Verify both `database.quota.max_connections` and `tenant.quota.max_connections`.

On disconnect, all three ref-counted permits are released atomically. If Phase 2 fails, the temporary permit is dropped and an error is returned.

### SPSC Bridge — Weighted-Fair Queue

The lock-free SPSC bridge between Control and Data Planes (`nodedb-bridge/src/wfq.rs`) implements **Weighted Fair Queueing (WFQ)** via **Deficit Round-Robin (DRR)**.

Each Data Plane core's request ring contains a set of virtual sub-queues, one per active `DatabaseId` on that core. The dispatcher schedules these sub-queues fairly by:

- Assigning each database a weight proportional to `database.quota.priority_class` (three tiers: `critical`, `standard`, `bulk`).
- Running deficit round-robin: on each dispatcher tick, the next database with deficit > 0 is chosen; its requests are dequeued until deficit exhausts, then the next database is selected.
- Computing backpressure (85% / 95%) per virtual queue (not globally), so one saturated database never throttles another's enqueue rate.
- Preserving total ring capacity (bounded).

A database saturating its deficit on a core throttles only its own writes; co-resident databases continue flowing.

### WAL Group Commit — Priority-Aware

Write-ahead log group commit in `nodedb-wal/src/group_commit.rs` separates commits into three independent priority-based fsync groups:

| Class      | Behavior                                           |
| ---------- | -------------------------------------------------- |
| `critical` | Dedicated fsync group, committed first             |
| `standard` | Default; batched with other standard-class commits |
| `bulk`     | Extended timeout, lower fsync rate                 |

Three groups maximum to avoid fsync amplification. The `priority_class` field is set per database via `WITH (priority_class='...')` on `CREATE` / `ALTER DATABASE SET QUOTA`. Critical-class databases' writes are flushed to durable storage first; bulk writes extend the timeout window and reduce fsync frequency.

### Document Cache — Per-Database Weighted LRU

The document cache in `nodedb/src/engine/sparse/doc_cache.rs` is a weighted LRU sharded by `DatabaseId`. Each shard's capacity share is proportional to `database.quota.cache_weight` (default 1).

`CacheKey` includes both `database_id` and `tenant_id`. Eviction prefers the database with the highest current-vs-weight overshoot, ensuring hot databases do not evict cold databases below their proportional share.

### Background Tasks — Per-Database CPU-Seconds Budget

The maintenance scheduler in `nodedb/src/control/maintenance/budget.rs` tracks per-database CPU-seconds spent in background tasks (compaction, index maintenance, cleanup) per minute and enforces a cap of `database.quota.maintenance_cpu_pct` (default 25%) of core time. Databases that exceed the cap defer their maintenance to the next minute window.

The following maintenance tasks acquire `MaintenanceLease` from the budget tracker (RAII handle):

- **Vector** — HNSW node removal + neighbour-link remapping (compact operation)
- **Graph** — CSR compaction (level-based reorganization) + dangling edge sweep
- **Timeseries** — Segment compaction + continuous aggregation refresh
- **Spatial** — R-tree rebalancing
- **Array** — Tile compaction and version cleanup
- **Full-Text Search** — LSM level compaction via `run_fts_compaction()` in `nodedb/src/data/executor/handlers/compact_fts.rs`

All sites using `acquire_maintenance_lease(db, force)` observe the per-database budget.

### Rate Limiter

The rate limiter in `nodedb/src/control/security/ratelimit/limiter.rs` enforces request-rate caps using token buckets. Buckets are consulted in order of specificity — first-deny wins:

```
user:{id} → org:{id} → tenant:{id} → database:{id}
```

The database bucket capacity is `database.quota.max_qps`. Additionally, pre-auth login rate limits are enforced:

- `login_ip:{addr}` — capacity `cluster.login_attempts_per_ip_per_min` (default 30 **failures**/min)
- `login_user:{username}` — capacity `cluster.login_attempts_per_user_per_min` (default 10 **failures**/min)
- Argon2-DoS ceiling per IP — `max(ip_cap × 4, 120)` credential verifications/min, consumed on every attempt

Only genuine credential failures consume the failure budgets — the admission check peeks the buckets, so a burst of successful reconnects (e.g., a warming connection pool) is never rejected. These pre-auth buckets are consulted before SCRAM/Argon2 verification (cheap exit path) and run in constant time with a uniform delay on any denial to prevent timing leaks. A rate-limit denial returns a distinct, retryable `TOO_MANY_CONNECTIONS` error rather than a generic credential failure.

### Per-Tenant Compute Caps

Two per-tenant compute bounds are enforced at planner time via `nodedb/src/control/planner/sql_plan_convert/`:

- **`max_vector_dim`**: Maximum vector dimensionality. Vector inserts / index operations above this bound are rejected with `TENANT_VECTOR_DIM_EXCEEDED`.
- **`max_graph_depth`**: Maximum graph traversal depth. MATCH queries / graph traversals declaring depth > this bound are rejected with `TENANT_GRAPH_DEPTH_EXCEEDED`.

Both are surfaced in `SHOW TENANT QUOTA` output.

### Metrics & Observability

All resource consumption is observable via Prometheus metrics prefixed `nodedb_`:

**Per-Database Metrics** (`nodedb_database_*{database="<name"}`):

- `qps` — Requests per second
- `memory_bytes` — Total memory reserved
- `storage_bytes` — Cumulative storage on-disk
- `connections` — Open connections
- `bridge_queue_depth` — Pending requests in SPSC bridge virtual queue
- `wal_commit_latency_p99` — 99th percentile WAL fsync latency (milliseconds)
- `maintenance_cpu_seconds` — CPU seconds spent in background tasks (per minute, reset window)
- `mirror_lag_ms` — Replication lag to follower replicas (distributed deployments)

**Per-Tenant Metrics** (`nodedb_tenant_*{database="<name>", tenant="<tenant>"}`):

- `qps` — Requests per second
- `memory_bytes` — Total memory reserved
- `storage_bytes` — Cumulative storage on-disk

Counters are incremented at the sites where resources are consumed:

- **QPS**: Request dispatcher (Control Plane)
- **Memory**: `MemoryGovernor::try_reserve()` success path
- **Storage**: WAL append + segment compaction commit
- **Connections**: Connection listener (Accept + Disconnect)
- **Bridge queue depth**: SPSC enqueue / dequeue
- **WAL latency**: Group commit fsync completion
- **Maintenance CPU**: Lease acquisition / release
- **Replication lag**: Raft follower log application

All metrics are dimensionalized by database and tenant to enable per-customer tracking and alerting.

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
