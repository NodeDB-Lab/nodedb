# Changelog

All notable changes to NodeDB will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
NodeDB uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Fixed

- **Native protocol transactions** — a row committed inside an explicit transaction over the native binary protocol (`Begin` op → SQL DML → `Commit` op) is now visible to PK point lookups and filtered aggregates, not just full scans (#193). The native COMMIT seam routed the commit's `ResolveTxn`/`TransactionBatch` meta-ops through the gateway, which cannot derive a route for collection-less meta plans and fell back to vShard 0 — durably applying the commit batch on the wrong core. Native COMMIT now dispatches with the task's pre-classified vShard, matching pgwire, and the gateway router rejects commit meta-ops outright instead of silently misrouting them. **Note:** rows committed through native-protocol transactions on affected multi-core builds were applied to vShard 0's core and remain there after upgrading — they stay visible to full scans but invisible to point lookups; re-inserting them (or dump/reload) re-homes them.

---

## [0.4.0] - 2026-07-20

NodeDB 0.4.0 is a substantial distributed-correctness and durability release. It adds cross-shard transactional execution and distributed query/graph processing, extends temporal and sparse-vector SQL, and hardens replication, recovery, indexing, authentication, and sync across the storage engines.

### ⚠️ Breaking changes

- **On-disk storage layout is now database-scoped.** Every engine (Document, KV, Columnar, Timeseries, Spatial, Vector, sparse-vector, Graph, FTS) keys its storage maps and on-disk paths by `database_id`, and the PK→surrogate catalog is scoped to `(database_id, tenant_id, collection, pk)`. **A 0.3.0 data directory will not be found by a 0.4.0 binary** — a dump/reload is required to upgrade.
- **WAL / on-disk format evolved (forward-incompatible).** New _required_ WAL record types (`SyncSeqAdvance`, `FtsIndex`, `FtsDelete`, `SpatialPut`, `SpatialDelete`), a map-encoded `ColumnarWalRecord` carrying per-row surrogates, and new replayable KV / graph-label / CRDT-list / columnar-predicate records. A 0.3.0 binary cannot replay a 0.4.0 WAL; a 0.4.0 binary still replays 0.3.0 logs.
- **`NodeDb` trait** gained CRDT movable-list operations — trait implementors and callers must update.
- **`single_node_calvin` now defaults to `true`.** A standalone node stands up the single-node Calvin sequencer by default, and cross-core (cross-vShard) transactions commit atomically through it instead of being rejected. Restore the prior behavior with `single_node_calvin = false`.
- **SQL authorization is now enforced across all transports.** Statements that previously bypassed authorization on the native/HTTP paths are now rejected.
- **Cluster wire compatibility** — removed superseded iterative-WCC and GraphAlgo-BSP wire types; all cluster nodes must run the same minor version.
- **The sync wire frame is incompatible with 0.3.x clients.** Sync now uses a versioned frame with CRC32C integrity and idempotent-producer metadata. Upgrade NodeDB Lite and other sync clients together with the server.
- **Static JWT and catalog OIDC providers must be bound to a server-trusted tenant.** Static `auth.jwt.providers` entries now require `tenant_id`, and catalog providers require `TENANT`; existing catalog provider records without that binding are rejected until recreated. Shared issuers must also use distinct, non-empty audiences so issuer/audience routing is unambiguous.

### Added

- **Distributed ACID transactions** — a deterministic Calvin sequencer for cross-shard/cross-vShard commits with per-participant vote tally, verdict barrier, failover recovery, serialization-failure reporting, and WAL-replayable redo records; a per-transaction staging overlay serving reads and buffering writes for **every** engine (point/predicate/`INSERT … SELECT` writes, KV atomic/batch/TTL ops, document UPSERT, columnar batch inserts, vector search, spatial predicate scans, graph single-/multi-hop/shortest-path, full-text search); full undo/rollback with index, R-tree, column-statistics, and bitemporal-aware reversal; wound-wait lock arbitration, per-vShard deterministic write fencing, and a write-admission gate at the SPSC chokepoint.
- **Complete transaction and DML lifecycle across client protocols** — native sessions now support transaction setup and teardown plus `SAVEPOINT` / `RELEASE` / `ROLLBACK TO SAVEPOINT`, matching pgwire behavior; transactional `MERGE`, `UPDATE … FROM`, and `INSERT … SELECT` resolve dependent reads into point operations while preserving overlays, `RETURNING`, rollback, replay, and cross-shard commit semantics. Abandoned pgwire and native sessions reclaim their transaction overlays.
- **MVCC read validation for distributed OCC** — LSN-versioned read-sets validated against committed write-versions at apply time; per-collection and per-index (`IndexEq`/`IndexRange`) read-version tracking; cross-shard OCC self-abort and read-only-participant handling for shuffle/gather/gathered JOINs.
- **Distributed query execution** — cross-node streaming `Exchange` / `ProviderScan` over QUIC; distributed shuffle JOIN and shuffle GROUP BY with ANALYZE-driven cost-model auto-selection; grace-hash join with recursive re-partitioning and io_uring spill-to-disk; memory-budget-bounded streaming scans replacing silent row caps; lazy streaming of unordered `SELECT`s over pgwire, native, HTTP, and QUIC.
- **Distributed graph** — cross-shard `MATCH` with multi-round continuation and variable-length resume cursors; distributed BSP PageRank, Personalized PageRank, and WCC; dual-homed cross-shard edge insert/delete routed through Calvin with implicit-edge reconciliation on predicate UPDATE/DELETE; owner-partitioned BFS frontier for full cross-node traversal.
- **CRDT via SQL** — `WITH (crdt=true)` on document collections routes DML to CRDT ops with `RETURNING` support; movable-list ops over the native wire protocol and `NodeDb` trait; committed-delta validation at data-plane apply time (CHECK constraints, write-set extraction, descriptor-version fencing); validator rejections surfaced as `DeltaReject`; CRDT writes made quorum-durable under Raft.
- **Sparse-vector engine** — a dimensionless `SPARSEVECTOR` column type threaded through parser, wire format, columnar/strict storage, and DDL; an inverted index maintained on document writes; a `sparse_score` `ORDER BY` surface for sparse-vector search.
- **Cluster elasticity & membership** — rendezvous-hashing placement with explicit per-group placement sets; learner add/remove/auto-promotion/eviction; Raft leadership transfer via `TimeoutNow`; immediate placement reconcile on node join; leaving-voter convergence; orphan partial-snapshot GC; and HiLo batch allocation for cross-node row surrogates.
- **Raft snapshots & compaction** — a Data-Plane snapshot builder wired into the Raft SEND path; exact clear-then-install for lagging followers; CRDT state, graph edges, columnar/timeseries engine state, and the PK→surrogate identity map carried through per-group snapshots and backups; configurable Raft log auto-compaction gated on the durable Data-Plane applied watermark.
- **Idempotent sync** — idempotent-producer wire types with frame integrity; a per-core idempotency gate for sync ingest; sync-HWM replay on startup; Raft-replicated sync writes for FTS, spatial, columnar, timeseries, and vector; collection-schema announcement and peer-collection materialization into the local catalog.
- **Bitemporal SQL and execution** — `FOR SYSTEM_TIME AS OF` and valid-time qualifiers are carried through planning and distributed execution for supported engines. `AS OF SYSTEM TIME NULL` exposes complete version history with temporal bounds for strict and schemaless documents, columnar collections, and timeseries collections, while reserved temporal columns stay out of ordinary projections.
- **pgwire / SQL surface** — `version()`, `current_setting()`, `server_version_num` in startup params, and an extended `pg_catalog` evaluator for driver compatibility; `ST_MakePoint` / `ST_GeomFromText` / `ST_GeomFromWKB` in `INSERT` values; computed expressions in `GROUP BY` keys; a MySQL-style trailing `ENGINE = <name>` clause on `CREATE COLLECTION`; per-collection FTS analyzer honored in every tokenization path.
- **`RESTORE TENANT ... FORCE`** — bypass the staleness guard on restore.

### Changed

- **Response shaping is now planner-driven** — a protocol-neutral output-schema builder wired into query planning drives response shaping and `Describe` across pgwire, native, and HTTP, replacing the per-transport projection paths.
- **CDC change events are published exactly once per write**, and graph node-label/edge writes and cluster-array writes now emit CDC events; the Event-Plane receiver is wired into vShard dispatch.
- **Replication and acknowledgement durability** — native autocommit writes route through Raft, the durable-at-ack barrier extends to pgwire submit and ILP, and async WAL group commit does not acknowledge before the durability barrier. Previously uncovered array-cell, bulk document, KV truncate, CRDT, vector, sparse-vector, graph-label, and graph-index write paths now replicate through Raft or WAL as appropriate. Startup replay and Raft compaction are gated on the durable Data-Plane applied watermark, while dropped Calvin fan-out is recovered from the sequencer log.
- **Cross-engine identity and index maintenance** — bulk KV/native writes, generated-key inserts, columnar batches, and replicated writes now preserve or mint stable surrogates consistently. Index reconciliation was hardened across bulk DML, `MERGE`, `UPDATE … FROM`, rollback, `TRUNCATE`, restore, and WAL replay, including secondary-field, vector, FTS, and spatial index paths.
- **License** — `nodedb-crdt`, `nodedb-mem`, and `nodedb-wal` relicensed to Apache-2.0.

### Fixed

- **Crash-safety hardening** — boot now fails on a corrupt/unreadable checkpoint for every engine (vector, spatial, graph-label, columnar, KV, sparse-vector, timeseries, CRDT, sync-HWM) instead of silently continuing; CRC framing added to checkpoint files; checkpoint encode/restore failures propagated instead of swallowed.
- **WAL replay correctness** — timeseries samples are no longer rejected during replay and a mid-record flush no longer duplicates rows on recovery; per-row surrogates are persisted and restored through columnar WAL replay; complete KV WAL replay (`incr`/`expire`/`persist`/`cas`/`field_set`/`register_index`/`drop_index`) with resolved expiry instants; graph node-label and columnar predicate UPDATE/DELETE records persisted and replayed.
- **Query and SQL correctness** — scan and join execution no longer silently truncates rows at implicit caps; streamed scans drain every chunk; bitemporal range scans, indexed residual predicates, computed and distributed `GROUP BY`, and scalar aggregates over empty input return complete results; strict `AS OF SYSTEM TIME NULL` queries resolve the correct historical schema.
- **Restore / backup** — WAL tombstones replicated via Raft on restore; plain-columnar rows re-issued durably rather than snapshot-installed; columnar/flushed-timeseries data and catalog propagated cluster-wide; replica multiplication and CRDT loss under RF>1 prevented.
- **Session plan cache** — a repeated literal PK point lookup no longer replays a stale empty result after the same session inserted that key. Document point reads and PK-targeted document mutations are excluded from the schema-only plan cache, so a byte-identical `SELECT` reflects the session's own committed writes and the simple and extended protocols agree.
- **Object ownership** — `DROP USER` reassigns every object the user owned to a validated administrative principal — the tenant's recorded admin, else an active `tenant_admin`, else an active superuser in that tenant — and is refused when no such principal exists, instead of repointing objects at a synthetic name that was never created and leaving the data directory unbootable. Collections inside their drop-retention window are included. Ownership records are keyed by `(object_type, database_id, tenant_id, object_name)`, so ownership of a collection no longer extends to a same-named collection in another database. The startup catalog check now repairs dangling owner references and revokes grants to removed users instead of refusing to start, so a data directory already affected by this recovers on its next boot.
- **Tenant management** — tenant IDs allocated via a durable high-water-mark; ghost rows in `SHOW TENANTS` after `DROP TENANT` eliminated; an existence gate enforced for unknown numeric tenant IDs; `DROP`/`ALTER`/`PURGE TENANT` accept a tenant name; duplicate `CREATE TENANT` names rejected with `42710` rather than allocating a second tenant id under the same name.
- **Security** — UTF-8 SQL parser offsets preserved; external superuser assertions prevented; OIDC providers bound to trusted tenants; credential integrity preserved across the auth lifecycle; legitimate login bursts no longer trip rate limits.
- **KV** — TTL-expired rows reaped without stranding index entries; per-row surrogates assigned on native/RESP batch-put and MSET so cross-engine joins over bulk KV writes resolve.
- **Cluster** — remote error codes preserved across RPC; `WrongOwner` excluded from circuit-breaker failures; Raft compaction and restart replay gated on durable apply; AFTER-trigger writes routed to the owning shard; SQL-inserted geometry indexed into the R-tree on document collections.
- **Graph and CRDT correctness** — distributed PageRank accounts for dangling rank mass across shards; variable-length `MATCH` resumes without dropping frontier work or duplicating seeds; cross-shard graph mutations reconcile edge ownership and labels; CRDT state is isolated per collection, quorum-replicated, snapshot-safe, materialized into document storage, and rejects malformed or constraint-violating deltas before apply.
- **Vector** — the prior HNSW node is removed before re-insert on a vector put.

### Quality

- **Multi-node and crash-recovery coverage** — expanded coverage for Calvin/OCC, failover, shuffle queries, snapshots, placement, sync, graph, and CRDT behavior; added real process-kill, WAL-truncation, checkpoint-corruption, and repeated-restart scenarios; hardened CI disk management, dependency policy, and concurrent server-test isolation.

### Removed

- Dead pgwire projection, catalog-propose, and DDL-router modules (superseded by the neutral shaping/DDL router).
- Superseded iterative-WCC and GraphAlgo-BSP cluster wire types.
- Database-scoping schema-migration machinery.
- The unreachable Data-Plane `INSERT … SELECT` handler and the dead spatial-index rebuild backstop.

---

## [0.3.0] - 2026-06-07

### ⚠️ Breaking changes

- **`NodeDb` trait** — `vector_search` and `text_search` gained an `allowed_ids` prefilter parameter. Existing callers and trait implementors must update their signatures.
- **`GraphStmt::GraphAlgo`** — added a `personalization` field; `GRAPH ALGO ... ON <collection>` now also accepts a quoted collection name. Exhaustive matches on this variant must be updated.

### Added

- **Personalized PageRank (PPR)** — seed-biased PageRank end to end: `GRAPH ALGO PAGERANK ... PERSONALIZATION {"node": weight, ...}` over the SQL DSL and via the raw native protocol (`algo_params.personalization_vector`); honored by the engine's teleport/dangling redistribution. `graph_pagerank` exposed on the `NodeDb` trait and both client transports.
- **Hybrid-search prefiltering** — `allowed_ids` candidate restriction on `vector_search` and `text_search`; predicate-filtered shape subscriptions and snapshots in sync.
- **Linear-weight RRF fusion** — `reciprocal_rank_fusion_linear` with per-list weights and deterministic tie-breaking across all fusion variants.
- **Graph observability** — `SHOW GRAPH STATS` with persistent O(1) edge-store counters, tenant-wide aggregation, and `AS OF SYSTEM TIME`; `GraphStats` wire type. `graph_stats` on the `NodeDb` trait and both backends.
- **pgwire / SQL surface** — in-process evaluator for `pg_catalog` virtual tables; `SHOW ROLES`, `SHOW STATS`, `SHOW METRICS`, `SHOW MEMORY`, `SHOW TENANT <name|id>`, `SHOW TENANTS WITH NAME`; superuser session tenant switching via `SET TENANT`; `CREATE INDEX` / `DROP INDEX` planning; `IF [NOT] EXISTS` and `WITH ADMIN` on auth DDL; `COLLECTION` / `TABLE` / `TENANT` object types in `GRANT` / `REVOKE`; `TenantSelector` for name-based tenant references; `SEARCH` function alias and JSON vector literals.
- **Bitemporal documents** — `NodedbStatement` and `Namespace` extended for bitemporal document reads/writes; `LatestVersion` namespace for O(1) live-version lookups; history namespaces.
- **Sync** — inbound sync handlers and wire types for columnar, vector, FTS, and spatial engines; Data-Plane sync ingest ops; DDL changes broadcast to connected Lite sessions after catalog commit.
- **Vector** — multi-dtype storage for HNSW indexes with `storage_dtype` propagated through vector-primary DDL and the upsert path; `VectorSegmentBacking` trait + `PlainMmapBacking`; versioned envelope for quantization codecs.
- **wasm32** — compatibility guards across memory governor, WAL, and vector so the embedded/WASM build links.

### Fixed

- `ALTER USER` / `ALTER ROLE` parsers no longer apply silent fallbacks on unrecognized clauses.
- `GRANT` grantees canonicalized as `user:<name>` or bare role name.
- `SHOW` commands routed through the DDL router before session-parameter handling.
- Native client edge properties serialized without a runtime JSON pass and no longer silently dropped on a serializer error.
- FTS hot paths no longer emit debug `eprintln`.

---

## [0.2.0] - 2026-05-11

### Added

- **Database primitive** — `CREATE`, `DROP`, `ALTER`, `USE`, and `SHOW DATABASE`; database context bound at connection handshake and propagated through WAL, catalog, routing, and planner
- **CLONE DATABASE** — copy-on-write clone with per-engine row materializer, surrogate ceiling for snapshot isolation, and `SHOW DATABASE LINEAGE FOR`
- **MOVE TENANT** — relocate a tenant's collections between databases
- **Mirror database** — cross-cluster read-only replica via Raft Observer role; lag monitor and automatic restart recovery
- **OIDC authentication** — bearer token auth with provider DDL (`CREATE OIDC PROVIDER`) and catalog persistence
- **Per-database audit** — DML audit mode (`ALTER DATABASE SET AUDIT_DML`), database lifecycle events, `user_id` / `statement_digest` propagated through Data Plane and WAL
- **Per-database quotas** — resource budgets for databases and tenants (`ALTER DATABASE SET QUOTA`); sum-of-quotas enforcement; live cap updates
- **Weighted-fair queue** — per-database DRR dispatch in the SPSC bridge; per-database and per-tenant QPS buckets; connection admission control
- **Per-database metrics** — dedicated Prometheus series per database; per-database CPU budget tracker for compaction enforcement
- **DocCache sharding** — shard document cache by `database_id` with weighted eviction
- **ClusterAdmin role** — cluster-wide admin identity; `GRANT/REVOKE ON DATABASE`; `ALTER USER SET DEFAULT DATABASE`
- **Session registry** — kill-channel per session, hard-revoke on credential change
- **Credential hardening** — persistent lockout state, per-user credential versioning, pre-authentication login rate limiting
- **Continuous aggregate DDL** — `CREATE CONTINUOUS AGGREGATE` with catalog persistence
- **`SHOW AUDIT WHERE`** — filter clause on audit log queries
- **nodedb-client** — graph DSL, field-aware vector ops, text search, and bound-parameter support (`sql_params`) in the native protocol
- **FTS** — crash-safe LSM compaction with dedicated compaction module
- **Memory governor** — over-release counter on `Budget` and `Governor` for accounting correctness

### Fixed

- `DISTINCT` deduplication now operates on projected output, not raw rows
- `ORDER BY` correctly propagated into aggregate plans; derived-`FROM` subqueries supported
- `DROP COLLECTION IF EXISTS` routed through typed handler so the flag is honoured
- Catalog orphan-row violations self-healed at startup
- `EventPlane` drop no longer silently discards pending `WriteEvent`s
- Consumer-disconnect events misclassified as security violations
- ILP measurement names with `/` now route correctly for database-qualified paths

---

## [0.1.0] - 2026-05-07

> First structured release. Ready for pilot deployments and early adopters.
> We welcome feedback before the 1.0 stable release.
> Versions prior to 0.1.0 were alpha iterations.

### Added

#### Engines

- **Document (schemaless)** — MessagePack blobs with secondary indexes, schemaless writes, predicate scans, CRDT sync variant for offline-first workloads
- **Document (strict)** — Binary Tuple encoding with O(1) field extraction, schema enforcement, multi-version `ALTER ADD COLUMN`, CRDT adapter
- **Key-Value** — Hash-indexed O(1) point lookups, native TTL with expiry wheel, optional secondary indexes on value fields, SQL-queryable
- **Columnar** — Compressed column segments (ALP, FastLanes, FSST, Gorilla, LZ4), 1024-row blocks with block statistics, predicate pushdown, delete bitmaps, crash-safe compaction
- **Timeseries** — Cascading compression (20–40× ratios), sparse primary index with block-level min/max skip, continuous aggregation engine with incremental refresh and watermarks, ILP ingest with adaptive batching, approximate aggregates (HLL, t-digest, topK)
- **Spatial** — R\*-tree index with bulk load and nearest-neighbor, geohash and H3 hexagonal indexes, OGC predicates (`ST_Contains`, `ST_Intersects`, `ST_DWithin`, etc.), WKB/WKT/GeoJSON/GeoParquet interchange, hybrid spatial-vector search
- **Vector** — HNSW (in-memory) and Vamana/DiskANN (SSD-resident, billion-scale); quantization: SQ8, PQ, IVF-PQ, OPQ, Binary, Ternary (BitNet 1.58), RaBitQ, BBQ; NaviX adaptive filtered traversal (VLDB 2025); SIEVE workload-routed subindices; MetaEmbed multi-vector with ColBERT MaxSim/PLAID; Matryoshka adaptive-dim; SPFresh streaming index updates; vector-primary collection mode (Pinecone/Qdrant replacement)
- **Array** — ND sparse multi-dimensional engine with dedicated DDL (`CREATE ARRAY ... DIMS ... TILE_EXTENTS`); coordinate-tuple keying; tile-based compression via `nodedb-codec`; Z-order indexing; per-tile MBR statistics; bitemporal cells with `audit_retain_ms` retention; targets genomics, single-cell, earth observation, climate, and sparse ML workloads
- **Graph** (cross-engine overlay) — CSR adjacency index, 13 native algorithms (PageRank, WCC, LabelPropagation, SSSP, Betweenness, Closeness, Louvain, k-Core, and more), Cypher-subset MATCH pattern engine, GraphRAG vector+graph fusion, distributed BSP
- **Full-Text Search** (cross-engine overlay) — Block-Max WAND BM25 with 128-doc block pruning, 16 Snowball stemmers, 27-language stop words, CJK bigram tokenization, posting compression, LSM storage, fuzzy matching, synonyms, phrase proximity, hybrid vector+text RRF fusion

#### Protocols & APIs

- PostgreSQL wire protocol (pgwire) — SQL over standard Postgres clients and drivers
- HTTP/REST — JSON API for document and query operations
- Native binary protocol — MessagePack over TCP for low-latency clients
- WebSocket — real-time sync endpoint for Lite clients
- SQL dialect — standard DML/DDL plus engine-specific extensions (`CREATE ARRAY`, `AS OF`, `MATCH`, vector distance functions)

#### Distributed

- vShard partitioning — tenant, collection, and partition-key based routing
- Multi-Raft consensus — linearizable writes per shard group, leader election, log replication, snapshots
- QUIC transport — low-latency inter-node communication via nexar/quinn
- CRDT sync — Loro-backed offline-first replication; AP local merges promoted to CP at Raft commit; declarative conflict policies; dead-letter queue for constraint-violating deltas
- Cross-engine identity — stable `u32` surrogate per row enabling zero-translation cross-engine joins via roaring-bitmap intersection

#### Event Plane

- AFTER triggers — async dispatch with configurable retry and dead-letter queue
- CDC change streams — consumer groups with offset tracking, per-collection routing
- Cron scheduler — SQL-dispatched recurring jobs with 1-second evaluation loop

#### Query & SQL

- Bitemporal queries — system time + valid time on Document, Columnar, Timeseries, Graph, and Array; `AS OF SYSTEM TIME` / `AS OF VALID TIME` SQL syntax
- HTAP bridge — CDC-driven materialized views from strict → columnar; `CONVERT` DDL between storage modes
- Cross-engine queries — vector + graph + spatial + FTS + metadata in a single query against a shared snapshot watermark; RRF fusion
- Row-level security — per-collection RLS policies evaluated at query time
- Multi-tenancy — tenant isolation with quotas and purge

#### Storage & WAL

- Write-Ahead Log — O_DIRECT via io_uring, group commit, AES-256-GCM encryption per segment, hash-chained audit trail
- Storage tiering — L0 in-memory memtables; L1 NVMe via mmap with async prefetch; L2 S3 cold storage (Parquet, HTTP range requests)
- Compression codecs — ALP, FastLanes, FSST, Gorilla, Pcodec, rANS, LZ4 (per-column selection in `nodedb-codec`)
- Memory governance — per-core jemalloc arenas with per-engine budgets and backpressure thresholds

#### Infrastructure

- Three-plane execution model — Tokio Control Plane, Thread-per-Core Data Plane (io_uring), async Event Plane; connected via bounded lock-free SPSC bridges
- Bounded backpressure — SPSC bridge (85%/95% thresholds) and Event Bus (WAL catchup on overflow); no unbounded queues in the hot path
- Encryption — AES-256-GCM at rest (WAL + columnar segments), TLS in transit for all protocols
- Audit log — hash-chained WAL-backed audit trail, Typeguard-based change tracking, SIEM export

---

[0.4.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.4.0
[0.3.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.3.0
[0.2.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.2.0
[0.1.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.1.0
