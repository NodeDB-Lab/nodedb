# Changelog

All notable changes to NodeDB will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
NodeDB uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.6.2] - 2026-10-16

### Fixed

- **Mobile builds:** the shared engine crates linked jemalloc, so iOS and Android builds failed to link. jemalloc is opt-in through the `nodedb-mem` `jemalloc` feature, which the server enables.

---

## [0.6.1] - 2026-10-15

### Fixed

- **Vector index:** search found nothing after the last vector was deleted, and compaction could hide live vectors.
- **Percentiles:** `approx_percentile` and percentile aggregates over fewer than about 200 values returned a wrong value.
- **Full-text indexing:** writes slowed quadratically as a common term's posting list grew.

---

## [0.6.0] - 2026-10-15

A correctness release. Statements that returned a plausible wrong answer now return the right one or a typed error, and cluster recovery, restore and Calvin transactions survive crashes and leader moves.

### ⚠️ Breaking changes

- **A 0.5.0 data directory cannot be opened by 0.6.0.** The WAL format, row identity, time encoding, full-text keys and columnar footers changed. Dump and reload.
- **Sync and native protocols changed.** Upgrade Lite and `nodedb-client` with the server.
- **SQL refuses what it used to accept silently:**
  - an unknown or ambiguous column returns `42703` / `42702` at plan time;
  - a NULL `PRIMARY KEY` returns `23502`, and a duplicate key returns `23505`;
  - a value outside a declared `DECIMAL(p,s)`, `SMALLINT`, `INT` or `REAL` width is refused;
  - a narrowing `ALTER COLUMN TYPE` is refused;
  - `ORDER BY … USING <operator>` is refused;
  - `MERGE … UPDATE SET *`, `INSERT *` and a `WHERE` or `DELETE WHERE` clause on a `MERGE` update are refused.
- **Some queries return different results:**
  - `time_bucket` returns an ISO-8601 instant;
  - `stddev` and `variance` use sample semantics, as in PostgreSQL;
  - KV sorted indexes order numbers by value;
  - aggregates inside a transaction count its own writes.
- **Typed errors keep their SQLSTATE across nodes** instead of collapsing to `XX000`. A full dispatch queue returns `57P03`, and HTTP returns `503` with `Retry-After`.
- **Multi-row `INSERT` and `UPSERT` are atomic** on KV, document and vector-primary collections. A row that fails writes nothing.
- **Config:**
  - an invalid `NODEDB_*` value aborts startup;
  - `${VAR}` in the TOML file expands;
  - `[server] single_node_calvin`, `quic_keep_alive_secs` and `quic_idle_timeout_secs` are removed;
  - `rpc_timeout_secs` has a 500 ms floor.
- **The minimum Rust version is 1.96.**
- **`nodedb-bridge`:** `AsyncControlHandle` has no public `inner` field. Use `try_send_request` for a non-blocking send.

### Added

- **Backup and restore:**
  - per-database consistent-cut backups, scheduled and verified, to local or remote storage;
  - selective restore by database;
  - point-in-time restore with `nodedb restore`.
- **Leader-lease linearizable reads.**
- **Sequences:** `nextval`, `currval`, `setval` and `SERIAL`, evaluated per row.
- **SQL:**
  - `EXISTS` / `NOT EXISTS`;
  - expressions over derived tables, CTEs and derived tables as join sides;
  - `CROSS JOIN`;
  - `FILTER (WHERE …)`, `HAVING` and grouping sets on timeseries aggregates.
- **SQL write forms:**
  - `RETURNING` on KV `UPDATE` / `DELETE`;
  - `CREATE [UNIQUE] INDEX IF NOT EXISTS`;
  - one real command tag per DML statement on every engine and protocol.
- **Continuous aggregates are queryable relations.** `REFRESH CONTINUOUS AGGREGATE` runs on any node.
- **`engine='vector'`** creates a vector-primary collection.
- **`[tuning.startup]`** sets `raft_ready_timeout_ms` (default 30 s) and `data_group_recovery_timeout_ms` (default 60 s). A boot wait fails only when it makes no progress for that long.
- **`[server] sync_host` / `NODEDB_SYNC_HOST`** sets the sync listener's bind address.
- **Japanese and Korean full-text segmentation** reads a lindera dictionary from `NODEDB_FTS_IPADIC_DIR` / `NODEDB_FTS_KO_DIC_DIR`. Without one, CJK text uses character bigrams.
- **`MERGE … WHEN MATCHED THEN DO NOTHING`.**

### Fixed

- **Restore and recovery:** restore dropped committed rows, graph edges and arrays. A crash during snapshot install left a half-installed group.
- **Cross-shard Calvin transactions** lost state on a leader move or restart.
- **Cluster writes:**
  - a write inside a transaction on a non-owning coordinator survived `ROLLBACK`;
  - `MERGE`, `UPDATE … FROM` and `INSERT … SELECT` wrote to non-owning nodes.
- **`INSERT … SELECT`, `MERGE` and `UPDATE … FROM`** stopped silently at 10,000 rows.
- **Typeguards** were skipped on some write paths.
- **Query results:**
  - joins and aggregates bypassed row-level security;
  - `TRUNCATE` left KV, columnar, timeseries, spatial and vector-primary rows in place;
  - unknown identifiers returned NULL rows.
- **Time keys** used mixed units, so cross-node joins dropped every row.
- **Server crashes:**
  - `CREATE VECTOR INDEX` and `CREATE SORTED INDEX` crashed the server with a stack overflow;
  - JWT bearer authentication over HTTP panicked the handler.
- **The published container image** could not start.
- **Write cost:** a multi-row write paid one fsync per row.

### Security

- `rustls` 0.23.46: RUSTSEC-2026-0285, RUSTSEC-2026-0346, RUSTSEC-2026-0347, RUSTSEC-2026-0348, RUSTSEC-2026-0349.
- Wasmtime 49.0.3: RUSTSEC-2026-0315, RUSTSEC-2026-0316, RUSTSEC-2026-0353 to RUSTSEC-2026-0364.
- `unsafe-libyaml` is no longer in the dependency tree: RUSTSEC-2024-0449.
- `quinn-proto` 0.11.19 and `ruzstd` 0.9.1: RUSTSEC-2026-0338, RUSTSEC-2026-0340, RUSTSEC-2026-0343, RUSTSEC-2026-0344.

---

## [0.5.0] - 2026-08-04

A security and durability release. It closes silent-failure paths in the WAL, replay, checkpoint, and sync layers, enforces authorization uniformly across transports, and makes expression errors, declared column widths, and affected-row counts report the truth instead of a plausible-looking wrong answer.

### ⚠️ Breaking changes

- **A 0.4.0 data directory cannot be opened by 0.5.0** — a dump/reload is required. The catalog store moved to redb 4, whose format refuses older files, and the at-rest encryption envelope was rebuilt around single-use keys.
- **The WAL opens with `O_DIRECT` by default.** A filesystem that rejects direct I/O now fails startup instead of silently running buffered; opt out with `NODEDB_WAL_DIRECT_IO=false`.
- **ILP ingest requires authentication** before line-protocol bytes are accepted.
- **`GRAPH TRAVERSE` / `PATH` / `NEIGHBORS` require `IN '<collection>'`**, and are denied on collections carrying a row policy. CSR snapshot format bumped to v3; graph checkpoints rebuild.
- **Division by zero raises SQLSTATE `22012` and unregistered functions raise `42883`**, instead of folding to NULL.
- **Integer and float values outside a declared column width are rejected on write.**
- **Positional `INSERT` / `UPSERT` binds to the declared column order**; excess values are rejected, and positional insert into a KV collection is refused.
- **Affected-row counts report the real count** — no-op writes now return 0, not 1.
- **Sync wire protocol changed** — new `RowPush` message, acks carry an outcome enum, and CRDT peer ids are per-collection and bound to their first producer. Upgrade Lite clients with the server.
- **`nodedb-wal`** — readers and replay functions take a key ring and return plaintext; `GroupCommitter` is removed in favor of a shared writer lock; the double-write-buffer slot format changed.
- **`nodedb-client`** — `PoolConfig::default` removed, `ConnectionBuilder::build` is fallible, and trust auth no longer defaults to `admin`.
- **Triggers reject `SECURITY INVOKER`**, and `Monitor` no longer grants read access.
- **HTTP and sync listeners bind at boot**, so a port conflict is fatal. `cluster.insecure_transport` now requires a loopback or private bind address.
- **New settings** — `ports.sync` (9090), `NODEDB_WAL_DIRECT_IO`, `server.native_crash_dumps`, `auth.jwt.max_token_lifetime_secs`.
- **Dependency majors** — redb 2→4, rand 0.9→0.10, arrow/parquet 58→59, wasmtime 45→47, reqwest 0.12→0.13, prost 0.13→0.14, toml 0.8→1.

### Added

- **Structured failure reports** — fsync'd diagnostics filed where a fault is detected: WAL corruption, lost durability, dropped events, and wedged appliers. Optional `native_crash_dumps` captures a minidump on native faults.
- **Torn-tail and segment-continuity verification** in WAL replay, plus a fail-point framework for crash-injection tests.
- **Unified index registry** — `DROP INDEX` resolves and tears down any index kind uniformly.
- **pgwire** — binary-format parameters including `NUMERIC` / `TIMESTAMP` / `TIMESTAMPTZ`, and `$N` type inference from both SQL text and the catalog.
- **SQL** — `SEARCH ... USING VECTOR` in subquery position; post-aggregate `ORDER BY` with `NULLS FIRST` / `LAST`; float and short-integer column types; declared `TIME_KEY` honored instead of inferred.
- **CRDT** — peer-id rekey, per-row bounded export, and local snapshot imports admitted without untrusted-peer ceilings.

### Fixed

- Replay no longer returns a silently truncated suffix below the retained floor; checkpoint truncation is bounded by consumer watermarks, and a failed apply aborts the boot instead of leaving a hole.
- A failed fsync stops the writer instead of acknowledging data that no longer exists; segment rollover can no longer wedge the WAL; resume no longer reuses LSNs; replay decrypts before decoding.
- Re-delivered Raft entries are no longer applied twice.
- Full-text search retracts postings for terms an update removed, and a failed index update rejects the write.
- Descriptor mutations replicate, so the metadata applier no longer wedges while health stays green.
- ILP no longer stalls during replicated DDL, and reports lines lost on disconnect.
- `DROP COLLECTION` reclaims per-core vector and spatial checkpoints.
- Timeseries series-id collisions no longer merge unrelated series.
- CRDT applies refuse causally-pending imports instead of reporting success; retryable sync refusals are no longer acked as terminal.
- Join `LIMIT` applies after post-join `WHERE`; same-named join columns no longer collapse into one value; outer `ORDER BY` / `OFFSET` / `DISTINCT` / `LIMIT` apply over subquery and CTE bodies; search hits return user primary keys.
- Vector segment backings are validated at attach, and export failures are reported instead of writing truncated vectors.

### Security

- Authentication and authorization are enforced uniformly across every transport and engine. Internal paths that could reach storage without the checks their public equivalents applied now route through a single authorization and row-policy chokepoint, with CI gates that fail the build if a new path bypasses it.
- Untrusted input is bounded at every decode boundary, and objects read from external storage are authenticated before use.
- Identity and scope are bound at their source: ingest connections, external identity authorities, policy administration, and unauthenticated cluster transport.
- Static analysis, secret scanning, and continuous fuzzing of the decoder surface run in CI, alongside a documented threat model.

### Removed

- `GroupCommitter` from `nodedb-wal`, the columnar segment-rewrite protocol and its WAL record, and `cargo-vet`.

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
- **Native protocol transactions** — a row committed inside an explicit `Begin`/`Commit` over the native protocol is now visible to PK point lookups and filtered aggregates, not just full scans; the commit batch was routed to vShard 0 instead of the collection's owning vShard, and the gateway router now rejects unroutable commit meta-ops instead of silently misrouting them (#193).
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

[0.6.2]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.6.2
[0.6.1]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.6.1
[0.6.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.6.0
[0.5.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.5.0
[0.4.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.4.0
[0.3.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.3.0
[0.2.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.2.0
[0.1.0]: https://github.com/NodeDB-Lab/nodedb/releases/tag/v0.1.0
