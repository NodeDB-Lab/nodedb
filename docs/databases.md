# Databases

A **database** is the top-level container in NodeDB—a unit of deployment, collection namespace, quota parent, and the atomic unit for clone, mirror, and backup operations. One database hosts multiple tenants, but a tenant's data never spans databases (use `MOVE TENANT` to relocate). This design enables multi-tenancy within a shared binary while preserving strong isolation boundaries.

## The Default Database

NodeDB reserves `DatabaseId(0)` for the special database named `"default"`. This database:

- Is created automatically on first boot against any storage.
- Cannot be dropped (migration target for administrative operations).
- Can be renamed cosmetically; the numeric identity `DatabaseId(0)` is immutable.
- Becomes the implicit target when no database is specified in a connection.

## Reserved ID Range

Database identifiers in the range `DatabaseId(0..=1023)` are reserved for system use. User-created databases start at `DatabaseId(1024)`. This allows NodeDB to grow future system databases without colliding with application namespaces.

## Lifecycle DDL

### Create a Database

```sql
CREATE DATABASE sales_prod;
CREATE DATABASE staging WITH (priority_class='bulk', max_memory_bytes=1073741824);
```

Options:

- `priority_class` — `'critical'`, `'standard'` (default), or `'bulk'` (affects WAL commit ordering and SPSC scheduling)
- `max_memory_bytes` — initial quota; can be changed later with `ALTER DATABASE`
- `max_storage_bytes`, `max_qps`, `max_connections`, `cache_weight`, `maintenance_cpu_pct` — quota fields (defaults inherited from cluster config)

### Drop a Database

```sql
DROP DATABASE staging;
DROP DATABASE staging CASCADE;      -- drops all collections in it
DROP DATABASE staging FORCE;        -- force-materializes all clones before dropping
```

Restrictions:

- Cannot drop `default` (returns `CANNOT_DROP_DEFAULT_DATABASE`)
- Non-CASCADE drop fails if the database has collections (returns `COLLECTION_EXISTS`)
- FORCE triggers materialization of all databases cloning from this one, blocks until complete, then drops

### Rename a Database

```sql
ALTER DATABASE staging RENAME TO staging_old;
```

The numeric `DatabaseId` remains unchanged; only the name changes in the catalog.

## Connection Binding

A session binds to exactly one database for its lifetime. The binding is immutable and set at connection time via the resolution chain:

1. **Explicit parameter** — `psql -d production`, HTTP `X-NodeDB-Database: production`, native handshake `database` field
2. **User default** — `ALTER USER alice SET DEFAULT DATABASE prod_emea`
3. **Tenant default** — per-tenant fallback (set via admin DDL)
4. **System default** — `"default"`

Once bound, the session is locked to that database. Switching databases requires `USE DATABASE` (or `\c` in psql), which aborts the current transaction, invalidates prepared statements, and rebinds to the new database.

```sql
USE DATABASE staging;
```

### Protocol-Specific Binding

- **pgwire** — `psql -d emp-prod` or `database=emp-prod` in connection string; `\c db_name` switches (rebinds)
- **Native** — `ConnectionBuilder::new(...).database("staging")`
- **HTTP** — `X-NodeDB-Database` header (preferred) or `?database=staging` query param
- **nodedb-cli** — `--database staging` flag or `\c staging` command

## Database Roles

Access to a database is controlled via roles. Three new role types were introduced with database scoping:

- `DatabaseOwner(DatabaseId)` — full control: alter config, materialize clones, backup
- `DatabaseEditor(DatabaseId)` — read and write collections
- `DatabaseReader(DatabaseId)` — read-only access

### Granting Database Access

```sql
GRANT DATABASE_READER ON DATABASE staging TO alice;
GRANT DATABASE_EDITOR ON DATABASE staging TO bob;
GRANT DATABASE_OWNER ON DATABASE sales_prod TO carol;

ALTER USER alice SET DEFAULT DATABASE staging;   -- sets alice's preferred database
```

A user without an explicit grant cannot bind to a database (returns `ACCESS_DENIED` at session bind).

## Cross-Database Access is Forbidden

Accessing a collection in a different database than your bound database returns `COLLECTION_NOT_FOUND` — indistinguishable from a collection that doesn't exist anywhere. This prevents accidental cross-database leaks and simplifies the security model. Applications requiring cross-database operations must open two separate connections.

The only exception: privileged admin DDL (CLONE, MIRROR, MOVE TENANT) can reference multiple databases explicitly.

Isolation is structural, not just a permission check: `database_id` is the outermost key component in every engine's storage — KV, document, graph (CSR and edge store), vector, sparse-vector, spatial, timeseries, columnar, and full-text indexes — as well as the surrogate identity catalog, WAL replication entries, and per-database features like retention policies, continuous aggregates, and alerts. Two databases can hold same-named collections with zero key-space overlap in any engine.

## Quotas

Quotas form a three-tier hierarchy: global (cluster-wide), database (per database), and tenant (per tenant within a database). Each tier has its own budget for memory, storage, queries-per-second, and connections. A request is admitted only if it passes all three tiers.

### Database Quotas

Set database quotas at creation or modify them later:

```sql
CREATE DATABASE analytics WITH (
    max_memory_bytes=5368709120,      -- 5 GB
    max_storage_bytes=10737418240,    -- 10 GB
    max_qps=5000,
    max_connections=500,
    cache_weight=2,
    priority_class='standard',
    maintenance_cpu_pct=50
);

ALTER DATABASE analytics SET QUOTA (
    max_qps=10000,
    max_connections=1000
);
```

Quota fields:

- `max_memory_bytes` — total in-memory budget (L0 cache, memtables, indexes)
- `max_storage_bytes` — total durable storage (WAL, LSM segments, snapshots)
- `max_qps` — queries per second (rate limiter capacity)
- `max_connections` — concurrent connections (semaphore capacity)
- `cache_weight` — share of the global doc cache (relative weight; default 1)
- `priority_class` — `'critical'`, `'standard'`, `'bulk'` (affects WAL group commit order and SPSC scheduling fairness)
- `maintenance_cpu_pct` — max CPU percentage for background tasks like compaction (default 25%)

### Storage Limit Enforcement

NodeDB checks `max_storage_bytes` at write admission, against the cluster total of the database's stored bytes. The total refreshes once per storage sample period.

- A write that can add bytes fails with SQLSTATE `53100` (HTTP 507) when the total is at or over the limit. INSERT, UPSERT, and UPDATE are refused.
- DELETE, TRUNCATE, DROP, purge, compaction, retention, and internal maintenance writes stay allowed, so an over-limit database can shrink.
- A burst can overshoot the limit by one period's writes, because the total lags the stored bytes.
- Before the first complete total after boot, writes pass.
- A tenant `max_storage_bytes` limit applies the same way to the tenant's total over all databases. Each limit is checked on its own.
- To recover, delete data or raise `max_storage_bytes`. Writes resume after the next sample period.

### Tenant Quotas

Tenants can have their own sub-quotas within a database:

```sql
ALTER TENANT acme IN DATABASE prod SET QUOTA (
    max_memory_bytes=1073741824,
    max_qps=500
);

SHOW TENANT QUOTA FOR acme IN DATABASE prod;
```

Constraint: sum of all tenant quotas in a database ≤ the database's quota. Violated adds return `QUOTA_OVERCOMMIT`.

### Viewing Quotas and Usage

```sql
SHOW DATABASE QUOTA FOR analytics;      -- configured limits
SHOW DATABASE USAGE FOR analytics;      -- current consumption (memory, storage, active connections, qps)
SHOW TENANT QUOTA FOR acme IN DATABASE prod;
SHOW TENANT USAGE FOR acme IN DATABASE prod;
```

## Clone

`CLONE DATABASE` creates a copy-on-write (CoW) read-write database from a source, returning in milliseconds regardless of source size. The clone starts with a point-in-time snapshot of the source catalog and delegates reads to source storage up to that point. Writes go to the clone's own storage. The source is unaffected.

### Basic Clone

```sql
CLONE DATABASE preview FROM prod AS OF SYSTEM TIME 1730000000000;
CLONE DATABASE latest FROM prod;                   -- uses prod's current LSN
CLONE DATABASE snapshot FROM prod AS OF LATEST;
```

Uses:

- **Staging**: clone prod for testing DDL changes or risky migrations
- **Point-in-time recovery**: clone at a past timestamp, inspect, then swap databases
- **CI/CD**: per-test-run clone from a baseline

### How Copy-on-Write Works

Initially, the clone's collections carry a `cloned_from` marker pointing to the source collection at a specific LSN. Reads first check the clone's own storage; if a row is not found, they delegate to source storage (capped at the `as_of` LSN). Writes to the clone allocate fresh surrogates in clone storage, leaving source untouched.

A background materializer gradually copies remaining rows from source to clone. Once materialization completes, source delegation stops and the clone is fully independent.

### Bitemporal Correctness

`AS OF SYSTEM TIME` queries on a clone are correct across all time regimes:

- **Query time > as_of**: Returns post-clone writes from clone storage, querying at query time
- **Query time ≤ as_of**: Returns source state at query time (clone did not exist yet)
- **Query time < clone_created_at**: Returns empty (clone predates query point)

This enables realistic point-in-time staging: clone prod as of yesterday, inspect yesterday's state, make corrections, promote.

### Clone Depth Limit

Clones can be nested (clone-of-clone). The lineage forms a tree. Hard limit: `MAX_CLONE_DEPTH = 8`. Exceeding returns `CLONE_DEPTH_EXCEEDED`. Force materialization with `ALTER DATABASE name MATERIALIZE` to flatten the lineage.

```sql
ALTER DATABASE my_clone MATERIALIZE;    -- blocks until all rows copied from source
```

### Orphan Protection

A source database cannot be dropped while any database clones from it. Attempting `DROP DATABASE source` returns `CLONE_DEPENDENCY` with a list of dependent clones. Use `DROP DATABASE source FORCE` to force-materialize all dependents (blocking) before dropping.

### Viewing Lineage

```sql
SHOW DATABASE LINEAGE FOR my_clone;
-- Output: my_clone → staging → prod (ancestor chain)
```

## Mirror

`MIRROR DATABASE` creates a read-only copy of a source database in another cluster. The mirror cluster runs its own Raft groups. An applier on its metadata leader fetches the source's committed log over an mTLS listener, and proposes each entry into the mirror's own groups. `ALTER DATABASE ... PROMOTE` makes the copy writable. It is one-way and permanent.

Uses:

- **Disaster recovery**: a copy in another region. Promote it when the source region fails.
- **Reporting**: analytics on a copy.
- **Testing**: a read-only environment that follows production.

### Config

Each cluster names its peers in `[mirror]`. The source runs the listener and an allowlist. The mirror names its sources.

```toml
# The source cluster
[mirror]
cluster_name = "prod-us"
listen = "0.0.0.0:9500"

[mirror.tls]
cert = "/etc/nodedb/mirror/node.pem"
key = "/etc/nodedb/mirror/node.key"
ca = "/etc/nodedb/mirror/clients-ca.pem"

[mirror.clients.dr-eu]
spki = "<64 hex digits: SHA-256 of the client certificate's SubjectPublicKeyInfo>"
databases = ["orders"]
```

```toml
# The mirror cluster
[mirror]
cluster_name = "dr-eu"

[mirror.sources.prod-us]
seeds = ["10.0.0.1:9500", "10.0.0.2:9500", "10.0.0.3:9500"]
server_name = "nodedb"
tls = { cert = "/etc/nodedb/mirror/client.pem", key = "/etc/nodedb/mirror/client.key", ca = "/etc/nodedb/mirror/source-ca.pem" }
```

- The listener has no insecure mode. It authenticates the client certificate against `[mirror.clients]` before any log read.
- A client reads only the databases its entry names. The source filters its log before an entry leaves.
- List every source replica in `seeds`. A fetch tries the next replica before it reports the log truncated.
- A node with no `listen` serves no mirror. A `listen` needs `cluster_name`, `[mirror.tls]` and at least one `[mirror.clients]` entry.
- Each `spki` holds 64 hex digits, and no two clients share one. Each client names at least one database.
- Each `[mirror.sources]` entry needs at least one seed. The server refuses to start on any of these config errors.

`[tuning.mirror]` on the source:

| Key | Default | Effect |
|---|---|---|
| `cut_interval_ms` | `1000` | Interval between cuts while any link holds a pin |
| `max_pinned_bytes_per_group` | 1 GiB | Log memory one replica of one group holds for pins |
| `max_pin_age_secs` | 86400 | Time a link can go without an ack before its pin expires |
| `sync_timeout_ms` | `10000` | Time a committed write waits for a sync mirror |

### Create a Mirror

```sql
MIRROR DATABASE orders_eu FROM prod-us.orders;
MIRROR DATABASE orders_eu FROM prod-us.orders MODE = sync;
```

The statement requires `Superuser`. It connects to the source and creates the local database with the source's database id. Then it runs in two parts:

1. **Bootstrap.** The source captures the database at one cut. The mirror fetches the capture in chunks, installs the catalog, re-issues every row, and checks the rows against the capture.
2. **Follow.** The source places a cut in every data group each `cut_interval_ms`. The mirror commits the log between two cuts in its own groups, then raises the cut, and every node reads it.

A mirror's nodes hold every value the source issued: surrogates and sequences continue above them after a promotion. A continuous aggregate arrives as state, so its reads match the source at the cut.

### Read Consistency

A mirror answers reads at its newest ready cut.

- Every level returns `STALE_READ_NOT_LEADER` while the mirror is `bootstrapping`, because it holds no cut yet.
- `strong` returns `STALE_READ_NOT_LEADER`. Strong is the default, so a session sets another level.
- `eventual` answers at once once the first cut is ready.
- `bounded_staleness:<secs>` answers while the lag is within the bound. It returns `STALE_READ_NOT_LEADER` before the mirror applied a cut, or when the lag passes the bound.
- A promoted mirror serves every level locally.

```sql
SET default_read_consistency = 'bounded_staleness:5';
SELECT * FROM orders;
```

- A pgwire or native session uses `SET default_read_consistency`.
- An HTTP request uses the body field `read_consistency` on `POST /v1/query` and `POST /v1/query/stream`: `{"sql": "...", "read_consistency": "eventual"}`.
- A WebSocket RPC `query` uses `params.read_consistency`: `{"id": 1, "method": "query", "params": {"sql": "...", "read_consistency": "bounded_staleness:5"}}`.
- `read_consistency` takes `strong`, `eventual` or `bounded_staleness:<secs>`. Any other value is refused.

### Status

```sql
SHOW DATABASE MIRROR STATUS FOR orders_eu;
```

The statement requires a tenant admin. The columns are `name`, `source_cluster`, `source_database`, `source_database_name`, `mode`, `status`, `failure`, `bytes_done`, `bytes_total`, `lag_ms`, `ready_cut`, `applied_cut` and `applied_cut_hlc_ns`.

`failure` holds the failure's message while the status is `failed`, and is empty otherwise. `bytes_done` and `bytes_total` are zero outside `bootstrapping`.

| Status | Meaning |
|---|---|
| `bootstrapping` | The capture is in transfer or install. `bytes_done` and `bytes_total` report it |
| `following` | The mirror applies cuts within normal lag |
| `degraded` | The mirror receives entries, and its lag passed the threshold |
| `disconnected` | The source is unreachable. The mirror serves its last cut, and the lag grows |
| `promoting` | `PROMOTE` fixed the last cut. The database stays read-only until every group applied it |
| `failed` | The link stopped at its last ready cut. `failure` states why. Reads and `PROMOTE` stay available |
| `promoted` | The database is writable and follows nothing |

### Recover a Failed Mirror

```sql
ALTER DATABASE orders_eu MIRROR RESUME;
ALTER DATABASE orders_eu MIRROR REBOOTSTRAP;
```

- Both statements require `Superuser`. Both return SQLSTATE `55000` on a mirror that is not `failed`.
- `RESUME` retries the link from its replicated positions, after an operator fixed the cause. It refuses a failure that leaves no good position, and a mirror that failed before its first cut. Each error names `MIRROR REBOOTSTRAP`.
- `REBOOTSTRAP` removes every object in the database and takes a new capture. The database's grants stay. A read sees no row until the new bootstrap's first cut.

| `failure` | Cause | Action |
|---|---|---|
| `LogTruncated` | The source compacted past the mirror's position, or expired its pin | `REBOOTSTRAP` |
| `SourceTimelineChanged` | The source restored to another log timeline | `REBOOTSTRAP` |
| `BootstrapFailed` | A capture did not install, or its install stopped part way | `REBOOTSTRAP` |
| `BootstrapOutrun` | The source's retention bound passed before the bootstrap ended | `REBOOTSTRAP` |
| `SourceDropped` | The source database was dropped. The mirror keeps its data | `PROMOTE` or `DROP DATABASE` |
| `ApplyError`, `SchemaMismatch`, `PoisonEntry`, `UnsupportedSourceOp` | A source entry did not apply, or the source ran what a mirror cannot follow | Fix the cause, then `RESUME` or `REBOOTSTRAP` as its error says |

### Promote (Irreversible)

```sql
ALTER DATABASE orders_eu PROMOTE;
```

`PROMOTE` fixes the last cut at the newest ready cut. Every group applies that cut, and each row of it stays. The database then takes writes on every node, and its link gives back the source's pin. There is no demote. A later source row never arrives.

`PROMOTE` requires `Superuser`. It also works on a `failed` mirror that applied a cut, and on one whose source is gone. A repeat succeeds and changes nothing.

`PROMOTE` returns SQLSTATE `55000` while the mirror is `bootstrapping`. Wait for the first cut, or run `DROP DATABASE`.

`PROMOTE` returns SQLSTATE `55000` on a mirror that failed before its first cut, because it holds no cut to end at. The error names `MIRROR REBOOTSTRAP`.

`PROMOTE` returns SQLSTATE `0A000` on a database that never was a mirror.

To follow a source again, drop the database and run `MIRROR DATABASE`.

### Sync Mode

`MODE = sync` makes a write on the source wait for its mirror.

- The write commits on the source first, as any write does. Then it waits until the mirror's groups committed a cut that holds it.
- A statement in a transaction block waits at its `COMMIT`. Every other write in the first list below waits at its own answer.
- A write that gets no confirmation within `sync_timeout_ms` returns `MIRROR_SYNC_TIMEOUT`, SQLSTATE `55M01`.

**A `MIRROR_SYNC_TIMEOUT` means the write committed.** Its row is on the source. A client must not retry it as a failed write. There is no silent downgrade to async: every write times out while the mirror stays stopped.

- Latency is at least the time to the next cut, plus the mirror's commit. Sync mode is not recommended cross-region.
- A link joins the wait at its first cut. A write during a bootstrap or a `REBOOTSTRAP` does not wait, because the mirror has no cut to confirm with.
- The wait continues after a pin expires. It ends when the link's row leaves the source's catalog: at the mirror's `PROMOTE`, at its `DROP`, or at twice `max_pin_age_secs` after the last ack. A dropped source database ends it too.
- These writes wait:
  - SQL DML in any form, including `UPSERT` and the `{ }` object forms of `INSERT` and `UPSERT`
  - `GRAPH INSERT EDGE`, `GRAPH DELETE EDGE`, `GRAPH LABEL` and `GRAPH UNLABEL`
  - `COPY ... FROM '<path>'` and `COPY <array> FROM STDIN`
  - `KV_INCR`, `KV_DECR`, `KV_INCR_FLOAT`, `KV_CAS`, `KV_GETSET`, `TRANSFER` and `TRANSFER_ITEM`
  - `crdt_apply`, `CRDT MERGE` and `RESTORE ... SET VERSION`
  - `PUBLISH TO`
  - `CALL`, whatever its body does, because the catalog does not record whether a body writes
  - Every native write op, including `ArrayPutBatch`, `EdgePutBatch`, `EdgeDeleteBatch`, `DocumentBatchInsert` and `VectorBatchInsert`
  - RESP write commands
  - `POST /v1/collections/{name}/crdt/apply` and Prometheus remote write. Each answers HTTP `409` on a timeout, and a remote-write client does not retry a `409`.
- These writes do not wait, because a timeout cannot reach a client that can act on it:
  - ILP: the line protocol sends no reply to any write, on any database, so a client learns nothing from a wait.
  - CRDT sync sessions: a device reads a refused delta as a reason to compensate or retry, while the row stays committed.
  - Event-plane writes, such as trigger bodies: no client waits on them.
- These statements do not wait, because they write no client row:
  - DDL
  - Maintenance statements, such as `REFRESH MATERIALIZED VIEW`, `CONVERT COLLECTION` and `REWRITE PARTITIONS`
  - `RATE_CHECK` and `RATE_RESET`, whose counters are not client rows
  - `COMMIT OFFSET`, because a consumer's position is this cluster's own

### Refusals

`MIRROR DATABASE`, and a link's first exchange, refuse:

| Refusal | SQLSTATE |
|---|---|
| A local name that exists | `42P04` |
| A source name `[mirror.sources]` does not hold, on the mirror | `42704` |
| A source that is this cluster | `0A000` |
| The source's `default` database | `0A000` |
| A source database that is a mirror, unless promoted | `0A000` |
| A database that is an unmaterialized clone, or holds a collection that is one: `ALTER DATABASE ... MATERIALIZE` first | `0A000` |
| A database the client's `[mirror.clients]` entry does not name | `42501` |
| A database the source does not hold | `3D000` |
| A source database id that a local database uses | `42710` |
| A second live mirror of one source database on one cluster | `42710` |
| Another mirror protocol version, or another cluster name | `08004` |
| A source that does not answer before the statement deadline | `08001` |
| Any other source refusal, such as an expired pin | `08006` |

On the mirror before `PROMOTE`:

- Writes and object DDL return `MIRROR_READ_ONLY`, SQLSTATE `25006`, on every protocol.
- `DROP DATABASE`, `ALTER DATABASE`, `PROMOTE`, grants, quotas and consumer offsets stay allowed.
- A mirror cannot be cloned: `CANNOT_CLONE_MIRROR`, SQLSTATE `0A000`. Promote it first.

### Metrics

| Metric | Meaning |
|---|---|
| `nodedb_database_mirror_lag_ms{database}` | Wall-clock now minus the last apply. Zero for other databases and for a promoted mirror |

`SHOW DATABASE MIRROR STATUS` reports the cut numbers. A stopped link files a diagnostic report under `<data_dir>/diagnostics`.

### Retention on a Mirror

A mirror decides no retention. Each decision enters the source's log, and the mirror applies it at the same position.

- A partition drop reaches every copy. The rows stay in a continuous aggregate that folded them.
- An audit purge can come to the mirror with its cutoff lowered, so the mirror keeps the versions a read at its cut still needs. The next decision takes the rest. After `PROMOTE` the mirror's own loop decides, and the retention window runs from then.

### Known Limits

- **Capture memory.** A capture sits in the source replica's memory until the mirror fetched its last chunk, or until it idles out. A capture also stays on the replica that took it. A mirror that moves to another replica starts the transfer over.
- **DDL lease.** A capture holds the DDL lease across its catalog read and its cut, so schema changes on the source wait that long. A capture past the lease's bound lets a DDL through, and the mirror applies that DDL from the tail.
- **Cuts pause shared data groups.** A cut commits and raises as one step. Each mirror group holds a cut's batch while it commits, and a source's barrier orders in each data group. A database's cuts affect every database in the groups they share.
- **Pinned log memory.** A pin keeps its log in memory on every source replica, up to `max_pinned_bytes_per_group`. A pin past a bound expires, and its mirror fails with `LogTruncated`.
- **Current reads can mix two cuts.** A read at the current scope reads each group at what that group applied. Across groups it can see one group at a cut and another at the cut before. A transaction block reads one snapshot, so its reads never mix.
- **Lag has a floor.** Rows become readable at a cut, so a mirror trails by at least `cut_interval_ms`.

## Move Tenant

`MOVE TENANT` relocates a tenant's data from one database to another. Offline in v1 (drains sessions, snapshots data, cuts over atomically).

### Move Sequence

```sql
MOVE TENANT acme FROM prod_us TO prod_eu;
```

Internally:

1. **Pre-flight** — verify target database has compatible schemas for all collections containing tenant data
2. **Drain** — revoke tenant's active sessions on source; reject new writes; wait for in-flight requests with timeout
3. **Snapshot** — backup tenant data to an in-cluster temp location (durable when backup is fsynced)
4. **Cutover** — in a single Raft proposal, drop tenant from source and restore into target (atomic)
5. **Resume** — writes accepted on target

### Idempotent Retry

Moving an already-moved tenant returns `MOVE_TENANT_ALREADY_AT_TARGET`:

```sql
MOVE TENANT acme FROM prod_us TO prod_eu;  -- succeeds, or already-at-target
```

This allows safe retry after network failures without duplicating the move.

## Audit & Change Tracking

### Per-Database Audit Filtering

```sql
SHOW AUDIT IN DATABASE staging [WHERE event_type = 'database_quota_changed'] [LIMIT n];
```

Events tagged with `database_id` include: `DatabaseCreated`, `DatabaseDropped`, `DatabaseRenamed`, `DatabaseQuotaChanged`, `DatabaseCloned`, `DatabaseMirrored`, `DatabasePromoted`, `TenantMoved`, etc.

### DML Audit (Optional)

Enable per-database DML audit to track every row change:

```sql
ALTER DATABASE prod SET AUDIT_DML = 'all';    -- all DML + SELECT
ALTER DATABASE prod SET AUDIT_DML = 'writes'; -- INSERT / UPDATE / DELETE only
ALTER DATABASE prod SET AUDIT_DML = 'none';   -- disabled (default)
```

Audit events include `(database_id, tenant_id, user_id, statement_digest, row_count)`. Cost is the operator's choice; enabled via `AUDIT_DML='all'|'writes'`.

## Idle Timeout & Session Management

### Idle Session Timeout

```sql
ALTER DATABASE analytics SET IDLE_TIMEOUT 1800;  -- 30 minutes
ALTER DATABASE analytics SET IDLE_TIMEOUT 0;     -- disabled
```

Sessions idle longer than the configured time are automatically closed with `SESSION_IDLE_TIMEOUT`. Tracks are monitored per-database; timeout is checked at request boundaries.

Separately, a process-global pgwire listener watchdog force-closes idle connections after `auth.idle_timeout_secs` (default 3600s; `auth.session_absolute_timeout_secs` caps total session age, default disabled). The watchdog never kills a connection mid-statement — the idle window starts when a statement completes — and its teardown reclaims any idle-in-transaction staging overlay. The per-database `SET IDLE_TIMEOUT` and the global watchdog are independent mechanisms; the stricter one wins.

### View and Kill Sessions

```sql
SHOW SESSIONS [IN DATABASE name];
-- Output: session_id, user, database, tenant, idle_seconds, bytes_in, bytes_out, current_statement_digest

KILL SESSION 'sid_abc123';
```

`KILL SESSION` requires `Superuser` or `DatabaseOwner` of the session's database. Idle sessions are force-closed; active sessions are marked for revocation and close at the next request boundary.

To end every session of one user, see `KILL USER SESSIONS` in [Session Management](security/sessions.md#killing-every-session-of-a-user).

## Per-Database Metrics

NodeDB exposes per-database metrics for observability:

```
nodedb_database_qps_total{database="prod"}
nodedb_database_errors_total{database="prod"}
nodedb_database_collections_total{database="prod"}
nodedb_database_tenants_total{database="prod"}
nodedb_database_memory_bytes{database="prod"}
nodedb_database_storage_bytes{database="prod"}
nodedb_database_connections{database="prod"}
nodedb_database_bridge_queue_depth{database="prod"}
nodedb_database_mirror_lag_ms{database="prod_eu"}  -- if mirrored
nodedb_database_wal_commit_latency_p99_us{database="prod"}
nodedb_database_maintenance_lease_seconds_total{database="prod"}
```

Tenant request counts are labeled by tenant id and database. The series counts requests admitted in that database:

```
nodedb_tenant_qps_total{tenant_id="7", database="prod"}
```

Other tenant metrics cover the tenant across all databases, so they carry `tenant_id` only:

```
nodedb_tenant_memory_used_bytes{tenant_id="7"}
```

Metrics are updated continuously and exposed via `/metrics` on the HTTP port.

## Admin DDL Gating Matrix

Database operations are gated by role:

| DDL Operation                  | Required Role                  |
| ------------------------------ | ------------------------------ |
| `CREATE DATABASE`              | `ClusterAdmin` or `Superuser`  |
| `DROP DATABASE` (non-default)  | `Superuser`                    |
| `DROP DATABASE … FORCE`        | `Superuser`                    |
| `ALTER DATABASE … RENAME`      | `DatabaseOwner` or `Superuser` |
| `ALTER DATABASE … SET QUOTA`   | `ClusterAdmin` or `Superuser`  |
| `ALTER DATABASE … MATERIALIZE` | `DatabaseOwner` or higher      |
| `ALTER DATABASE … PROMOTE`     | `Superuser`                    |
| `CLONE DATABASE`               | `Superuser`                    |
| `MIRROR DATABASE`              | `Superuser`                    |
| `MOVE TENANT`                  | `Superuser`                    |
| `BACKUP DATABASE`              | `DatabaseOwner` or `Superuser` |
| `RESTORE DATABASE`             | `DatabaseOwner` or `Superuser`; `Superuser` when the database does not exist |

See [Roles & Permissions](security/rbac.md) for full role definitions.

## Error Codes Quick Reference

| Error Code                      | Cause                                                  | Mitigation                                               |
| ------------------------------- | ------------------------------------------------------ | -------------------------------------------------------- |
| `DATABASE_NOT_FOUND`            | Database does not exist                                | Check database name; list with `SHOW DATABASES`          |
| `ACCESS_DENIED`                 | User lacks grant for the database                      | Ask admin to `GRANT DATABASE_READER ON DATABASE`         |
| `COLLECTION_NOT_FOUND`          | Collection in another database, or truly absent        | Open a second connection to the other database if needed |
| `MIRROR_READ_ONLY`              | Writing to a pre-promotion mirror                      | Promote the mirror first, or write to source             |
| `STALE_READ_NOT_LEADER`         | `Strong` consistency requested on non-leader mirror    | Use `BoundedStaleness` or read from source               |
| `MIRROR_SYNC_TIMEOUT`           | The write committed, and a sync mirror did not confirm it | Do not retry the write as failed; check the mirror    |
| `CLONE_DEPTH_EXCEEDED`          | Clone tree exceeds 8 levels                            | Materialize clones to flatten the lineage                |
| `CANNOT_CLONE_MIRROR`           | Attempting to clone from a mirror before promotion     | Promote the mirror first                                 |
| `CLONE_DEPENDENCY`              | Cannot drop source while clones depend on it           | Use `DROP DATABASE source FORCE` or drop clones first    |
| `MOVE_TENANT_ALREADY_AT_TARGET` | Tenant already in target database (safe retry)         | No action needed; operation succeeded                    |
| `*_QUOTA_EXCEEDED`              | Memory, storage, QPS, connections, or budget limit hit | Increase quota or reduce load                            |
| `QUOTA_OVERCOMMIT`              | Sum of tenant quotas exceeds database quota            | Reduce or rebalance tenant quotas                        |

## See Also

- [Roles & Permissions](security/rbac.md) — full GRANT/REVOKE reference
- [Audit & Change Tracking](security/audit.md) — audit log format and filtering
- [Multi-Tenancy](security/tenants.md) — tenant isolation and lifetime management
- [Architecture](architecture.md) — how databases interact with the execution model
