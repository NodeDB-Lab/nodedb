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

`MIRROR DATABASE` creates a continuously-updated read-only replica of a source database in a different region or cluster. The mirror is a Raft observer of the source: it applies the source's log entries under its own catalog but does not participate in quorum. Promotion to writable is one-way and permanent.

### Basic Mirror

```sql
MIRROR DATABASE prod_eu FROM prod_us MODE = async;
MIRROR DATABASE prod_eu FROM prod_us MODE = sync;
```

Modes:

- `async` — mirror trails source asynchronously; lag is observable via metrics
- `sync` — source waits for mirror acknowledgment before commit (latency cost; not recommended cross-region)

Uses:

- **Disaster recovery**: read-only replica in a different region; promote on regional failure
- **Reporting**: offload analytics to a replica
- **Testing**: read-only staging environment that auto-updates with production data

### Read Consistency

- `BoundedStaleness` — served at mirror's `last_applied` LSN
- `Strong` — returns `STALE_READ_NOT_LEADER` with source endpoint hint
- `Eventual` — served immediately

```sql
SELECT * FROM orders CONSISTENCY='bounded_staleness';  -- served from mirror
```

### Bootstrap and Lag

Bootstrap transfers a snapshot from source to mirror over QUIC, then log-streams changes. Lag is observable:

```sql
SHOW DATABASE MIRROR STATUS FOR prod_eu;
-- Output: status=Following, lag_ms=150, last_applied=12345, mode=async
```

### Promotion (Irreversible)

```sql
ALTER DATABASE prod_eu PROMOTE;
```

Promotion:

- Stops observing the source
- Becomes a normal Raft group with its own leader election
- Starts accepting writes
- Is one-way and permanent (no DEMOTE)

To re-mirror after promotion: drop the database and `CLONE DATABASE` from source.

### Constraints

- A mirror cannot be cloned (rejected with `CANNOT_CLONE_MIRROR`); promote first if needed
- A clone can be mirrored
- Pre-promotion writes return `MIRROR_READ_ONLY`

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
SHOW AUDIT IN DATABASE staging [WHERE event_type = 'database_quota_changed'];
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

## Per-Database Metrics

NodeDB exposes per-database metrics for observability:

```
nodedb_database_qps{database="prod"}
nodedb_database_memory_bytes{database="prod"}
nodedb_database_storage_bytes{database="prod"}
nodedb_database_connections{database="prod"}
nodedb_database_mirror_lag_ms{database="prod_eu"}  -- if mirrored
nodedb_database_wal_commit_latency_p99{database="prod"}
nodedb_database_maintenance_cpu_seconds{database="prod"}
```

Similarly, tenant metrics are labeled by both database and tenant:

```
nodedb_tenant_qps{database="prod", tenant="acme"}
nodedb_tenant_memory_bytes{database="prod", tenant="acme"}
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
| `BACKUP DATABASE`              | `DatabaseOwner` or higher      |
| `RESTORE DATABASE`             | `Superuser`                    |

See [Roles & Permissions](security/rbac.md) for full role definitions.

## Error Codes Quick Reference

| Error Code                      | Cause                                                  | Mitigation                                               |
| ------------------------------- | ------------------------------------------------------ | -------------------------------------------------------- |
| `DATABASE_NOT_FOUND`            | Database does not exist                                | Check database name; list with `SHOW DATABASES`          |
| `ACCESS_DENIED`                 | User lacks grant for the database                      | Ask admin to `GRANT DATABASE_READER ON DATABASE`         |
| `COLLECTION_NOT_FOUND`          | Collection in another database, or truly absent        | Open a second connection to the other database if needed |
| `MIRROR_READ_ONLY`              | Writing to a pre-promotion mirror                      | Promote the mirror first, or write to source             |
| `STALE_READ_NOT_LEADER`         | `Strong` consistency requested on non-leader mirror    | Use `BoundedStaleness` or read from source               |
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
