# Multi-Tenancy

Each tenant has fully isolated storage, indexes, and security policies. Cross-tenant data access is impossible by design.

## Database vs Tenant

These are distinct concepts:

| Concept      | Scope                                      | Usage                                                                                               |
| ------------ | ------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| **Database** | Deployment unit; namespace for collections | Multi-database deployments (prod, staging, analytics); clone/mirror/backup unit; quota/audit parent |
| **Tenant**   | Row-level scoping within a database        | Multi-tenant SaaS; billing/usage isolation; logical separation within a namespace                   |

One database hosts **many tenants**. A tenant's data **does not span databases** — use `MOVE TENANT` to reassign a tenant to a different database. Cross-database queries are forbidden.

**Example: SaaS with three customers**

```sql
-- Database: deployment unit (one per region/cluster)
CREATE DATABASE us_west;

-- Tenants: customers in that database
CREATE TENANT acme_corp;
CREATE TENANT bigcorp_inc;
CREATE TENANT startup_xyz;

-- RLS filters rows by tenant_id within each collection
CREATE RLS POLICY tenant_isolation ON orders FOR ALL
    USING (tenant_id = $auth.tenant_id);

-- Audit is per-database; RLS is per-tenant
ALTER DATABASE us_west SET AUDIT_DML = 'writes';
SHOW AUDIT IN DATABASE us_west WHERE event_type = 'dml_audit';
```

## Creating Tenants

```sql
-- Superuser only
CREATE TENANT acme;
```

## Quotas

```sql
-- Set resource limits
ALTER TENANT acme SET QUOTA max_qps = 5000;
ALTER TENANT acme SET QUOTA max_storage_bytes = 53687091200;  -- 50 GB
ALTER TENANT acme SET QUOTA max_connections = 50;

-- Inspect
SHOW TENANT USAGE FOR acme;
SHOW TENANT QUOTA FOR acme;

-- Export for billing
EXPORT USAGE FOR TENANT acme PERIOD '2026-03' FORMAT 'json';
```

`max_storage_bytes` is enforced at write admission against the tenant's cluster total over all databases. The total refreshes every storage sample period, so a burst can overshoot by one period's writes. Writes pass until the first complete total exists after boot. A write that can add bytes then fails with SQLSTATE `53100` once the total reaches the limit. Deletes, TRUNCATE, DROP, and maintenance writes stay allowed. See [Databases](../databases.md#storage-limit-enforcement).

## Creating Users for a Tenant

```sql
-- Superuser creates a user scoped to a tenant
CREATE USER alice WITH PASSWORD 'secret' ROLE readwrite TENANT 42;
```

## Tenant Backup/Restore

Backup bytes flow over the pgwire COPY framing. The client redirects
output to (or reads input from) a file under the operator's UID; the
database never touches a caller-named filesystem path.

```sql
-- Grant backup permission
GRANT BACKUP ON TENANT acme TO ops_user;

-- Backup: bytes stream to STDOUT over the wire.
COPY (BACKUP TENANT acme) TO STDOUT;

-- Validate a backup blob before restoring.
COPY tenant_restore(acme) FROM STDIN DRY RUN;

-- Restore.
COPY tenant_restore(acme) FROM STDIN;
```

Backups cover all 7 engines: documents, indexes, vectors, graph edges, KV tables, timeseries, and CRDT state. Payloads are encrypted with AES-256-GCM under the tenant WAL key.

Each backup records a row count and a digest per collection and engine. A restore checks them twice:

- Before its first write, against the backup's own rows. A mismatch refuses the restore, and nothing changes.
- After its last write, against a capture of the destination. A mismatch fails the restore with an error that names every mismatched collection. The restore does not roll back: the restored data stays in place for inspection.

`DRY RUN` runs the first check only.

## Database Backup/Restore

`BACKUP DATABASE` writes every tenant's rows in one database to an object-store URI. `RESTORE DATABASE` reads it back through the tenant restore, with the same two checks.

```sql
BACKUP DATABASE shop TO 's3://backups/shop/nightly.ndbb';
RESTORE DATABASE shop FROM 's3://backups/shop/nightly.ndbb' DRY RUN;
RESTORE DATABASE shop FROM 'file:///srv/nodedb/backups/shop.ndbb' FORCE;
```

- `s3://<bucket>/<key>` uses the `[backup_storage]` endpoint, region and keys. Empty keys use IAM credentials.
- `file:///<path>` must lie inside `[backup_storage] local_root`, with every symlink on the path followed. A symlink that leaves the root refuses the URI. Without `local_root`, every `file://` URI is refused.
- The server reads, writes, lists and deletes a `file://` object below a directory handle of the root and follows no symlink on the way. On Linux it opens through `openat2` with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS`. A component swapped for a symlink after the URI resolved refuses the read or write with SQLSTATE `22023`. Scheduled-backup retention never lists a symlink under the target and never deletes through one.
- The backup takes one cut for every tenant of the database and records it in the manifest. Each Raft data group captures the database's tenants when it applies the cut's barrier entry. The capture holds every entry at or below the barrier and none above it, so no row written after the cut is in the backup. A backup whose group changed leader before the capture was collected fails with a retryable error that names the group. On a server with no Raft groups the cut is the instant the backup dispatches its snapshots to the Data Plane cores. Every write, whatever path sends it, reaches a core through one dispatcher, so the backup holds every write dispatched before the cut and none after it. Each core runs the backup's snapshot as a barrier: it sees every request dispatched to that core before it, in any priority tier, and none after it. A write's restore-staleness mark, a RESTORE's included, lands on the same side of the cut as the write.
- Each tenant's backup quota admits and meters the backup and the restore, as for `COPY`.
- A restore also counts against the write quota of every collection it restores, under the name DML charges. A hard write cap on one collection refuses the restore before any write. A grant on `*` is charged once per restored row. A grant on the `tenant:<id>` marker is charged the tenant's restored rows. A `DRY RUN` charges no write quota.
- `DEFINE SCOPE '<name>' AS BACKUP ON 'tenant:<id>'` defines a backup scope for one tenant.
- A malformed URI, an unknown scheme, or a path outside `local_root` fails with SQLSTATE `22023` before any store is touched.
- A backup restores only under its own database name.
- Credentials never come from the SQL text.

```toml
[backup_storage]
local_root = "/srv/nodedb/backups"
endpoint = ""
access_key = ""
secret_key = ""
region = "us-east-1"
```

## Tenant Purge (GDPR Erasure)

```sql
-- Remove catalog metadata only (data remains on disk until compaction)
DROP TENANT acme;

-- Remove ALL data across all engines and caches (permanent)
PURGE TENANT acme CONFIRM;
```

`PURGE` is idempotent and safe to re-run after a crash. WAL records are retained (append-only) but are inert after purge.

## Isolation Model

| Layer   | Isolation                                               |
| ------- | ------------------------------------------------------- |
| Storage | Separate key prefixes per tenant in redb                |
| Indexes | Tenant-scoped — no cross-tenant index overlap           |
| WAL     | Per-tenant segments with per-tenant encryption keys     |
| Queries | Tenant ID injected at plan time, enforced in Data Plane |
| RLS     | Policies scoped to tenant                               |
| Audit   | Per-tenant audit entries with tenant_id field           |

[Back to security](README.md)
