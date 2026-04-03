# Security

NodeDB has a defense-in-depth security model covering authentication, authorization, encryption, and audit. This overview covers the main features — full configuration details will be in the API documentation.

## Authentication

Multiple auth methods, usable together:

- **JWKS auto-discovery** — Multi-provider support (Auth0, Clerk, Supabase, Firebase, Keycloak, Cognito). ES256/ES384/RS256 algorithms. Built-in cache with disk fallback and circuit breaker for provider outages.
- **mTLS** — Mutual TLS with certificate revocation list (CRL) support
- **API keys** — For service-to-service communication
- **SCRAM-SHA-256** — Password-based auth compatible with PostgreSQL clients

## Authorization (RBAC + RLS)

**Role-Based Access Control** — Define roles with fine-grained permissions on collections, fields, and operations.

**Row-Level Security** — Policies filter data based on the authenticated user's context. RLS predicates use `$auth.*` session variables (user ID, roles, org, scopes) and are substituted at query plan time — they apply across all seven engines.

```sql
-- Only show users their own data
DEFINE RLS ON orders WHERE customer_id = $auth.user_id;

-- Org-scoped access
DEFINE RLS ON projects WHERE org_id = $auth.org_id;

-- Debug: see the full permission resolution chain
EXPLAIN PERMISSION SELECT ON orders FOR CURRENT USER;
```

### Function and Procedure Permissions

User-defined functions and stored procedures require an explicit `EXECUTE` grant before non-owner roles can call them.

```sql
-- Grant execute permission on a function
GRANT EXECUTE ON FUNCTION full_name TO analyst;

-- Grant execute permission on a procedure
GRANT EXECUTE ON PROCEDURE transfer_funds TO accountant;

-- Revoke
REVOKE EXECUTE ON FUNCTION full_name FROM analyst;
```

Functions created with `SECURITY DEFINER` execute with the permissions of the function owner, not the caller. Use this only when the privilege escalation is intentional and the function body is fully trusted.

### Tenant Backup and Restore

`BACKUP TENANT` and `RESTORE TENANT` require the `BACKUP` privilege on the target tenant. Backups are encrypted with AES-256-GCM using the tenant's WAL key — restoring a backup to a different tenant requires re-keying. Backups cover all 7 engines: documents, indexes, vectors, graph edges, KV tables, timeseries, and CRDT state.

```sql
-- Grant backup privilege
GRANT BACKUP ON TENANT acme TO ops_user;

-- Validate a backup before restoring (no data written)
RESTORE TENANT acme FROM '/backups/acme.bak' DRY RUN;
```

### Tenant Data Purge

`PURGE TENANT` permanently deletes all data for a tenant across every engine and cache. Requires explicit `CONFIRM` to prevent accidents. Superuser-only.

```sql
-- Remove catalog metadata only (data remains on disk)
DROP TENANT acme;

-- Remove ALL data across all engines and caches (GDPR erasure)
PURGE TENANT acme CONFIRM;
```

Purge is idempotent — safe to re-run after a crash. WAL records are retained (WAL is append-only) but are inert after purge.

### Tenant Usage and Quotas

```sql
-- Set resource limits per tenant
ALTER TENANT acme SET QUOTA max_qps = 5000;
ALTER TENANT acme SET QUOTA max_storage_bytes = 53687091200;
ALTER TENANT acme SET QUOTA max_connections = 50;

-- Inspect current usage vs limits
SHOW TENANT USAGE FOR acme;
SHOW TENANT QUOTA FOR acme;

-- Export usage data for billing integration
EXPORT USAGE FOR TENANT acme PERIOD '2026-03' FORMAT 'json';
```

## Scopes and Organizations

- **Scopes** — Define and grant fine-grained permissions with time-bound expiry and grace periods
- **Organizations** — Membership management with JIT provisioning from JWT claims. Tenant isolation.
- **Impersonation & delegation** — Time-limited, scope-subset impersonation for support workflows

## Rate Limiting and Metering

- **Rate limiting** — Token bucket algorithm with per-user, per-org, and per-API-key hierarchy and tier resolution
- **Usage metering** — Per-operation cost tracking with quotas (hard/soft/throttle enforcement)
- **Tenant ceilings** — Hard resource limits per tenant

## Security Controls

- **Blacklists** — Block users and IP ranges (CIDR) with TTL. Kill active sessions on blacklist.
- **Conditional permissions** — Permissions gated on temporal windows, MFA status, IP range, or device trust level
- **Risk scoring** — Signal combination with adaptive thresholds for step-up auth
- **Emergency lockdown** — Two-party authorization with break-glass key for incident response
- **Column-level redaction** — Mask or hash-pseudonymize sensitive fields per role

## Encryption

- **At rest** — AES-256-XTS for data volumes, AES-256-GCM for WAL segments, per-file data encryption keys
- **In transit** — TLS for all protocols (pgwire, HTTP, WebSocket, native)
- **Lite devices** — AES-256-GCM + Argon2id key derivation for on-device encryption

## Audit

- **Hash-chain audit log** — Tamper-evident append-only log of all auth and access events
- **SIEM export** — CDC-based webhook export with HMAC signature verification
- **Auth observability** — Prometheus metrics for auth events, anomaly detection

[Back to docs](README.md)
