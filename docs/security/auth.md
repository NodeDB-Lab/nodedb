# Authentication

NodeDB supports multiple authentication methods, usable together.

## Password Auth (SCRAM-SHA-256)

Compatible with any PostgreSQL client (`psql`, `pgcli`, application drivers).

```sql
-- Create a user with password
CREATE USER alice WITH PASSWORD 'strong_password_here';

-- Create with a specific role
CREATE USER bob WITH PASSWORD 'secret' ROLE readonly;

-- Create for a specific tenant (superuser only)
CREATE USER service_bot WITH PASSWORD 'key' ROLE readwrite TENANT 42;

-- View all users
SHOW USERS;
```

**Roles:** `readonly`, `readwrite`, `tenant_admin`, `superuser`, `cluster_admin`, `monitor`

Connect via psql:

```bash
psql -h localhost -p 6432 -U alice
```

## API Keys

For service-to-service communication without passwords.

```sql
-- Create an API key for a user (returns the key once — store it securely)
CREATE API KEY FOR alice;

-- Create a key scoped to specific databases
CREATE API KEY FOR service_account WITH DATABASES (analytics, logs);

-- Revoke
REVOKE API KEY <key_id>;

-- List API keys
SHOW API KEYS [FOR <user>];
```

**Database scoping:**

```sql
-- Create a key accessible only to the 'prod' database
CREATE API KEY FOR alice WITH DATABASES (prod);

-- Attempt to use key on a different database is rejected
-- Error: DATABASE_NOT_AUTHORIZED
```

API keys default to the owner's accessible databases. Narrow with `WITH DATABASES (db1, db2)` to restrict to a subset.

Use in HTTP requests:

```bash
curl -H "Authorization: Bearer <api-key>" http://localhost:6480/v1/query
```

## Service Accounts

Credentials for daemons, batch jobs, and application backends. Service accounts are scoped to a database.

```sql
-- Create a service account for a database
CREATE SERVICE ACCOUNT 'batch-processor' FOR DATABASE analytics;

-- Create an API key on the service account
CREATE API KEY FOR 'batch-processor' WITH DATABASES (analytics);

-- Adjust accessible databases
ALTER SERVICE ACCOUNT 'batch-processor' SET DATABASES (analytics, logs);

-- View all service accounts
SHOW SERVICE ACCOUNTS;
```

Service accounts are isolated from user accounts and cannot authenticate via password.

## OIDC Single Sign-On

Authenticate via external identity providers (Auth0, Okta, Keycloak) using JWT bearer tokens. One-time setup per provider, then users authenticate directly with the provider.

```sql
-- Admin registers provider
CREATE OIDC PROVIDER auth0
    ISSUER 'https://your-domain.auth0.com/'
    JWKS_URI 'https://your-domain.auth0.com/.well-known/jwks.json'
    TENANT 42
    AUDIENCE 'nodedb-api';
```

**Supported on:** Native protocol and HTTP. **Not on pgwire** — the Postgres wire protocol has no standard bearer-token framing; pgwire stays SCRAM-SHA-256 only.

See [OIDC Single Sign-On](oidc.md) for full setup and claim mapping.

## JWT Configuration — Legacy

Multi-provider support for Auth0, Clerk, Supabase, Firebase, Keycloak, and Cognito via `nodedb.toml` configuration.

Configure in `nodedb.toml`:

```toml
[[auth.jwt.providers]]
name = "auth0"
jwks_url = "https://your-domain.auth0.com/.well-known/jwks.json"
issuer = "https://your-domain.auth0.com/"
audience = "your-api"
tenant_id = 42
```

JWT claims map to `$auth.*` session variables:

| JWT Claim         | Session Variable | Usage                                            |
| ----------------- | ---------------- | ------------------------------------------------ |
| `sub`             | `$auth.id`       | RLS: `WHERE user_id = $auth.id`                  |
| `role` / custom   | `$auth.role`     | RLS: `WHERE $auth.role = 'admin'`                |
| `org_id` / custom | `$auth.org_id`   | RLS: `WHERE org_id = $auth.org_id`               |
| `scope`           | `$auth.scopes`   | RLS: `WHERE $auth.scopes CONTAINS 'read:orders'` |

Supported algorithms: ES256, ES384, RS256. Built-in JWKS cache with disk fallback and circuit breaker for provider outages.

**Note:** The OIDC provider mechanism (above) is the recommended approach for modern SSO; this `nodedb.toml` method is supported for backward compatibility. Every static provider requires an explicit `tenant_id`; startup fails closed when it is omitted. Providers may share an issuer only when each has a distinct, non-empty audience.

JWTs may assert ordinary built-in and custom roles, but they cannot assert NodeDB superuser authority. NodeDB ignores both the `is_superuser` claim and the `"superuser"` role string on every external JWT path. Superuser must be granted through NodeDB credential administration.

## mTLS (Mutual TLS)

For zero-trust environments. Both client and server present certificates.

Configure in `nodedb.toml`:

```toml
[server.tls]
cert = "/path/to/server.crt"
key = "/path/to/server.key"
client_ca = "/path/to/ca.crt"     # enables mTLS
crl = "/path/to/revocation.crl"   # optional CRL
```

## Dropping Users

`DROP USER` is safe against dangling references. Before the user row is deleted:

- Every object the user owns — collections, functions, procedures, triggers, materialized views, sequences, schedules, change streams, continuous aggregates, indexes — is reassigned to the tenant admin (`{tenant}_admin`).
- All grants held by the user are revoked.

The operation is fail-closed: if any reassignment fails, the user is not deleted, so a partially-cleaned user can never leave orphaned owner references.

```sql
DROP USER alice;
DROP USER IF EXISTS alice;
```

## Login Rate Limiting

Pre-auth rate limits protect against brute-force and Argon2/SCRAM CPU exhaustion:

- **Per-IP failure cap** — default 30 failed attempts/min (`cluster.login_attempts_per_ip_per_min`)
- **Per-user failure cap** — default 10 failed attempts/min (`cluster.login_attempts_per_user_per_min`)
- **Per-IP verification ceiling** — `max(ip_cap × 4, 120)` credential verifications/min, consumed on every attempt (successful or not) to bound password-hashing CPU

Only genuine credential failures consume the failure budgets — a burst of successful reconnects (e.g., a warming connection pool) is never rejected. Set a cap to `0` to disable it. A rate-limit rejection returns a distinct, retryable `TOO_MANY_CONNECTIONS` error rather than a generic credential failure, so clients can distinguish "slow down and retry" from "bad password." All denials take constant time with a uniform delay to prevent timing leaks.

## Auth Priority

When multiple methods are configured, NodeDB checks in order:

1. mTLS (if client certificate present)
2. JWT Bearer token (if `Authorization` header present)
3. API key (if `Authorization: Bearer` matches a key)
4. SCRAM-SHA-256 (pgwire password auth)

[Back to security](README.md)
