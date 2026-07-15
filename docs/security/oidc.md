# OIDC Single Sign-On

NodeDB integrates with external OIDC providers for JWT-based bearer authentication. Users authenticate once with an identity provider and receive a token usable across NodeDB without managing separate passwords.

## Overview

OIDC is a delegated authentication protocol where:

1. User authenticates with an external provider (Auth0, Okta, Keycloak, etc.)
2. Provider issues a JWT bearer token
3. User presents token to NodeDB in the `Authorization: Bearer <token>` header
4. NodeDB validates the signature via the provider's JWKS, applies claim-mapping rules, and grants access

**Supported on:** Native protocol and HTTP only. **Not on pgwire** — the Postgres wire protocol cannot carry bearer tokens without a non-standard extension; pgwire stays SCRAM-SHA-256 only.

## Registering a Provider

Create a provider configuration:

```sql
CREATE OIDC PROVIDER okta ISSUER 'https://dev-12345.okta.com/' JWKS_URI 'https://dev-12345.okta.com/.well-known/jwks.json' TENANT 42 AUDIENCE 'api://nodedb';
```

**Parameters:**

| Parameter   | Required | Description                                                                                       |
| ----------- | -------- | ------------------------------------------------------------------------------------------------- |
| `ISSUER`    | Yes      | Provider's issuer URL (e.g. `https://accounts.google.com`, `https://your-domain.auth0.com/`)      |
| `JWKS_URI`  | Yes      | JWKS endpoint for signature validation (e.g. `https://accounts.google.com/.well-known/jwks.json`) |
| `TENANT`    | Yes      | Numeric NodeDB tenant ID bound to identities authenticated through this provider                  |
| `AUDIENCE`  | Optional | Expected `aud` claim. Required when the same issuer is registered for more than one tenant         |

**Permissions:** `CREATE OIDC PROVIDER` requires the Superuser role.

**Persistence:** Provider config is stored in `_system.oidc_providers` and replicated via Raft. The session tenant is always derived from this stored binding; a token's `tenant_id` claim is never authoritative. Existing provider records without a tenant binding are rejected until recreated with `TENANT`.

A shared corporate issuer may serve multiple NodeDB tenants by registering one provider per tenant with a distinct, non-empty audience. Duplicate or ambiguous issuer/audience routes are rejected.

## Claim Mapping

Map JWT claims to NodeDB identity attributes. Define rules using the `CLAIM MAPPING WHEN` clause:

```sql
CREATE OIDC PROVIDER okta ISSUER 'https://dev-12345.okta.com/' JWKS_URI 'https://dev-12345.okta.com/.well-known/jwks.json' TENANT 42
  CLAIM MAPPING WHEN email = 'alice@company.com' SET DEFAULT_DATABASE = 1 ADD DATABASES [1, 2] ADD ROLES ['readwrite']
  CLAIM MAPPING WHEN email = 'bob@company.com' SET DEFAULT_DATABASE = 2 ADD DATABASES [2]
  CLAIM MAPPING WHEN department = 'engineering' ADD DATABASES [1, 2, 3] ADD ROLES ['cluster_admin'];
```

**Rule structure:**

```
CLAIM MAPPING WHEN <claim> = '<value>' [SET DEFAULT_DATABASE = <db_id>] [ADD DATABASES [<id>, ...]] [ADD ROLES ['<role>', ...]]
```

**How it works:**

- If JWT contains `<claim>` with value matching `<value>`, apply the corresponding actions
- `SET DEFAULT_DATABASE` sets the user's default database for the session (numeric database ID)
- `ADD DATABASES` adds the databases to the user's accessible set (numeric IDs in brackets)
- `ADD ROLES` adds non-superuser roles to the authenticated identity (quoted role names in array)
- `superuser` is database-owned authority and cannot be granted by an OIDC claim mapping; CREATE and ALTER reject mappings that request it
- Multiple rules are OR-combined: if any rule matches, its actions apply
- Claim values support wildcards (`*`) for matching any value of a claim

**Example: wildcard for department**

```sql
CREATE OIDC PROVIDER okta ISSUER '...' JWKS_URI '...' TENANT 42
  CLAIM MAPPING WHEN department = '*' ADD DATABASES [5];
```

All users with a `department` claim get access to the database with ID 5.

Externally authenticated identities cannot assert NodeDB superuser authority. The JWT `is_superuser` field and a raw `"superuser"` role are ignored. Legacy or corrupted stored mappings that contain `superuser` are sanitized during authentication, while their permitted roles and database access remain intact; NodeDB emits a bounded operator warning when it encounters such an assertion.

## Updating Provider Configuration

Modify claim mapping:

```sql
ALTER OIDC PROVIDER okta SET CLAIM MAPPING WHEN email = 'alice@company.com' ADD DATABASES [4];
```

Changes take effect immediately for new authentications. Existing sessions retain their identity until the next request (same as role-change propagation).

## Listing Providers

View all configured providers:

```sql
SHOW OIDC PROVIDERS;
```

**Output columns:**

| Column                | Type   | Description             |
| --------------------- | ------ | ----------------------- |
| `name`                | String | Name (e.g. 'okta')      |
| `issuer`              | String | Issuer URL              |
| `jwks_uri`            | String | JWKS endpoint           |
| `tenant_id`           | String | Bound NodeDB tenant ID  |
| `audience`            | String | Expected audience claim |
| `claim_mapping_rules` | String | Number of claim rules   |

## Removing a Provider

```sql
DROP OIDC PROVIDER okta;
```

**Permissions:** Superuser.

**Effect:** Existing sessions tied to this provider are revoked at their next request boundary. New OIDC logins via this provider are rejected.

## JWKS Caching

NodeDB caches JWKS locally to avoid repeated network roundtrips:

**Cache behavior:**

- **Fetch on startup:** When a provider is registered, JWKS is fetched once
- **Refresh on `kid` miss:** If a token's `kid` (key ID) is not in the cache, refresh the JWKS
- **TTL expiry:** Cache expires after 1 hour; next validation triggers refresh
- **Circuit breaker:** If the provider is unreachable, use cached JWKS for up to 24 hours

## JWT Verification Sequence

1. Decode JWT header (check `alg`, `kid`)
2. Look up one unambiguous provider route by the `iss` and `aud` claims
3. Fetch/cache JWKS from provider's `jwks_uri`
4. Validate signature using the key with matching `kid`
5. Check `aud` claim matches provider's configured audience
6. Check `exp` (expiry) not in the past
7. Apply claim mapping rules
8. Build an ephemeral `AuthenticatedIdentity` using the provider's stored tenant binding

**Failure mode:** Any unauthenticated failure returns the same generic authentication rejection, without issuer, audience, provider, key, or signature details.

## Required Role

OIDC provider and claim-mapping operations require Superuser. Regular users cannot list or modify OIDC providers.

```sql
-- Regular user attempt
ALTER OIDC PROVIDER okta SET claim_mapping = [...];
-- Error: INSUFFICIENT_PRIVILEGE
```

## End-to-End Example

**1. Provider registration (admin)**

```sql
CREATE OIDC PROVIDER auth0 ISSUER 'https://your-domain.auth0.com/' JWKS_URI 'https://your-domain.auth0.com/.well-known/jwks.json' TENANT 42 AUDIENCE 'nodedb-api';
```

**2. Get token from provider**

```bash
# Via Auth0's token endpoint
curl -X POST https://your-domain.auth0.com/oauth/token \
  -H 'content-type: application/json' \
  -d '{
    "client_id": "your-client-id",
    "client_secret": "your-client-secret",
    "audience": "nodedb-api",
    "grant_type": "client_credentials"
  }'

# Returns: { "access_token": "eyJhbGc..." }
```

**3. Connect to NodeDB with token**

Native protocol:

```rust
let identity = AuthenticationMethod::OidcBearer {
    token: "eyJhbGc...".to_string(),
    provider: "auth0".to_string(),
};
let session = client.authenticate(identity).await?;
```

HTTP:

```bash
curl -H "Authorization: Bearer eyJhbGc..." \
     http://localhost:6480/v1/query \
     -d '{"sql": "SELECT * FROM users"}'
```

**4. Session bound to database(s) from claims**

User's token contains `sub: alice@company.com`. If claim mapping includes:

```
{ claim = 'sub', value = 'alice@company.com', effect = { add_databases = ['prod'] } }
```

Then Alice's session is bound to the `prod` database. She cannot query other databases.

## Comparison with Password Auth

| Feature                | Password (SCRAM)            | OIDC Bearer                  |
| ---------------------- | --------------------------- | ---------------------------- |
| **Credential storage** | NodeDB (hashed)             | External provider            |
| **Password change**    | `ALTER USER … SET PASSWORD` | Provider's self-service      |
| **MFA**                | N/A                         | Provider-enforced            |
| **Session lifetime**   | Connection lifetime         | Token expiry (usually 1h)    |
| **Refresh**            | Reconnect only              | Token refresh endpoint       |
| **Protocol**           | pgwire, HTTP, native        | HTTP, native only            |
| **Use case**           | Internal teams              | Federated (enterprise, SaaS) |

## Audit

OIDC authentication events appear in the audit log:

```sql
SHOW AUDIT WHERE event_type = 'AuthSuccess' AND provider = 'okta';
```

**Audit entry includes:**

| Field         | Value                               |
| ------------- | ----------------------------------- |
| `event_type`  | `AuthSuccess`                       |
| `provider`    | OIDC provider name                  |
| `jwt_subject` | JWT `sub` claim (e.g. user's email) |
| `auth_method` | `OidcBearer`                        |
| `timestamp`   | Login time                          |

**Failed OIDC attempts:**

```sql
SHOW AUDIT WHERE event_type = 'AuthFailure' AND auth_method = 'OidcBearer';
-- Returns: invalid signature, missing kid, expired token, audience mismatch, etc.
```

[Back to security](README.md)
