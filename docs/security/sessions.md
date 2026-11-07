# Session Management

NodeDB tracks active sessions with comprehensive lifecycle management: idle timeout, admin disconnect, and automatic revocation when user permissions change.

## Listing Sessions

View all active connections across the cluster.

```sql
-- Show all sessions (superuser only)
SHOW SESSIONS;

-- Filter by database
SHOW SESSIONS IN DATABASE prod_shard;

-- Filter by tenant
SHOW SESSIONS IN TENANT acme;

-- Filter by user
SHOW SESSIONS WHERE user_id = 42;
```

**Output columns:**

| Column                     | Type      | Description                                |
| -------------------------- | --------- | ------------------------------------------ |
| `session_id`               | UUID      | Unique session identifier                  |
| `addr`                     | String    | Client IP:port address                     |
| `user`                     | String    | Authenticated username                     |
| `database`                 | String    | Currently bound database name              |
| `started_at`               | Timestamp | Connection created at                      |
| `last_active_ms`           | i64       | Milliseconds since last request            |
| `idle_timeout_secs`        | u32       | Per-database idle timeout (0 = disabled)   |
| `token_expiry_ms`          | i64       | OIDC bearer token expiry, if applicable    |
| `bytes_in`                 | i64       | Total bytes received                       |
| `bytes_out`                | i64       | Total bytes sent                           |
| `current_statement_digest` | String    | Query hash of in-flight statement (if any) |

## Killing a Session

Admin termination of a session. The connection closes at the next request boundary with `SESSION_REVOKED` error code.

```sql
-- Kill a session by ID
KILL SESSION '550e8400-e29b-41d4-a716-446655440000';
```

**Permissions:** Superuser, ClusterAdmin, or the session owner.

**Error:** `42704 SESSION_NOT_FOUND` if the session ID does not exist.

**Audit:** Emits a `SessionRevoked` audit entry with `reason = AdminKill` before the connection closes.

## Killing Every Session of a User

```sql
-- End the sessions of credential-store user 42
KILL USER SESSIONS '42';

-- End the OIDC sessions whose token claims carry user_id 42
KILL USER SESSIONS '42' OIDC;
```

A credential-store user and an OIDC principal share no id space, so they can hold the same number. Each form ends one principal's sessions and leaves the other connected. `SHOW SESSIONS` lists each session's `user_id` and `auth_method`.

**Permissions:** Superuser.

**Errors:** `42601` for any other trailing word. `22003` for a non-numeric id.

**Blacklist:** `BLACKLIST AUTH USER '42'` names no auth method. It refuses every identity with that `user_id`, stored or OIDC, and `WITH KILL SESSIONS` ends the same set. A token with no `user_id` claim goes by its subject: `BLACKLIST AUTH USER 'subject'`.

## Idle Timeout

Per-database setting to automatically close sessions idle beyond a threshold.

```sql
-- Set 30-minute idle timeout
ALTER DATABASE prod SET IDLE_TIMEOUT 1800;

-- Disable idle timeout (default)
ALTER DATABASE prod SET IDLE_TIMEOUT 0;

-- View current setting
SHOW DATABASE prod;
```

**How it works:**

- Session tracks `last_activity_ms` (updated on every request entry)
- Background timer in the Control Plane checks every 30 seconds
- Sessions idle >= `configured_threshold_secs` close with `SESSION_IDLE_TIMEOUT` error
- Idle-timeout deadline composed as `min(token_expiry, last_active_ms + timeout_secs * 1000)`
  - If OIDC token expires sooner than idle timeout, token expiry takes precedence
  - Activity resets the idle clock but cannot extend an expired token

**Scope:** Per-database; affects all users connecting to that database.

## KillReason

Every session termination is recorded with a reason code in the audit log. Possible values:

| Reason           | Cause                                              |
| ---------------- | -------------------------------------------------- |
| `Alive`          | Session still active                               |
| `UserDropped`    | User account was deleted (DROP USER)               |
| `IdleTimeout`    | Exceeded idle_timeout_secs threshold               |
| `TokenExpired`   | OIDC bearer token TTL expired                      |
| `AdminKill`      | Admin executed KILL SESSION or KILL USER SESSIONS  |
| `SessionRevoked` | User role/database grant was revoked (soft-revoke) |

Audit log shows `KillReason` in the `reason` field of `SessionRevoked` events.

## Session Revocation

When user permissions change, the session is invalidated. Revocation type determines how the session reacts:

**Hard revocation** (session closed at next request):

- `DROP USER` (user deleted)
- User soft-delete (`ALTER USER name SET ACTIVE false`)
- Complete role purge (all roles revoked)

Hard-revoke emits audit entry and closes the connection immediately at the next request boundary.

**Soft revocation** (identity refreshed without reconnect):

- `ALTER USER name SET ROLE new_role`
- `GRANT ROLE` or `REVOKE ROLE`
- `REVOKE DATABASE` (removes database access)

Soft-revoke triggers identity rehydration: the session's `AuthenticatedIdentity` is rebuilt from the latest `UserRecord` at the next request entry. The in-flight statement completes normally with the old identity; the refresh takes effect on the _next_ request.

**Test: in-flight propagation**

```sql
-- Connection 1
ALTER USER alice SET DEFAULT DATABASE prod;

-- Connection 2 (Alice's other session)
SELECT user(), database();  -- Returns 'alice', previous database (unchanged for in-flight)
SELECT user(), database();  -- Returns 'alice', 'prod' (refreshed on next request)
```

## In-Flight Permission Propagation

GRANT, REVOKE, and role changes on one connection take effect on other open connections' _next statement_ — no reconnect needed.

**How it works:**

- `CredentialStore` maintains a per-user version counter
- Every mutation (role change, grant, revoke) bumps the version
- At request entry, the session checks if its cached version is stale
- If stale, the `AuthenticatedIdentity` is rebuilt from the latest `UserRecord`
- Refresh is atomic and silent — users don't see a reconnect

**Example: distributed team**

```sql
-- Alice has three concurrent connections (laptop, phone, tablet)
-- Colleague revokes Alice's insert permission from one browser window:

-- Superuser's connection:
REVOKE INSERT ON orders FROM alice;

-- Alice's next request on any of her three connections:
INSERT INTO orders VALUES (1, 'item');
-- Error: INSUFFICIENT_PRIVILEGE (permission check refreshed inline)
```

## Persistent Login Lockout

Failed login attempts lock the account after a configurable threshold. Lockout state survives restart.

**Configuration:**

```toml
[cluster]
login_failure_threshold = 5        # Lock after 5 failed attempts
login_lockout_duration_secs = 900  # Lock for 15 minutes
```

**How it works:**

- Lockout state stored in `_system.lockout_state` redb table
- `last_failure_ip` recorded for forensics
- Restart does NOT unlock accounts — lockout persists
- Expired lockouts (older than `login_lockout_duration_secs`) garbage-collected at startup

**Audit:**

```sql
SHOW AUDIT WHERE event_type = 'lockout_triggered';
-- Returns: username, ip, locked_until (timestamp)
```

**Admin unlock:**

```sql
-- Superuser clears lockout
ALTER USER alice SET ACTIVE true;  -- Implicitly clears lockout
```

## Pre-Auth Login Rate Limiting

Brute-force protection applied before password verification. Two token buckets per connection:

| Bucket                  | Capacity    | Resets       |
| ----------------------- | ----------- | ------------ |
| `login_ip:{addr}`       | 30 / minute | Every minute |
| `login_user:{username}` | 10 / minute | Every minute |

**Configuration:**

```toml
[cluster]
login_attempts_per_ip_per_min = 30
login_attempts_per_user_per_min = 10
```

**How it works:**

- Buckets checked _before_ SCRAM exchange or Argon2 hash (cheap exit path)
- Rate limit hit returns generic `INVALID_CREDENTIALS` error after uniform delay (no timing leak)
- In-memory only — resets on restart, one-minute window
- Protects against slow brute-force and credential-stuffing attacks

**Audit:**

```sql
SHOW AUDIT WHERE event_type = 'login_rate_limited';
-- Returns: username, ip, timestamp
```

## Session Registry Bounds

Maximum concurrent sessions per cluster is configurable. Over-cap rejects new login attempts.

```toml
[cluster]
max_active_sessions = 10000
```

**Error:** `SESSION_CAP_EXCEEDED` if attempting to connect and registry is at capacity.

Sessions are **never silently evicted** (LRU is forbidden). Instead, new logins are rejected until an existing session disconnects.

## Combining Lockout, Rate Limit, and Timeout

All three mechanisms work together:

1. **Before login**: rate limit checked first (cheap)
2. **During authentication**: lockout checked (persistent)
3. **Session alive**: idle timeout monitored (background)
4. **Token bearer**: expiry checked (OIDC)
5. **Permission change**: revocation applied (next request)

**Example timeline:**

```
t=0:00   User fails login 5 times → locked (LockoutTriggered audit)
t=0:05   User attempts login, rate-limited (LoginRateLimited audit)
t=15:00  User attempts login, lockout expired, succeeds
t=15:05  Superuser revokes user's database access
t=15:06  User's next query fails with PermissionDenied audit
t=15:07  User idle for 30 min → SessionIdleTimeout audit, connection closes
```

## Errors

All session-related errors produce audit rows:

| Error Code             | Event Type               | Trigger                              |
| ---------------------- | ------------------------ | ------------------------------------ |
| `INVALID_CREDENTIALS`  | `LoginRateLimited`       | IP or username rate limit exceeded   |
| `LOCKOUT_TRIGGERED`    | `LockoutTriggered`       | Failed attempts exceed threshold     |
| `SESSION_REVOKED`      | `SessionRevoked`         | User/role change invalidated session |
| `SESSION_IDLE_TIMEOUT` | (reason = `IdleTimeout`) | Idle threshold exceeded              |
| `SESSION_NOT_FOUND`    | N/A                      | KILL SESSION with invalid ID         |
| `SESSION_CAP_EXCEEDED` | N/A                      | Max active sessions exceeded         |

[Back to security](README.md)
