# Roles & Permissions (RBAC)

Role-based access control with fine-grained permissions on collections, functions, and procedures.

## Roles

```sql
CREATE ROLE analyst;
CREATE ROLE data_engineer;
```

### Built-in Roles

| Role            | Permissions                       |
| --------------- | --------------------------------- |
| `readonly`      | SELECT on all collections         |
| `readwrite`     | SELECT, INSERT, UPDATE, DELETE    |
| `monitor`       | Read metrics, health, audit logs  |
| `tenant_admin`  | Admin within a tenant             |
| `cluster_admin` | Cluster-wide DDL (no data bypass) |
| `superuser`     | Unrestricted (cross-tenant)       |

**ClusterAdmin** is distinct from Superuser:

- Superuser: bypasses all checks (RLS, CRDT validation, etc.)
- ClusterAdmin: permission-driven cluster operations only (rename, quotas, OIDC config); cannot bypass RLS or cross databases

### Database-Scoped Roles

Assign roles on a specific database:

```sql
-- Grant database owner (full control of one database)
GRANT DATABASE_OWNER ON DATABASE prod TO alice;

-- Grant editor (SELECT, INSERT, UPDATE, DELETE on one database)
GRANT DATABASE_EDITOR ON DATABASE prod TO bob;

-- Grant reader (SELECT only on one database)
GRANT DATABASE_READER ON DATABASE prod TO charlie;

-- Set default database for a user
ALTER USER alice SET DEFAULT DATABASE prod;
```

Database-scoped roles are exclusive to a single database. A user can have different roles on different databases.

## Assigning Roles

Add a role to an existing user, or promote them, with `GRANT <role> TO <user>` —
no `ON` clause. Built-in and custom roles both work, and multiple roles may be
granted at once:

```sql
GRANT tenant_admin TO eman;            -- promote a user
GRANT readonly, mae8_reader TO viewer; -- grant several roles at once
REVOKE readwrite FROM viewer;          -- remove a role

-- `GRANT ROLE <role> TO <user>` is accepted as an equivalent alias.
```

A custom role may inherit from one parent role; granting a role to a role sets
that inheritance:

```sql
GRANT mae8_ingester TO mae8_contributor;   -- mae8_contributor inherits mae8_ingester
REVOKE mae8_ingester FROM mae8_contributor;
```

## Granting Permissions

A `GRANT` with an `ON` clause grants object permissions (rather than role
membership). Permissions may be comma-separated:

```sql
-- Collection-level (object-type keywords COLLECTION and TABLE are equivalent)
GRANT SELECT ON COLLECTION orders TO analyst;
GRANT INSERT, UPDATE ON TABLE orders TO data_engineer;
GRANT ALL ON orders TO admin;

-- With ADMIN OPTION (grantee can delegate the permission to others)
GRANT SELECT ON COLLECTION orders TO analyst WITH ADMIN OPTION;

-- Function/procedure execute
GRANT EXECUTE ON FUNCTION full_name TO analyst;
GRANT EXECUTE ON PROCEDURE transfer_funds TO data_engineer;

-- Tenant-scoped privileges (apply to every collection in the tenant)
GRANT BACKUP ON TENANT acme TO ops_user;
```

## Revoking Permissions

```sql
REVOKE INSERT ON orders FROM analyst;
REVOKE EXECUTE ON FUNCTION full_name FROM analyst;
```

## Introspection

```sql
SHOW GRANTS FOR analyst;
SHOW PERMISSIONS;
```

## SECURITY DEFINER

Functions and triggers can execute with the owner's permissions instead of the caller's:

```sql
CREATE FUNCTION admin_count() RETURNS INT
    SECURITY DEFINER
    AS BEGIN
        RETURN (SELECT COUNT(*) FROM audit_log);
    END;
```

Use with caution — this is intentional privilege escalation.

## Admin DDL Gating Matrix

Who can execute cluster and database-scoped DDL:

| DDL                                   | Required Role                                   |
| ------------------------------------- | ----------------------------------------------- |
| `CREATE DATABASE`                     | Superuser or ClusterAdmin                       |
| `DROP DATABASE` (non-default)         | Superuser                                       |
| `DROP DATABASE ... FORCE`             | Superuser                                       |
| `ALTER DATABASE ... RENAME TO`        | Superuser or ClusterAdmin                       |
| `ALTER DATABASE ... SET QUOTA`        | Superuser or ClusterAdmin                       |
| `ALTER DATABASE ... SET AUDIT_DML`    | Superuser or ClusterAdmin                       |
| `ALTER DATABASE ... SET IDLE_TIMEOUT` | Superuser or ClusterAdmin                       |
| `ALTER DATABASE ... MATERIALIZE`      | Superuser, ClusterAdmin, or DatabaseOwner       |
| `CLONE DATABASE`                      | Superuser                                       |
| `MIRROR DATABASE`                     | Superuser                                       |
| `ALTER DATABASE ... PROMOTE`          | Superuser (irreversible; requires vault access) |
| `MOVE TENANT`                         | Superuser                                       |
| `BACKUP DATABASE`                     | Superuser or DatabaseOwner                      |
| `RESTORE DATABASE`                    | Superuser or DatabaseOwner; Superuser for a database that does not exist |
| `KILL SESSION`                        | Superuser, ClusterAdmin, or session owner       |
| `KILL USER SESSIONS '<id>' [OIDC]`    | Superuser                                       |
| `CREATE/ALTER/DROP OIDC PROVIDER`     | Superuser or ClusterAdmin                       |

**Permission denials** emit `PermissionDenied` audit entries and return `INSUFFICIENT_PRIVILEGE` (SQLSTATE 42501).

**Promotion note:** `ALTER DATABASE ... PROMOTE` is restricted to Superuser only because promotion is irreversible and breaks lineage to the source replica. The operational risk is high; cluster-admin credentials must be distributed across at least two operators and stored in a sealed vault.

## Permission Hierarchy

```
superuser
  └── cluster_admin (cluster DDL only; no data/RLS bypass)
  ├── database_owner (full control of one database)
  │     └── database_editor
  │           └── database_reader
  ├── tenant_admin (scoped to one tenant)
  │     └── admin (DDL + DML within tenant)
  │           └── readwrite (DML only)
  │                 └── readonly (SELECT only)
```

Higher roles inherit all permissions of lower roles within their scope.

[Back to security](README.md)
