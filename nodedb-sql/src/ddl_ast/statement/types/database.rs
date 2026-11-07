// SPDX-License-Identifier: Apache-2.0

//! Database DDL/DML statements.

use crate::ddl_ast::statement::collection::{
    AlterDatabaseOperation, AlterTenantOperation, CloneAsOf,
};

#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseStmt {
    /// `CREATE DATABASE [IF NOT EXISTS] <name> [WITH (...)]`
    CreateDatabase {
        name: String,
        if_not_exists: bool,
        /// Key-value pairs from `WITH (...)`, if present.
        options: Vec<(String, String)>,
    },
    /// `DROP DATABASE [IF EXISTS] <name> [CASCADE | FORCE]`
    ///
    /// `FORCE` and `CASCADE` are accepted as synonyms by the parser and both
    /// set `cascade = true`. PostgreSQL's `WITH (FORCE)` extension also
    /// terminates active sessions; that is a separate concern handled by the
    /// session registry at drop time and does not require a distinct AST flag.
    DropDatabase {
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    /// `ALTER DATABASE <name> <operation>`
    AlterDatabase {
        name: String,
        operation: AlterDatabaseOperation,
    },
    /// `SHOW DATABASES`
    ShowDatabases,
    /// `SHOW DATABASE QUOTA FOR <name>` — quota limits for a named database.
    ShowDatabaseQuota {
        name: String,
    },
    /// `SHOW DATABASE USAGE FOR <name>` — runtime usage counters for a database.
    ShowDatabaseUsage {
        name: String,
    },
    /// `SHOW DATABASE LINEAGE FOR <name>` — walks the parent clone chain from
    /// `<name>` up to the root, returning one row per ancestor with
    /// `(database_id, name, as_of_lsn, clone_created_at_lsn)`.
    ShowDatabaseLineage {
        name: String,
    },
    /// `ALTER TENANT <name> IN DATABASE <db> <operation>`
    ///
    /// New SQL surface. Sets per-tenant resource budgets within a specific database.
    AlterTenant {
        name: String,
        database: String,
        operation: AlterTenantOperation,
    },
    /// `SHOW TENANT QUOTA FOR <name> IN DATABASE <db>`
    ShowTenantQuotaInDatabase {
        name: String,
        database: String,
    },
    /// `SHOW TENANT USAGE FOR <name> IN DATABASE <db>`
    ShowTenantUsageInDatabase {
        name: String,
        database: String,
    },
    /// `SHOW TENANTS`
    ShowTenants,
    /// `SHOW TENANT <name|id>` — single-row introspection for a tenant
    /// identified by name or numeric id. The handler resolves whichever
    /// form `ident` matches.
    ShowTenantByIdentifier {
        ident: String,
    },
    /// `SHOW TENANTS WITH NAME <name>` — filtered list form. Same row
    /// shape as `SHOW TENANTS`, restricted server-side to the named
    /// tenant.
    ShowTenantsFilteredByName {
        name: String,
    },
    /// `MOVE TENANT <tenant> FROM <db_a> TO <db_b>`
    ///
    /// Returns `FEATURE_NOT_YET_IMPLEMENTED` until the tenant-move subsystem lands.
    MoveTenant {
        tenant_name: String,
        from_db: String,
        to_db: String,
    },
    /// `USE DATABASE <name>` — session reset to a different database.
    UseDatabase {
        name: String,
    },
    /// `CLONE DATABASE <new> FROM <source> [AS OF SYSTEM TIME <ms> | LATEST]`
    CloneDatabase {
        new_name: String,
        source_name: String,
        /// The temporal anchor for this clone. `Latest` means "use the
        /// source's current commit LSN at clone time".
        as_of: CloneAsOf,
    },
    /// `MIRROR DATABASE <local_name> FROM <source_cluster>.<source_database> [MODE = sync | async]`
    ///
    /// Creates a continuously-updated read-only replica of `source_database` in
    /// `source_cluster`. The local database is initialized with
    /// `MirrorStatus::Bootstrapping` and transitions to `Following` once the
    /// initial snapshot transfer completes.
    ///
    /// Every match on this variant must be exhaustive — no `_ =>` arms.
    MirrorDatabase {
        /// Name of the new local mirror database.
        local_name: String,
        /// Cluster identifier of the source cluster.
        source_cluster: String,
        /// Name of the database in the source cluster to mirror.
        source_database: String,
        /// Replication mode: `Sync` means the source waits for mirror ack;
        /// `Async` (default) means the mirror trails the source.
        mode: nodedb_types::MirrorMode,
    },
    /// `SHOW DATABASE MIRROR STATUS [FOR <name>]`
    ///
    /// Returns one row per mirror database (or one row if `FOR <name>` is given):
    /// `name`, `source_cluster`, `source_database`, `source_database_name`,
    /// `mode`, `status`, `failure`, `bytes_done`, `bytes_total`, `lag_ms`,
    /// `ready_cut`, `applied_cut`, `applied_cut_hlc_ns`.
    ///
    /// Every match on this variant must be exhaustive — no `_ =>` arms.
    ShowDatabaseMirrorStatus {
        /// Filter to a specific mirror by name, or `None` to show all mirrors.
        name: Option<String>,
    },
    /// `BACKUP DATABASE <name> TO '<uri>'`: every tenant's rows in the
    /// database, written to an object-store URI.
    BackupDatabase {
        name: String,
        uri: String,
    },
    /// `RESTORE DATABASE <name> FROM '<uri>' [FORCE] [DRY RUN]`.
    RestoreDatabase {
        name: String,
        uri: String,
        /// Overwrite writes newer than the backup.
        force: bool,
        /// Check the backup and write nothing.
        dry_run: bool,
    },

    // ── Backup / restore ─────────────────────────────────────────
    BackupTenant {
        tenant_id: String,
    },
    RestoreTenant {
        dry_run: bool,
        force: bool,
        tenant_id: String,
    },
}
