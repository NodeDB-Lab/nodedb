// SPDX-License-Identifier: BUSL-1.1

//! The `CatalogEntry` enum itself.
//!
//! Every variant corresponds to a single mutation on the host-side
//! `SystemCatalog` redb and/or an in-memory registry on
//! `SharedState`. Adding a variant forces every consumer to handle
//! it (the apply / post_apply / tests modules use exhaustive
//! matches).

use crate::control::security::catalog::{
    StoredCollection, StoredContinuousAggregate, StoredCustomType, StoredMaterializedView,
    StoredOidcProvider, StoredRlsPolicy, StoredSynonymGroup,
    auth_types::{
        StoredApiKey, StoredOwner, StoredPermission, StoredRole, StoredTenant, StoredUser,
    },
    function_types::StoredFunction,
    procedure_types::StoredProcedure,
    sequence_types::{SequenceState, StoredSequence},
    trigger_types::StoredTrigger,
};
use crate::event::cdc::stream_def::ChangeStreamDef;
use crate::event::scheduler::types::ScheduleDef;

#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum CatalogEntry {
    // ── Collection ─────────────────────────────────────────────────
    /// Upsert a collection record. Used by CREATE COLLECTION and by
    /// every ALTER COLLECTION path that ships a full updated record
    /// (strict schema changes, retention / legal_hold / LVC /
    /// append_only toggles, materialized_sum bindings).
    PutCollection(Box<StoredCollection>),
    /// Create-only collection upsert: applies iff the collection is
    /// absent, and never clobbers an existing schema. Used by CRDT
    /// sync to materialize announced collections without racing or
    /// overwriting a locally-authored definition of the same name.
    PutCollectionIfAbsent(Box<StoredCollection>),
    /// Mark a collection as `is_active = false`. Record is
    /// preserved for audit + undrop. The soft-delete step in the
    /// two-step DROP → retention-expiry → PURGE flow.
    DeactivateCollection {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },
    /// Hard-delete a collection: remove the `StoredCollection`
    /// row + owner row + cascade-dependent catalog entries, and
    /// dispatch `MetaOp::UnregisterCollection` to every node's Data
    /// Plane so per-engine storage is reclaimed.
    ///
    /// Reached by three paths:
    ///
    /// 1. `DROP COLLECTION ... PURGE` (immediate, operator-requested,
    ///    superuser / tenant_admin only).
    /// 2. `CollectionGC` sweeper on the Event Plane, after the
    ///    configured `deactivated_collection_retention_days` window
    ///    has elapsed since `DeactivateCollection`.
    /// 3. `SELECT _system.purge_collection(...)` operator function.
    ///
    /// Preserves the two-step safety net: soft-deleted collections
    /// are UNDROP-able until retention expires; after purge the
    /// record is gone and data is unrecoverable (except from backup).
    PurgeCollection {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },

    // ── Sequence ───────────────────────────────────────────────────
    /// Upsert a sequence record. Used by CREATE SEQUENCE and ALTER
    /// SEQUENCE FORMAT. Carries the full updated record so
    /// followers can apply the change without shipping a diff.
    PutSequence(Box<StoredSequence>),
    /// Delete a sequence record entirely. Used by DROP SEQUENCE and
    /// by the cascade path in DROP COLLECTION that removes implicit
    /// `{coll}_{field}_seq` sequences for SERIAL columns.
    DeleteSequence { tenant_id: u64, name: String },
    /// Upsert the runtime state of a sequence (current value,
    /// is_called, epoch, period_key). Used by ALTER SEQUENCE
    /// RESTART to propagate the new counter across nodes.
    PutSequenceState(Box<SequenceState>),

    // ── Trigger ────────────────────────────────────────────────────
    /// Upsert a trigger record. Used by CREATE [OR REPLACE] TRIGGER
    /// and by ALTER TRIGGER ENABLE/DISABLE paths that ship a full
    /// updated record.
    PutTrigger(Box<StoredTrigger>),
    /// Delete a trigger record.
    DeleteTrigger { tenant_id: u64, name: String },

    // ── Function ───────────────────────────────────────────────────
    /// Upsert a function record. Used by CREATE [OR REPLACE]
    /// FUNCTION. WASM binaries still live in their separate
    /// wasm-store redb table and are written directly on the
    /// proposing node; replicated wasm binary distribution is its
    /// own future batch.
    PutFunction(Box<StoredFunction>),
    /// Delete a function record.
    DeleteFunction { tenant_id: u64, name: String },

    // ── Procedure ──────────────────────────────────────────────────
    /// Upsert a stored procedure. Same body-cache invalidation
    /// pattern as `PutFunction` — the `block_cache` is cleared so
    /// the next CALL re-parses the new body.
    PutProcedure(Box<StoredProcedure>),
    /// Delete a stored procedure.
    DeleteProcedure { tenant_id: u64, name: String },

    // ── Schedule ───────────────────────────────────────────────────
    /// Upsert a scheduled-job definition. Post-apply syncs the
    /// in-memory `schedule_registry` so the cron executor on every
    /// node picks up the new / updated schedule immediately.
    PutSchedule(Box<ScheduleDef>),
    /// Delete a scheduled-job definition.
    DeleteSchedule { tenant_id: u64, name: String },

    // ── Synonym group ──────────────────────────────────────────────
    /// Upsert a synonym group. Post-apply syncs the in-memory `synonym_registry`.
    PutSynonymGroup(Box<StoredSynonymGroup>),
    /// Delete a synonym group. Post-apply removes it from the registry.
    DeleteSynonymGroup { tenant_id: u64, name: String },

    // ── Custom type ────────────────────────────────────────────────
    /// Upsert a custom type (enum or composite). Post-apply syncs the
    /// in-memory `custom_type_registry`.
    PutCustomType(Box<StoredCustomType>),
    /// Delete a custom type. Post-apply removes it from the registry.
    DeleteCustomType { tenant_id: u64, name: String },

    // ── Change stream ──────────────────────────────────────────────
    /// Upsert a CDC change-stream definition. Post-apply syncs the
    /// in-memory `stream_registry` so the Event Plane starts
    /// buffering matching WriteEvents on every node.
    PutChangeStream(Box<ChangeStreamDef>),
    /// Delete a CDC change-stream definition + tear down its
    /// buffer via `cdc_router.remove_buffer`.
    DeleteChangeStream { tenant_id: u64, name: String },

    // ── User ───────────────────────────────────────────────────────
    /// Upsert a user record. The leader builds the full `StoredUser`
    /// (including Argon2 hash, SCRAM salt, and user_id) via
    /// `CredentialStore::prepare_user` before proposing — followers
    /// accept the pre-computed record verbatim and bump their local
    /// `next_user_id` counter to stay ahead of replicated IDs.
    PutUser(Box<StoredUser>),
    /// Drop a user: fully remove the identity record from every
    /// node's in-memory cache and redb catalog, freeing the
    /// username for reuse.
    DropUser { username: String },

    // ── Role ───────────────────────────────────────────────────────
    /// Upsert a custom role. Built-in roles (Superuser/TenantAdmin/
    /// ReadWrite/ReadOnly/Monitor) never flow through this variant —
    /// they're hardcoded in `identity.rs`.
    PutRole(Box<StoredRole>),
    /// Delete a custom role. Does not cascade to grants that
    /// reference it (matching current local-only DROP semantics).
    DeleteRole { name: String },

    // ── ApiKey ─────────────────────────────────────────────────────
    /// Upsert an API key record. The leader builds the full
    /// `StoredApiKey` (including SHA-256 secret_hash) via
    /// `ApiKeyStore::prepare_key`; followers accept the pre-computed
    /// record verbatim. The plaintext secret NEVER enters raft —
    /// only the proposing client receives the token.
    PutApiKey(Box<StoredApiKey>),
    /// Revoke an API key — sets `is_revoked = true` in the cached
    /// record and re-writes the redb row. Preserves the record for
    /// audit trails.
    RevokeApiKey { key_id: String },

    // ── Materialized View ──────────────────────────────────────────
    /// Upsert a materialized view definition. The Data Plane
    /// refresh loop picks up the new definition on its next tick
    /// and starts materializing rows from source → target.
    PutMaterializedView(Box<StoredMaterializedView>),
    /// Delete a materialized view definition. The target
    /// collection is NOT deleted — operators drop it separately
    /// with `DROP COLLECTION` if desired.
    DeleteMaterializedView { tenant_id: u64, name: String },

    // ── Continuous Aggregate ───────────────────────────────────────
    /// Upsert a continuous-aggregate definition. The applier writes
    /// the catalog row plus the owner row; the post-apply sync
    /// re-dispatches `MetaOp::RegisterContinuousAggregate` to the
    /// local Data Plane so the runtime manager picks up the change
    /// without re-issuing DDL.
    PutContinuousAggregate(Box<StoredContinuousAggregate>),
    /// Delete a continuous-aggregate definition. The target
    /// collection that holds materialized rows is NOT deleted —
    /// operators drop it separately with `DROP COLLECTION` if
    /// desired (mirrors the materialized-view contract).
    DeleteContinuousAggregate {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },

    // ── Tenant ─────────────────────────────────────────────────────
    /// Upsert a tenant identity record. Quotas are NOT part of
    /// `StoredTenant`; they live in the in-memory `TenantStore` and
    /// quota replication is handled separately. Post-apply seeds
    /// default quota on every node so reads work immediately after
    /// creation.
    PutTenant(Box<StoredTenant>),
    /// Atomically create a tenant and its authoritative administrator.
    PutTenantWithAdmin {
        tenant: Box<StoredTenant>,
        admin: Box<StoredUser>,
    },
    /// Hard-delete a tenant identity record. Tenant data is not
    /// purged — that is a separate `PURGE TENANT CONFIRM` Data
    /// Plane meta op.
    DeleteTenant { tenant_id: u64 },

    // ── RLS policy ─────────────────────────────────────────────────
    /// Upsert an RLS policy. The leader serializes the runtime
    /// `RlsPolicy` (compiled predicate + deny mode) into the
    /// catalog-shape `StoredRlsPolicy` before proposing; followers
    /// re-hydrate the runtime form via `to_runtime()` in post_apply.
    PutRlsPolicy(Box<StoredRlsPolicy>),
    /// Delete a single RLS policy by `(tenant_id, collection, name)`.
    DeleteRlsPolicy {
        tenant_id: u64,
        collection: String,
        name: String,
    },

    // ── Permission grant ───────────────────────────────────────────
    /// Upsert an explicit permission grant
    /// (`GRANT <perm> ON <target> TO <grantee>`). The catalog row is
    /// the authoritative copy on every node; the in-memory
    /// `PermissionStore.grants` set is rebuilt from it on apply.
    PutPermission(Box<StoredPermission>),
    /// Delete a permission grant by `(target, grantee, permission)`.
    /// `permission` is the lowercase canonical name
    /// (`read|write|create|drop|alter|admin|monitor|execute`).
    DeletePermission {
        target: String,
        grantee: String,
        permission: String,
    },

    // ── Database lifecycle ─────────────────────────────────────────
    /// Upsert a database descriptor. Used by `CREATE DATABASE` and by
    /// `ALTER DATABASE RENAME`, `SET QUOTA`, `MATERIALIZE`, `PROMOTE`.
    /// Followers apply the full updated record verbatim.
    PutDatabase(Box<crate::control::security::catalog::database_types::DatabaseDescriptor>),
    /// Hard-delete a database descriptor and its reverse-lookup row from
    /// `_system.databases` and `_system.databases_by_name`. Used by
    /// `DROP DATABASE` after all collections have been cascaded. Does not
    /// touch collection rows — those must be removed before proposing this.
    DeleteDatabase {
        /// Numeric database id.
        db_id: u64,
    },
    /// Upsert a database-level permission grant.
    /// Stored in `_system.database_grants`. Mirrors `PutPermission` for
    /// collection-level grants but keyed by `(db_id, user_id, privilege)`.
    PutDatabaseGrant {
        db_id: u64,
        user_id: u64,
        privilege: String,
    },
    /// Delete a database-level permission grant.
    DeleteDatabaseGrant {
        db_id: u64,
        user_id: u64,
        privilege: String,
    },

    // ── Object ownership ───────────────────────────────────────────
    /// Upsert an ownership record. Used by handlers whose object
    /// has no replicated parent variant (indexes, spatial indexes,
    /// `ALTER OBJECT OWNER`). Objects that already ship a parent
    /// `Stored*` carrying an `owner` field replicate ownership via
    /// the parent's post_apply instead — this variant is only for
    /// the orphan path.
    PutOwner(Box<StoredOwner>),
    /// Delete an ownership record by database-scoped object identity.
    DeleteOwner {
        object_type: String,
        database_id: u64,
        tenant_id: u64,
        object_name: String,
    },

    // ── Move Tenant lifecycle ──────────────────────────────────────
    /// Atomically move a tenant's collections from one database to another.
    ///
    /// This is the single Raft proposal that makes the cutover phase of
    /// `MOVE TENANT` atomic. On apply it:
    /// 1. Writes each `StoredCollection` in `collections` to `target_db_id`.
    /// 2. Deletes each collection from `source_db_id`.
    ///
    /// The handler builds this entry after snapshot succeeds; the Raft
    /// proposal is a complete, self-contained mutation that any follower
    /// can replay without external lookups.
    MoveTenantCutover {
        tenant_id: u64,
        source_db_id: u64,
        target_db_id: u64,
        /// The tenant's collections serialized at their source state.
        /// Each will be re-keyed to `target_db_id` on apply.
        collections: Vec<StoredCollection>,
    },

    // ── OIDC provider lifecycle ────────────────────────────────────
    /// Upsert an OIDC provider. Used by `CREATE / ALTER OIDC PROVIDER`.
    /// Post-apply refreshes the in-memory `oidc_provider_cache`.
    PutOidcProvider(Box<StoredOidcProvider>),
    /// Delete an OIDC provider record by name.
    DeleteOidcProvider { name: String },

    // ── WAL replay tombstone ───────────────────────────────────────
    /// Record (or raise) a per-(database, tenant, collection) WAL replay tombstone.
    /// Replicated on RESTORE so every replica's boot-time WAL replay barrier
    /// (`purge_lsn`) matches — without it, purged writes resurrect on follower
    /// restart. Idempotent + monotone (see `record_wal_tombstone`).
    RecordWalTombstone {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        purge_lsn: u64,
    },

    // ── Clone lifecycle ────────────────────────────────────────────
    /// Atomically record a new CoW clone database.
    ///
    /// On apply this entry does three things as a single unit:
    /// 1. Writes the target `DatabaseDescriptor` (with `status = Cloning`
    ///    and `parent_clone` populated) into `_system.databases`.
    /// 2. Upserts `clone_lineage`: adds `target_db_id` to the children
    ///    list of `source_db_id`.
    ///
    /// The handler builds this entry after resolving `as_of_lsn` and
    /// allocating `target_db_id` so that the Raft proposal is a complete,
    /// self-contained mutation that any follower can replay without
    /// external lookups.
    CloneDatabase {
        /// The descriptor for the newly created target database.
        target_descriptor:
            Box<crate::control::security::catalog::database_types::DatabaseDescriptor>,
        /// Numeric id of the source database (for lineage update).
        source_db_id: u64,
    },
}

impl CatalogEntry {
    /// Short, human-readable descriptor of this entry — used in
    /// trace / metric labels.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::PutCollection(_) => "put_collection",
            Self::PutCollectionIfAbsent(_) => "put_collection_if_absent",
            Self::DeactivateCollection { .. } => "deactivate_collection",
            Self::PurgeCollection { .. } => "purge_collection",
            Self::PutSequence(_) => "put_sequence",
            Self::DeleteSequence { .. } => "delete_sequence",
            Self::PutSequenceState(_) => "put_sequence_state",
            Self::PutTrigger(_) => "put_trigger",
            Self::DeleteTrigger { .. } => "delete_trigger",
            Self::PutFunction(_) => "put_function",
            Self::DeleteFunction { .. } => "delete_function",
            Self::PutProcedure(_) => "put_procedure",
            Self::DeleteProcedure { .. } => "delete_procedure",
            Self::PutSchedule(_) => "put_schedule",
            Self::DeleteSchedule { .. } => "delete_schedule",
            Self::PutChangeStream(_) => "put_change_stream",
            Self::DeleteChangeStream { .. } => "delete_change_stream",
            Self::PutUser(_) => "put_user",
            Self::DropUser { .. } => "drop_user",
            Self::PutRole(_) => "put_role",
            Self::DeleteRole { .. } => "delete_role",
            Self::PutApiKey(_) => "put_api_key",
            Self::RevokeApiKey { .. } => "revoke_api_key",
            Self::PutMaterializedView(_) => "put_materialized_view",
            Self::DeleteMaterializedView { .. } => "delete_materialized_view",
            Self::PutContinuousAggregate(_) => "put_continuous_aggregate",
            Self::DeleteContinuousAggregate { .. } => "delete_continuous_aggregate",
            Self::PutTenant(_) => "put_tenant",
            Self::PutTenantWithAdmin { .. } => "put_tenant_with_admin",
            Self::DeleteTenant { .. } => "delete_tenant",
            Self::PutRlsPolicy(_) => "put_rls_policy",
            Self::DeleteRlsPolicy { .. } => "delete_rls_policy",
            Self::PutPermission(_) => "put_permission",
            Self::DeletePermission { .. } => "delete_permission",
            Self::PutOwner(_) => "put_owner",
            Self::DeleteOwner { .. } => "delete_owner",
            Self::PutDatabase(_) => "put_database",
            Self::DeleteDatabase { .. } => "delete_database",
            Self::PutDatabaseGrant { .. } => "put_database_grant",
            Self::DeleteDatabaseGrant { .. } => "delete_database_grant",
            Self::PutSynonymGroup(_) => "put_synonym_group",
            Self::DeleteSynonymGroup { .. } => "delete_synonym_group",
            Self::PutCustomType(_) => "put_custom_type",
            Self::DeleteCustomType { .. } => "delete_custom_type",
            Self::PutOidcProvider(_) => "put_oidc_provider",
            Self::DeleteOidcProvider { .. } => "delete_oidc_provider",
            Self::RecordWalTombstone { .. } => "record_wal_tombstone",
            Self::MoveTenantCutover { .. } => "move_tenant_cutover",
            Self::CloneDatabase { .. } => "clone_database",
        }
    }
}
