// SPDX-License-Identifier: BUSL-1.1

//! The `CatalogEntry` enum itself.
//!
//! Each variant is one mutation on the host-side `SystemCatalog` redb
//! and/or an in-memory registry on `SharedState`. The apply, post_apply,
//! and test modules match exhaustively, so adding a variant forces every
//! consumer to handle it.
//!
//! `Put*` variants carry the full updated record, so followers apply
//! verbatim without a diff. Leader-side preparation (hashing, compiling,
//! validating) happens before the proposal; apply writes what consensus
//! accepted. Variants appended at the end of the enum stay there to keep
//! MessagePack discriminants stable across rolling upgrades.

use crate::control::security::catalog::backup_schedule_marks::StoredBackupScheduleMark;
use crate::control::security::catalog::column_stats::StoredColumnStats;
use crate::control::security::catalog::types::CheckpointRecord;
use crate::control::security::catalog::{
    StoredCollection, StoredContinuousAggregate, StoredCustomType, StoredIndexRecord,
    StoredMaterializedView, StoredOidcProvider, StoredRedactionPolicy, StoredRlsPolicy,
    StoredScopeGrant, StoredSynonymGroup,
    auth_types::{
        StoredApiKey, StoredAuthUser, StoredOwner, StoredPermission, StoredRole, StoredScopeQuota,
        StoredTenant, StoredUser,
    },
    function_types::StoredFunction,
    procedure_types::StoredProcedure,
    sequence_types::{SequenceState, StoredSequence},
    trigger_types::StoredTrigger,
};
use crate::engine::timeseries::retention_policy::RetentionPolicyDef;
use crate::event::alert::types::AlertDef;
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::event::cdc::consumer_group::OffsetCommit;
use crate::event::cdc::stream_def::ChangeStreamDef;
use crate::event::scheduler::types::ScheduleDef;
use crate::event::topic::TopicDef;
use crate::types::DatabaseId;
use nodedb_types::{StoredVectorIndexParams, VectorModelEntry};

#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum CatalogEntry {
    // ── Collection ─────────────────────────────────────────────────
    /// CREATE COLLECTION, and every ALTER COLLECTION path that ships a
    /// full updated record.
    PutCollection(Box<StoredCollection>),
    /// Create-only: applies iff the collection is absent, never clobbering an
    /// existing schema. Used by CRDT sync to materialize announced collections.
    PutCollectionIfAbsent(Box<StoredCollection>),
    /// Soft delete: sets `is_active = false`, keeping the row for audit and
    /// undrop. Step one of DROP → retention-expiry → PURGE.
    DeactivateCollection {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Prior committed version + 1. `0` is the pre-stamping compat sentinel.
        /// Frozen at propose time — an apply-time stamp diverges across replicas.
        descriptor_version: u64,
        /// Drop time, and the instant retention measures from.
        modification_hlc: nodedb_types::Hlc,
    },
    /// Hard delete: removes the `StoredCollection` row, owner row, and cascade
    /// dependents, then dispatches `MetaOp::UnregisterCollection` to every node.
    ///
    /// Reached by `DROP COLLECTION ... PURGE` (superuser / tenant_admin only),
    /// the `CollectionGC` sweeper after
    /// `deactivated_collection_retention_days`, or
    /// `SELECT _system.purge_collection(...)`. After purge the data is
    /// unrecoverable except from backup.
    PurgeCollection {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_descriptor_version: u64,
        target_hlc: nodedb_types::Hlc,
    },

    // ── Sequence ───────────────────────────────────────────────────
    /// CREATE SEQUENCE and ALTER SEQUENCE FORMAT.
    PutSequence(Box<StoredSequence>),
    /// DROP SEQUENCE, and the DROP COLLECTION cascade that removes implicit
    /// `{coll}_{field}_seq` sequences for SERIAL columns.
    DeleteSequence {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_descriptor_version: u64,
        target_hlc: nodedb_types::Hlc,
    },
    /// Runtime state (current value, is_called, epoch, period_key). Used by
    /// ALTER SEQUENCE RESTART to propagate the new counter across nodes.
    PutSequenceState(Box<SequenceState>),

    // ── Trigger ────────────────────────────────────────────────────
    /// CREATE [OR REPLACE] TRIGGER, and ALTER TRIGGER ENABLE/DISABLE.
    PutTrigger(Box<StoredTrigger>),
    DeleteTrigger {
        database_id: DatabaseId,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_descriptor_version: u64,
        target_hlc: nodedb_types::Hlc,
    },

    // ── Function ───────────────────────────────────────────────────
    /// CREATE [OR REPLACE] FUNCTION. WASM bytes travel in the transient
    /// `StoredFunction::wasm_module` and install before metadata persists.
    PutFunction(Box<StoredFunction>),
    DeleteFunction {
        database_id: DatabaseId,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_descriptor_version: u64,
        target_hlc: nodedb_types::Hlc,
    },

    // ── Procedure ──────────────────────────────────────────────────
    /// CREATE PROCEDURE. Post-apply clears `block_cache` so the next CALL
    /// re-parses the body.
    PutProcedure(Box<StoredProcedure>),
    DeleteProcedure {
        database_id: DatabaseId,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_descriptor_version: u64,
        target_hlc: nodedb_types::Hlc,
    },

    // ── Schedule ───────────────────────────────────────────────────
    /// Scheduled-job definition. Post-apply syncs `schedule_registry` so every
    /// node's cron executor picks it up immediately.
    PutSchedule(Box<ScheduleDef>),
    DeleteSchedule {
        database_id: DatabaseId,
        tenant_id: u64,
        name: String,
    },

    // ── Synonym group ──────────────────────────────────────────────
    /// Post-apply syncs the in-memory `synonym_registry`.
    PutSynonymGroup(Box<StoredSynonymGroup>),
    /// Post-apply removes it from `synonym_registry`.
    DeleteSynonymGroup {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_hlc: nodedb_types::Hlc,
    },

    // ── Custom type ────────────────────────────────────────────────
    /// Enum or composite type. Post-apply syncs `custom_type_registry`.
    PutCustomType(Box<StoredCustomType>),
    /// Post-apply removes it from `custom_type_registry`.
    DeleteCustomType {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },

    // ── Change stream ──────────────────────────────────────────────
    /// CDC stream definition. Post-apply syncs `stream_registry` so every node
    /// starts buffering matching WriteEvents.
    PutChangeStream(Box<ChangeStreamDef>),
    /// Removes the definition and tears down its buffer via
    /// `cdc_router.remove_buffer`.
    DeleteChangeStream {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_hlc: nodedb_types::Hlc,
    },

    // ── User ───────────────────────────────────────────────────────
    /// The leader builds the full record (Argon2 hash, SCRAM salt, user_id) via
    /// `CredentialStore::prepare_user`; followers bump their `next_user_id`.
    PutUser(Box<StoredUser>),
    /// Removes the identity from every node's cache and redb catalog, freeing
    /// the username for reuse.
    DropUser {
        username: String,
    },

    // ── Role ───────────────────────────────────────────────────────
    /// Custom roles only. Built-in roles are hardcoded in `identity.rs`.
    PutRole(Box<StoredRole>),
    /// Does not cascade to grants that reference the role.
    DeleteRole {
        name: String,
    },

    // ── ApiKey ─────────────────────────────────────────────────────
    /// The leader builds the record (SHA-256 secret_hash) via
    /// `ApiKeyStore::prepare_key`. The plaintext secret NEVER enters raft.
    PutApiKey(Box<StoredApiKey>),
    /// Sets `is_revoked = true` and rewrites the row, preserving it for audit.
    RevokeApiKey {
        key_id: String,
    },

    // ── Auth user ──────────────────────────────────────────────────
    /// Externally-authenticated (`_system.auth_users`) record, proposed by
    /// auto-escalation on a `Suspended` / `Banned` verdict.
    ///
    /// The proposer has already written and installed the record: an
    /// enforcement decision must hold even if replication is unavailable.
    /// Apply is an idempotent upsert on every node, proposer included.
    PutAuthUser(Box<StoredAuthUser>),

    // ── Materialized View ──────────────────────────────────────────
    /// The Data Plane refresh loop picks up the definition on its next tick.
    PutMaterializedView(Box<StoredMaterializedView>),
    /// Removes the definition and its implementation-owned target collection as
    /// one mutation. Post-apply waits for Data Plane reclaim before advancing
    /// the applied index, so a same-name re-CREATE starts fresh.
    DeleteMaterializedView {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_descriptor_version: u64,
        target_hlc: nodedb_types::Hlc,
    },
    // ── Continuous Aggregate ───────────────────────────────────────
    /// Writes the catalog row plus the owner row. Post-apply re-dispatches
    /// `MetaOp::RegisterContinuousAggregate` to the local Data Plane.
    PutContinuousAggregate(Box<StoredContinuousAggregate>),
    /// The target collection holding materialized rows is NOT deleted —
    /// operators drop it separately, mirroring the materialized-view contract.
    DeleteContinuousAggregate {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_descriptor_version: u64,
        target_hlc: nodedb_types::Hlc,
    },

    // ── Tenant ─────────────────────────────────────────────────────
    /// Quotas are not part of `StoredTenant` and replicate separately.
    /// Post-apply seeds default quota so reads work right after creation.
    PutTenant(Box<StoredTenant>),
    /// Atomically create a tenant and its authoritative administrator.
    PutTenantWithAdmin {
        tenant: Box<StoredTenant>,
        admin: Box<StoredUser>,
    },
    /// Hard-deletes the identity record only. Tenant data is purged separately
    /// by the `PURGE TENANT CONFIRM` Data Plane meta op.
    DeleteTenant {
        tenant_id: u64,
    },

    // ── RLS policy ─────────────────────────────────────────────────
    /// The leader serializes the runtime `RlsPolicy` into `StoredRlsPolicy`;
    /// followers re-hydrate via `to_runtime()` in post_apply.
    PutRlsPolicy(Box<StoredRlsPolicy>),
    /// Keyed by `(tenant_id, collection, name)`.
    DeleteRlsPolicy {
        tenant_id: u64,
        collection: String,
        name: String,
    },

    // ── Redaction policy ──────────────────────────────────────────
    /// The leader flattens the runtime `RedactionPolicy` rule list into
    /// `StoredRedactionPolicy`; followers re-hydrate via `to_runtime()`.
    PutRedactionPolicy(Box<StoredRedactionPolicy>),
    /// Keyed by `(tenant_id, collection, for_role)`.
    DeleteRedactionPolicy {
        tenant_id: u64,
        collection: String,
        for_role: String,
    },

    // ── Permission grant ───────────────────────────────────────────
    /// `GRANT <perm> ON <target> TO <grantee>`. The catalog row is
    /// authoritative; `PermissionStore.grants` is rebuilt from it on apply.
    PutPermission(Box<StoredPermission>),
    /// Keyed by `(target, grantee, permission)`. `permission` is the lowercase
    /// canonical name (`read|write|create|drop|alter|admin|monitor|execute`).
    DeletePermission {
        target: String,
        grantee: String,
        permission: String,
    },

    // ── Database lifecycle ─────────────────────────────────────────
    /// `CREATE DATABASE`, `ALTER DATABASE RENAME`, `SET QUOTA`, `MATERIALIZE`,
    /// `PROMOTE`.
    PutDatabase(Box<crate::control::security::catalog::database_types::DatabaseDescriptor>),
    /// `DROP DATABASE`: removes the descriptor, its `_system.databases_by_name`
    /// row, and its quota and mirror rows. The objects inside the database,
    /// arrays included, travel as their own deletes in the same commit, ahead
    /// of this entry.
    DeleteDatabase {
        /// Numeric database id.
        db_id: u64,
    },
    /// Database-level grant in `_system.database_grants`, keyed by
    /// `(db_id, user_id, privilege)`.
    PutDatabaseGrant {
        db_id: u64,
        user_id: u64,
        privilege: String,
    },
    DeleteDatabaseGrant {
        db_id: u64,
        user_id: u64,
        privilege: String,
    },

    // ── Index registry ─────────────────────────────────────────────
    /// Written by every `CREATE [<kind>] INDEX` path, whatever engine backs it,
    /// so the index is listable and droppable by name on every node.
    PutIndexRecord(Box<StoredIndexRecord>),
    /// Keyed by `(database_id, tenant_id, name)`. The DROP handler performs the
    /// kind-specific teardown before proposing this.
    DeleteIndexRecord {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Not needed to locate the record. It lets post-apply invalidate exactly
        /// the cached plans still holding an `IndexLookup` on the dropped index.
        collection: String,
    },

    // ── Object ownership ───────────────────────────────────────────
    /// Orphan path only: objects with no replicated parent variant (indexes,
    /// spatial indexes, `ALTER OBJECT OWNER`). A parent `Stored*` carrying an
    /// `owner` field replicates ownership through its own post_apply instead.
    PutOwner(Box<StoredOwner>),
    /// Keyed by database-scoped object identity.
    DeleteOwner {
        object_type: String,
        database_id: u64,
        tenant_id: u64,
        object_name: String,
    },

    // ── Move Tenant lifecycle ──────────────────────────────────────
    /// The single proposal that makes the `MOVE TENANT` cutover atomic: writes
    /// each collection to `target_db_id`, then deletes it from `source_db_id`.
    ///
    /// Built after snapshot succeeds, and self-contained so any follower
    /// replays it without external lookups.
    MoveTenantCutover {
        tenant_id: u64,
        source_db_id: u64,
        target_db_id: u64,
        /// Collections serialized at their source state. Each is re-keyed to
        /// `target_db_id` on apply.
        collections: Vec<StoredCollection>,
    },

    // ── OIDC provider lifecycle ────────────────────────────────────
    /// `CREATE / ALTER OIDC PROVIDER`. Post-apply refreshes
    /// `oidc_provider_cache`.
    PutOidcProvider(Box<StoredOidcProvider>),
    DeleteOidcProvider {
        name: String,
    },

    // ── WAL replay tombstone ───────────────────────────────────────
    /// Records (or raises) a per-(database, tenant, collection) replay barrier.
    /// Replicated on RESTORE so `purge_lsn` matches everywhere — otherwise
    /// purged writes resurrect on follower restart. Idempotent and monotone.
    RecordWalTombstone {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        purge_lsn: u64,
    },

    // ── Clone lifecycle ────────────────────────────────────────────
    /// Records a new CoW clone database as one unit: writes the target
    /// descriptor (`status = Cloning`, `parent_clone` set) into
    /// `_system.databases`, and adds `target_db_id` to the `clone_lineage`
    /// children of `source_db_id`.
    ///
    /// Built after `as_of_lsn` resolves and `target_db_id` is allocated, so any
    /// follower replays it without external lookups.
    CloneDatabase {
        /// The descriptor for the newly created target database.
        target_descriptor:
            Box<crate::control::security::catalog::database_types::DatabaseDescriptor>,
        /// Numeric id of the source database (for lineage update).
        source_db_id: u64,
        /// The incarnation every shadow collection of the clone takes. A
        /// shadow is a new collection under a new key, never the source's
        /// incarnation. The proposer stamps it.
        incarnation: nodedb_types::Hlc,
    },

    /// Streaming MV definition plus its database-scoped owner row.
    PutStreamingMaterializedView(Box<crate::event::streaming_mv::StreamingMvDef>),
    /// Streaming MVs are Event-Plane objects, so this removes both the
    /// database-scoped catalog record and the in-memory registry entry.
    DeleteStreamingMaterializedView {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },

    // ── Scope grant ────────────────────────────────────────────────
    /// `GRANT SCOPE`, and `RENEW SCOPE` as the same upsert with a later
    /// `expires_at`. The catalog row is authoritative; `ScopeGrantStore` is
    /// installed from it on apply, so the grant authorizes identically
    /// everywhere.
    PutScopeGrant(Box<StoredScopeGrant>),
    /// Keyed by `(scope_name, grantee_type, grantee_id)`. `grantee_type` is the
    /// lowercase form (`user|role|org|team`).
    DeleteScopeGrant {
        scope_name: String,
        grantee_type: String,
        grantee_id: String,
    },

    // ── Resource quota ─────────────────────────────────────────────
    /// Database quota row in `_system.database_quotas`. The leader checks the
    /// global ceiling before proposing; post-apply pushes it into enforcement.
    PutDatabaseQuota {
        db_id: u64,
        record: Box<nodedb_types::QuotaRecord>,
    },
    /// Enforcement falls back to `QuotaRecord::DEFAULT` on every node.
    DeleteDatabaseQuota {
        db_id: u64,
    },
    /// Tenant quota row in `_system.tenant_quotas`, keyed by
    /// `(db_id, tenant_id)`.
    PutTenantQuota {
        db_id: u64,
        tenant_id: u64,
        record: Box<nodedb_types::QuotaRecord>,
    },
    /// Keyed by `(db_id, tenant_id)`.
    DeleteTenantQuota {
        db_id: u64,
        tenant_id: u64,
    },

    // ── Scope token quota ──────────────────────────────────────────
    /// Per-scope token quota row in `_system.scope_quotas`. The leader parses
    /// and range-checks it; post-apply installs it in every `QuotaManager`.
    PutScopeQuota(Box<StoredScopeQuota>),
    /// Drops the row and the in-memory definition on every node.
    DeleteScopeQuota {
        scope_name: String,
    },

    // ── Retention policy ───────────────────────────────────────────
    /// Retention policy row in `_system.retention_policies`, keyed by
    /// `(database_id, tenant_id, name)`. CREATE and ALTER both ship the
    /// full record; post-apply installs it in every node's registry.
    PutRetentionPolicy(Box<RetentionPolicyDef>),
    /// Drops the row and the registry entry on every node.
    DeleteRetentionPolicy {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Target collection, carried so plan-cache eviction stays precise.
        collection: String,
    },

    // ── Alert rule ─────────────────────────────────────────────────
    /// Alert rule row in `_system.alert_rules`, keyed by
    /// `(database_id, tenant_id, name)`. CREATE and ALTER both ship the full
    /// record; post-apply installs it in every node's `AlertRegistry`.
    PutAlertRule(Box<AlertDef>),
    /// Drops the row, the registry entry, and the hysteresis state on every
    /// node.
    DeleteAlertRule {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },

    // ── Topic ──────────────────────────────────────────────────────
    /// Create-only topic row in `_system.topics_ep`. Apply keeps an existing
    /// definition, so a replay never rewinds its durable high-water marks.
    CreateTopicIfAbsent(Box<TopicDef>),
    /// Drops the topic row, its retained messages, and every consumer-group
    /// row attached to it, in one catalog transaction.
    DeleteTopicWithConsumerGroups {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_hlc: nodedb_types::Hlc,
    },

    // ── Consumer group ─────────────────────────────────────────────
    /// Create-only consumer-group row in `_system.consumer_groups`. The leader
    /// reports the duplicate; apply stays idempotent under replay.
    PutConsumerGroupIfAbsent(Box<ConsumerGroupDef>),
    /// Drops the canonical row, and the legacy unscoped row under DEFAULT.
    DeleteConsumerGroup {
        database_id: u64,
        tenant_id: u64,
        stream_name: String,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_hlc: nodedb_types::Hlc,
    },
    /// Re-keys a bare-topic group row onto its canonical `topic:<name>` stream.
    /// `def` carries the row as the canonical key stores it; `legacy_stream` is
    /// the bare topic name of the row it replaces.
    MigrateConsumerGroupStream {
        def: Box<ConsumerGroupDef>,
        legacy_stream: String,
    },

    // ── Version-history checkpoint ─────────────────────────────────
    /// Named checkpoint row in `_system.checkpoints`, keyed by
    /// `(database_id, tenant_id, collection, doc_id, checkpoint_name)`.
    PutCheckpoint(Box<CheckpointRecord>),
    /// Drops one checkpoint row. The leader reports a missing checkpoint, so
    /// apply stays idempotent under replay.
    DeleteCheckpoint {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        doc_id: String,
        checkpoint_name: String,
    },
    /// One `COMPACT HISTORY` statement: drops every checkpoint for the
    /// document below `before_timestamp`, and compacts the document's oplog
    /// to `target_version_json` on every node.
    ///
    /// The checkpoint delete and the compaction travel together because they
    /// are one operator action. The target rides in the entry because apply
    /// deletes the checkpoint row that holds it.
    ///
    /// `database_id` scopes both halves: it keys the checkpoint rows apply
    /// removes, and it names the collection post-apply compacts.
    CompactHistory {
        tenant_id: u64,
        database_id: u64,
        collection: String,
        doc_id: String,
        /// Exclusive boundary. A checkpoint stamped exactly here survives.
        before_timestamp: u64,
        /// Loro version vector every node compacts to.
        target_version_json: String,
    },

    // ── Vector model metadata / index parameters ───────────────────
    /// Embedding-model row in `_system.vector_model_metadata`, keyed by
    /// `(database_id, tenant_id, collection, column)`. A re-delivery rewrites
    /// the row.
    PutVectorModel(Box<VectorModelEntry>),
    /// Drops one column's embedding-model row. Apply treats a missing row
    /// as a no-op, so replay on every node stays idempotent.
    DeleteVectorModel {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        column: String,
    },
    /// Build-parameter row in `_system.vector_index_params`, keyed by
    /// `(database_id, tenant_id, collection, field_name)`.
    PutVectorIndexParams(Box<StoredVectorIndexParams>),
    /// Drops one vector index's build parameters. The leader reports the
    /// missing index, so apply stays idempotent under replay.
    DeleteVectorIndexParams {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        field_name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_hlc: nodedb_types::Hlc,
    },

    // ── Column statistics ──────────────────────────────────────────
    /// Statistics rows in `_system.column_stats` for one collection, keyed by
    /// `(database_id, tenant_id, collection, column)`. ANALYZE scans every
    /// vShard, so the numbers describe the whole collection and every node
    /// plans from the same figures. One entry carries every column so a
    /// planner never sees a subset and costs against it.
    PutColumnStats(Box<Vec<StoredColumnStats>>),

    // ── Clone copy-on-write rows ───────────────────────────────────
    // Each names the clone collection by `(database_id, tenant_id,
    // collection)`. Apply writes only while that collection is still a clone,
    // so a replay after its materialization or purge is a no-op.
    /// Maps a source row a clone copied up to its target surrogate.
    PutCloneCopyup {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        source_surrogate: u32,
        target_surrogate: u32,
    },
    /// Hides a source row, by its surrogate, from the clone's reads.
    PutCloneTombstone {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        source_surrogate: u32,
    },
    /// Hides a source KV key from the clone's reads.
    PutKvCloneTombstone {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        kv_key: String,
    },

    // ── Clone source drain claims ──────────────────────────────────
    /// A materialization claims its KV source's drain. Written before the
    /// drain starts, so a later singleton worker can end an orphaned drain.
    PutCloneSourceDrain(
        Box<crate::control::security::catalog::clone_source_drains::CloneSourceDrain>,
    ),
    /// Releases a claim once its drain ended or its copy is no longer needed.
    DeleteCloneSourceDrain {
        clone_database: u64,
        tenant_id: u64,
        clone_collection: String,
    },

    // ── Array ──────────────────────────────────────────────────────
    /// CREATE ARRAY and ALTER ARRAY: the full stored definition. Post-apply
    /// opens the array on every core of every node.
    PutArray(Box<crate::control::array_catalog::ArrayCatalogEntry>),
    /// DROP ARRAY, the DROP DATABASE teardown, and the source side of a MOVE
    /// TENANT rekey. Removes the row and its surrogate bindings.
    DeleteArray {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Incarnation this delete targets, frozen at propose time from the
        /// stored row. `Hlc::ZERO` applies unfenced.
        target_hlc: nodedb_types::Hlc,
        /// MOVE TENANT only. Apply rekeys the surrogate bindings to the
        /// target database, and post-apply renames the cell store there
        /// instead of purging it.
        moved_to: Option<crate::control::array_catalog::ArrayMove>,
    },

    // ── Consumer offsets ───────────────────────────────────────────
    /// Raises one consumer group's committed offsets on every node, so a
    /// consumer that moves to another node resumes where it committed. A
    /// node whose registered group is another incarnation than the commit's
    /// `group_hlc` skips the entry, so a replayed commit of a dropped group
    /// never moves a recreated group's cursor. Apply only raises offsets, so
    /// a replay is a no-op.
    CommitConsumerOffsets(Box<OffsetCommit>),

    // ── Scheduled backups ──────────────────────────────────────────
    /// Raises one scheduled backup's mark on every node, so the node that
    /// runs scheduled backups after a leader change neither skips a due
    /// minute nor repeats a finished one. The mark is keyed by the schedule's
    /// config incarnation, and apply only raises it, so a replay is a no-op.
    PutBackupScheduleMark(Box<StoredBackupScheduleMark>),

    // ── User-defined CRSs ──────────────────────────────────────────
    /// `CREATE SPATIAL_REF_SYS`: one user-defined CRS. Apply defines it in
    /// the catalog's set, and leaves the set unchanged when the code is live
    /// or dropped. Post-apply sends the set to every core of every node.
    PutSpatialRefSys(Box<nodedb_crs::CrsDef>),
    /// `DROP SPATIAL_REF_SYS`: retire a user-defined CRS. The code stays a
    /// tombstone and is never reused, so stored data that names it fails to
    /// resolve instead of changing meaning.
    DropSpatialRefSys {
        srid: nodedb_crs::Srid,
    },

    // ── Credentials ────────────────────────────────────────────────
    /// `CREATE CREDENTIAL`: one credential, its secret sealed before the
    /// proposal so the log holds ciphertext only. Apply writes the row when
    /// the name is free and leaves a live row unchanged.
    PutCredential(Box<crate::control::security::source_credential::StoredCredential>),
    /// `DROP CREDENTIAL`: remove one credential.
    DropCredential {
        tenant_id: u64,
        name: String,
    },
}
