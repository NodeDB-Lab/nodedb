// SPDX-License-Identifier: BUSL-1.1

//! redb `TableDefinition` constants for every system-catalog table.
//!
//! Constants are `pub(super)` so sibling catalog modules can use them
//! directly; `types.rs` re-exports the set for code that imports via
//! `super::types::*`.

use redb::TableDefinition;

// ── Auth / tenancy ────────────────────────────────────────────────────

/// Table: username (string) -> MessagePack-serialized user record.
pub(super) const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.users");

/// Table: key_id (string) -> MessagePack-serialized API key record.
pub(super) const API_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.api_keys");

/// Table: tenant_id (string) -> MessagePack-serialized tenant record.
pub(super) const TENANTS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.tenants");

/// Table: seq (u64 as big-endian bytes) -> MessagePack-serialized audit entry.
pub(super) const AUDIT_LOG: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("_system.audit_log");

/// Table: DML audit position index.
/// Key: event core (u32 BE) | event LSN (u64 BE) | audit seq (u64 BE). Value: empty.
/// It holds one row per `DmlAudit` entry whose detail names an event key.
pub(super) const AUDIT_DML_POSITIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("_system.audit_dml_positions");

/// Table: role_name -> MessagePack-serialized custom role record.
pub(super) const ROLES: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.roles");

/// Table: "target:role_or_user" -> MessagePack-serialized permission grant.
pub(super) const PERMISSIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.permissions");

/// Table: "{object_type}:{database_id}:{tenant_id}:{object_name}" -> owner username.
pub(super) const OWNERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.owners");

// ── Collections ───────────────────────────────────────────────────────

/// Table: `(database_id: u64, "{tenant_id}:{name}")` -> MessagePack collection metadata.
///
/// The compound key prepends `database_id` (as raw `u64`) so every collection
/// is namespaced within its database. The inner key preserves the legacy
/// `"{tenant_id}:{name}"` encoding for catalog-resolver compatibility.
pub(super) const COLLECTIONS: TableDefinition<(u64, &str), &[u8]> =
    TableDefinition::new("_system.collections_v2");

/// Table: `(database_id, tenant_id, collection_name)` -> `purge_lsn` (u64 LE).
///
/// Holds the canonical collection-tombstone set used by WAL replay to
/// shadow writes that precede a hard-delete. Persisted here (rather
/// than only on the WAL) so startup replay is O(1) instead of requiring
/// a full segment scan to rebuild the set. Entries are GC'd by
/// `delete_wal_tombstones_before_lsn` when all segments referencing
/// them have been truncated past retention.
pub(super) const WAL_TOMBSTONES: TableDefinition<(u64, u64, &str), u64> =
    TableDefinition::new("_system.wal_tombstones");

/// Table: `(database_id, tenant_id, collection_name)` -> MessagePack-serialized
/// `StoredL2CleanupEntry`.
///
/// Populated when a collection hard-delete finishes on this node but
/// L2 (S3) object delete has not completed yet. Drained by the L2
/// cleanup worker as `DELETE` calls succeed. Surfaced via the
/// `_system.l2_cleanup_queue` virtual view so operators can see the
/// object-store delete backlog even after the `StoredCollection` row
/// has been purged.
pub(super) const L2_CLEANUP_QUEUE: TableDefinition<(u64, u64, &str), &[u8]> =
    TableDefinition::new("_system.l2_cleanup_queue");

/// Table: `(tenant_id, collection_name)` -> MessagePack-serialized
/// `StoredPendingReclaim`.
///
/// Populated when the synchronous, result-checked engine purge for a
/// dropped collection (`clear_collection_all_engines` via
/// `MetaOp::UnregisterCollection`) FAILS on this node after the catalog
/// row has already been removed. Left unrecorded, that failure
/// leaves engine storage rows behind a gone catalog row — permanent
/// divergence that resurrects the dropped collection's history on
/// re-CREATE. A Tokio worker (and a boot-time drain) retries the engine
/// purge for each entry until it succeeds, then removes the row. This
/// gives durable at-least-once purge without wedging the node.
///
/// Table: `{database_id}:{tenant_id}:{collection}` -> MessagePack-serialized
/// pending-reclaim record.
pub(super) const PENDING_RECLAIM: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.pending_reclaim");

// ── DDL objects ───────────────────────────────────────────────────────

/// Table: "{database_id}:{tenant_id}:{name}" -> MessagePack-serialized materialized view metadata.
pub(super) const MATERIALIZED_VIEWS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.materialized_views");

/// Table: `(database_id: u64, "{tenant_id}:{name}")` -> MessagePack-serialized continuous aggregate metadata.
pub(super) const CONTINUOUS_AGGREGATES: TableDefinition<(u64, &str), &[u8]> =
    TableDefinition::new("_system.continuous_aggregates");

/// Table: `v2:{tenant_id}:{database_id}:{name}` -> MessagePack-serialized
/// user function definition.
pub(super) const FUNCTIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.functions");

/// Table: `v2:{tenant_id}:{database_id}:{name}` -> MessagePack-serialized
/// trigger definition.
pub(super) const TRIGGERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.triggers");

/// Table: `\0v2:{tenant_id}:{database_id}:{name}` -> MessagePack-serialized
/// `ArrayCatalogEntry`. One row per ND array registered via DDL.
pub(super) const ARRAYS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.arrays");

// ── Surrogate PK map ──────────────────────────────────────────────────

/// Table: `(database_id, tenant_id, collection)` -> surrogate scope id.
///
/// A scope id names one collection's PK map. The map tables key by the id,
/// so a key compare reads no collection name.
pub(super) const SURROGATE_SCOPES: TableDefinition<(u64, u64, &str), u64> =
    TableDefinition::new("_system.surrogate_scopes");

/// Singleton table: the next unused surrogate scope id. Ids start at 1.
pub(super) const SURROGATE_SCOPE_SEQ: TableDefinition<(), u64> =
    TableDefinition::new("_system.surrogate_scope_seq");

/// Table: `scope_id` (8 bytes big-endian) followed by the encoded pk bytes ->
/// `Surrogate` (u32).
///
/// Forward direction of the PK ↔ Surrogate map. The key is one byte string,
/// so a compare is one `memcmp`, and one scope's rows are one key range.
/// Every binding writes the forward and the reverse row in one redb txn.
pub(super) const SURROGATE_PK: TableDefinition<&[u8], u32> =
    TableDefinition::new("_system.surrogate_pk_v4");

/// Table: `(scope_id << 32) | surrogate` -> encoded pk bytes.
///
/// Reverse direction of `_system.surrogate_pk_v4`. One scope's rows are one
/// key range, in surrogate order.
pub(super) const SURROGATE_PK_REV: TableDefinition<u128, &[u8]> =
    TableDefinition::new("_system.surrogate_pk_rev_v4");

// ── Event Plane ───────────────────────────────────────────────────────

/// Table: `v2/{database_id}/{tenant_id}/{len}/{hex(stream_name)}` ->
/// MessagePack-serialized ChangeStreamDef.
pub(super) const CHANGE_STREAMS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.change_streams");

/// Table: `v2:{database_id}:{tenant_id}:{len}:{stream}:{len}:{group}` ->
/// MessagePack-serialized ConsumerGroupDef.
pub(super) const CONSUMER_GROUPS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.consumer_groups");

/// Table: `v2:{tenant_id}:{database_id}:{schedule_name}` ->
/// MessagePack-serialized ScheduleDef.
pub(super) const SCHEDULES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.schedules");

/// Table: `(database_id: u64, "{tenant_id}:{policy_name}")` -> MessagePack-serialized RetentionPolicyDef.
pub(super) const RETENTION_POLICIES: TableDefinition<(u64, &str), &[u8]> =
    TableDefinition::new("_system.retention_policies");

/// Table: `(database_id: u64, "{tenant_id}:{alert_name}")` -> MessagePack-serialized AlertDef.
pub(super) const ALERT_RULES: TableDefinition<(u64, &str), &[u8]> =
    TableDefinition::new("_system.alert_rules");

/// Table: `v2/{database_id}/{tenant_id}/{len}/{hex(topic_name)}` ->
/// MessagePack-serialized TopicDef.
pub(super) const TOPICS_EP: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.topics_ep");

/// Table: `[database_id: be u64][tenant_id: be u64][name_len: be u16]
/// [topic_name bytes][sequence: be u64]` -> MessagePack-serialized TopicMessage.
///
/// The big-endian fixed-width components keep messages for one scoped topic
/// contiguous and ordered by sequence in redb's bytewise key order.
pub(super) const TOPIC_MESSAGES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("_system.topic_messages");

/// Table: `v2:{database_id}:{tenant_id}:{mv_name}` -> MessagePack-serialized
/// StreamingMvDef.
pub(super) const STREAMING_MVS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.streaming_mvs");

// ── Procedures, deps, sequences, stats ────────────────────────────────

/// Table: `v2:{tenant_id}:{database_id}:{name}` -> MessagePack-serialized
/// stored procedure definition.
pub(super) const PROCEDURES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.procedures");

/// Table: `v2:{source_type}:{tenant_id}:{database_id}:{source_name}` ->
/// MessagePack-serialized dependency list.
pub(super) const DEPENDENCIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.dependencies");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized sequence definition.
pub(super) const SEQUENCES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.sequences");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized sequence runtime state.
pub(super) const SEQUENCE_STATE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.sequence_state");

/// Table: "{database_id}:{tenant_id}:{collection}:{column}" -> MessagePack-serialized column statistics.
pub(super) const COLUMN_STATS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.column_stats");

/// Table: metadata key -> value bytes (counters, config).
pub(super) const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.metadata");

// ── Database catalog ──────────────────────────────────────────────────

/// Table: `database_id (u64)` -> MessagePack-serialized `DatabaseDescriptor`.
/// One row per database; `DatabaseId(0)` = "default" always exists.
pub(super) const DATABASES: TableDefinition<u64, &[u8]> = TableDefinition::new("_system.databases");

/// Table: `name (string)` -> `database_id (u64)`.
/// Unique reverse lookup: name → DatabaseId. Updated atomically with
/// `DATABASES` on CREATE/RENAME/DROP.
pub(super) const DATABASES_BY_NAME: TableDefinition<&str, u64> =
    TableDefinition::new("_system.databases_by_name");

/// Table: `"global"` -> highest issued database id, `"reserve_index"` ->
/// highest metadata log index folded into it (`u64`). Written on every
/// allocation; seeds `DatabaseRegistry` at startup.
pub(super) const DATABASE_HWM: TableDefinition<&str, u64> =
    TableDefinition::new("_system.database_hwm");

// ── Synonyms / custom types / WASM ────────────────────────────────────

/// Table: "{database_id}:{tenant_id}:{group_name}" -> MessagePack-serialized `StoredSynonymGroup`.
pub(super) const SYNONYM_GROUPS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.synonym_groups");

/// Table: "{database_id}:{tenant_id}:{type_name}" -> MessagePack-serialized `StoredCustomType`.
pub(super) const CUSTOM_TYPES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.custom_types");

/// Table: SRID code -> MessagePack-serialized `CustomCrs`: a user-defined
/// CRS or the tombstone of a dropped one.
pub(super) const SPATIAL_REF_SYS: TableDefinition<u32, &[u8]> =
    TableDefinition::new("_system.spatial_ref_sys");

/// Table: "wasm_module:{sha256_hex}" -> raw WASM binary bytes.
pub(super) const WASM_MODULES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.wasm_modules");

// ── Auth state (lockout, blacklist, JIT users, orgs, scopes) ──────────

/// Table: username -> MessagePack-serialized `StoredLockoutRecord`.
///
/// Persistent mirror of the in-memory `LoginAttemptTracker`. Written on every
/// failure and success; rebuilt into cache on `CredentialStore::open`.
pub(super) const LOCKOUT_STATE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.lockout_state");

/// Table: blacklist key (user_id or IP) -> MessagePack-serialized blacklist entry.
pub(super) const BLACKLIST: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.blacklist");

/// Table: scope name -> MessagePack-serialized `StoredScopeQuota`.
///
/// Quota definitions are admin-authored catalog objects, like scope grants:
/// a definition that lived only in process memory is forgotten by every
/// restart, silently lifting every cap it expressed.
pub(super) const SCOPE_QUOTAS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.scope_quotas");

/// Table: auth_user_id -> MessagePack-serialized auth user record (JIT-provisioned).
pub(super) const AUTH_USERS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.auth_users");

/// Table: org_id -> MessagePack-serialized org record.
pub(super) const ORGS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.orgs");

// ── OIDC providers ───────────────────────────────────────────────────

/// Table: provider_name (string) -> MessagePack-serialized `StoredOidcProvider`.
pub(super) const OIDC_PROVIDERS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.oidc_providers");

/// Table: "{org_id}:{user_id}" -> MessagePack-serialized org membership.
pub(super) const ORG_MEMBERS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.org_members");

/// Table: scope_name -> MessagePack-serialized scope definition.
pub(super) const SCOPES: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.scopes");

/// Table: `"{database_id}:{user_id}:{privilege}"` -> empty value.
///
/// Stores explicit per-database grants created by `GRANT … ON DATABASE …`.
/// The key encodes all three dimensions so range scans by `database_id`
/// or `user_id` prefix work without secondary tables.
pub(super) const DATABASE_GRANTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.database_grants");

/// Table: "{scope_name}:{grantee_type}:{grantee_id}" -> MessagePack-serialized scope grant.
pub(super) const SCOPE_GRANTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.scope_grants");

// ── Database and tenant quotas ────────────────────────────────────────

/// Table: `database_id (u64)` -> MessagePack-serialized `QuotaRecord`.
///
/// Stores the explicit resource budget for each database. Absence means the
/// database has no configured ceiling; enforcement falls back to global limits.
pub(super) const DATABASE_QUOTAS: TableDefinition<u64, &[u8]> =
    TableDefinition::new("_system.database_quotas");

/// Table: `(database_id: u64, tenant_id: u64)` -> MessagePack-serialized `QuotaRecord`.
///
/// Stores the resource budget for a specific tenant within a specific database.
/// The sum of all tenant quotas within a database must not exceed that database's
/// quota; this invariant is checked at write time.
pub(super) const TENANT_QUOTAS: TableDefinition<(u64, u64), &[u8]> =
    TableDefinition::new("_system.tenant_quotas");

// ── Clone CoW tables ─────────────────────────────────────────────────

/// Table: `(target_collection_key: &str, source_surrogate: u32)` → `target_surrogate: u32`.
///
/// Records copy-up events: when a row that originally existed only in the source
/// is written to in the target clone (UPDATE or DELETE on a source-only row),
/// the source surrogate is mapped to the fresh target surrogate allocated at
/// copy-up time.  The `target_collection_key` is the
/// `"{database_id}:{tenant_id}:{collection_name}"` compound form.
pub(super) const CLONE_COPYUPS: TableDefinition<(&str, u32), u32> =
    TableDefinition::new("_system.clone_copyups");

/// Table: `(target_collection_key: &str, source_surrogate: u32)` → `()`.
///
/// Records tombstone events: when a row that existed only in the source is
/// deleted from the clone, the source surrogate is recorded here.  The read
/// planner checks this table before falling back to source storage so that
/// deleted rows are invisible even though they still exist in the source.
pub(super) const CLONE_TOMBSTONES: TableDefinition<(&str, u32), ()> =
    TableDefinition::new("_system.clone_tombstones");

/// Table: `(target_collection_key: &str, kv_key: &str)` → `()`.
///
/// Records KV-engine tombstone events: when a KV row that exists only in the
/// source is deleted from the clone, the raw KV key string is recorded here.
/// The read path filters source KV scan results against this set so that
/// deleted rows remain invisible from clone queries.
pub(super) const CLONE_KV_TOMBSTONES: TableDefinition<(&str, &str), ()> =
    TableDefinition::new("_system.clone_kv_tombstones");

/// Table: `source_database_id (u64)` → MessagePack-serialized `Vec<u64>` (child DatabaseIds).
///
/// Tracks the lineage tree: for each database that is the source of one or more
/// clones, this table stores the list of child database ids.  Used at DROP
/// DATABASE time to detect orphan dependency violations and by the depth-check
/// at CLONE DATABASE time (walk upward through parent_clone links counting
/// hops).
pub(super) const CLONE_LINEAGE: TableDefinition<u64, &[u8]> =
    TableDefinition::new("_system.clone_lineage");

// ── Mirror catalog ────────────────────────────────────────────────────

/// Table: `(mirror_database_id: u64, source_collection_name: &str)` →
/// `local_collection_name: &str` (MessagePack bytes).
///
/// Maps source-side collection names to this mirror's local collection names.
/// In typical deployments the names match; the map exists so a future
/// `RENAME COLLECTION ON MIRROR` can decouple them without breaking replication.
/// Updated atomically when a DDL entry from the source is applied.
pub(super) const MIRROR_COLLECTION_MAP: TableDefinition<(u64, &str), &[u8]> =
    TableDefinition::new("_system.mirror_collection_map");

/// Table: `database_id (u64)` → MessagePack-serialized `MirrorLagRecord`.
///
/// Stores the last-applied LSN and apply-timestamp for each mirror database.
/// Read by the metrics collector to produce the `mirror_lag_ms` gauge and by
/// the `SHOW DATABASE MIRROR STATUS` handler.
pub(super) const MIRROR_LAG: TableDefinition<u64, &[u8]> =
    TableDefinition::new("_system.mirror_lag");

// ── Vector model + checkpoints ────────────────────────────────────────

/// Table: "{database_id}:{tenant_id}:{collection}:{column}" -> MessagePack-serialized VectorModelEntry.
pub(super) const VECTOR_MODEL_METADATA: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.vector_model_metadata");

/// Table: "{database_id}:{tenant_id}:{collection}:{field_name}" -> MessagePack-serialized StoredVectorIndexParams.
pub(super) const VECTOR_INDEX_PARAMS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.vector_index_params");

/// Table: "{database_id}:{tenant_id}:{index_name}" -> MessagePack StoredIndexRecord.
///
/// The identity registry for every index kind. `SHOW INDEXES` lists it,
/// `DROP INDEX` resolves against it, and collection teardown enumerates it.
pub(super) const INDEX_REGISTRY: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.index_registry");

/// Table: "{database_id}:{tenant_id}:{collection}:{doc_id}:{checkpoint_name}" -> MessagePack CheckpointRecord.
pub(super) const CHECKPOINTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.checkpoints");
