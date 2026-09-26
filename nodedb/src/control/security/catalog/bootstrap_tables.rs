// SPDX-License-Identifier: BUSL-1.1

//! Canonical registry of `_system.*` redb tables created at bootstrap.
//!
//! `SystemCatalog::open` iterates [`BOOTSTRAP_TABLES`] and opens each
//! entry inside the init write transaction — opening a table in redb
//! creates it if absent. Holding the list here, instead of an inline
//! sequence of `open_table` calls in `open`, makes it structurally
//! impossible to declare a table and read it in production code without
//! also creating it at startup: a table that is consulted on a fresh
//! catalog but missing from this list would fail the first reader with
//! `Table '…' does not exist`. The `bootstrap_creates_every_registered_table`
//! unit test re-opens every entry read-only against a freshly-bootstrapped
//! catalog to keep the registry and the init path in lockstep.
//!
//! Migration-only tables (`_system.collections`, `_system.surrogate_pk`,
//! `_system.surrogate_pk_rev`, `_system.surrogate_pk_v2`,
//! `_system.surrogate_pk_rev_v2` — superseded key layouts) are intentionally
//! absent: they are read only by the idempotent migration path, which already
//! tolerates their absence, and materialising empty copies on every fresh
//! server would misrepresent the catalog state.

use redb::{ReadTransaction, TableError, WriteTransaction};

use super::tables::*;

/// One bootstrap table: a short label for diagnostics plus thunks that
/// open the table in a write transaction (creating it) or a read
/// transaction (probing its existence). The thunks erase the table's
/// concrete `TableDefinition<K, V>` key/value types so tables with
/// different shapes share one homogeneous slice.
pub(super) struct BootstrapTable {
    /// Short, stable identifier used in error messages and test output.
    pub(super) label: &'static str,
    /// Open — and therefore create — the table in a write transaction.
    pub(super) create: fn(&WriteTransaction) -> Result<(), TableError>,
    /// Open the table read-only; fails with `TableDoesNotExist` if it was
    /// never created. Used by `SystemCatalog::open` to decide whether the
    /// catalog needs bootstrapping, and by the bootstrap-completeness test.
    pub(super) probe: fn(&ReadTransaction) -> Result<(), TableError>,
}

macro_rules! bootstrap_tables {
    ($($label:literal => $def:expr),+ $(,)?) => {
        &[$(
            BootstrapTable {
                label: $label,
                create: |txn| txn.open_table($def).map(|_| ()),
                probe: |txn| txn.open_table($def).map(|_| ()),
            }
        ),+]
    };
}

/// Every `_system.*` table created unconditionally on `SystemCatalog::open`.
pub(super) const BOOTSTRAP_TABLES: &[BootstrapTable] = bootstrap_tables![
    // ── Auth / tenancy ──
    "users" => USERS,
    "api_keys" => API_KEYS,
    "roles" => ROLES,
    "permissions" => PERMISSIONS,
    "owners" => OWNERS,
    "tenants" => TENANTS,
    "audit_log" => AUDIT_LOG,
    "blacklist" => BLACKLIST,
    "auth_users" => AUTH_USERS,
    "lockout_state" => super::lockout::LOCKOUT_STATE_TABLE,
    "orgs" => ORGS,
    "org_members" => ORG_MEMBERS,
    "scopes" => SCOPES,
    "scope_grants" => SCOPE_GRANTS,
    "scope_quotas" => SCOPE_QUOTAS,
    "oidc_providers" => OIDC_PROVIDERS,
    // ── Collections + storage bookkeeping ──
    "collections" => COLLECTIONS,
    "metadata" => METADATA,
    "wal_tombstones" => WAL_TOMBSTONES,
    "tenant_group_marks" => super::tenant_group_marks::TENANT_GROUP_MARKS,
    "calvin_applied" => super::calvin_applied::CALVIN_APPLIED,
    "l2_cleanup_queue" => L2_CLEANUP_QUEUE,
    "pending_reclaim" => PENDING_RECLAIM,
    "column_stats" => COLUMN_STATS,
    "vector_model_metadata" => VECTOR_MODEL_METADATA,
    "vector_index_params" => VECTOR_INDEX_PARAMS,
    "index_registry" => INDEX_REGISTRY,
    "checkpoints" => CHECKPOINTS,
    // ── Surrogate identity map ──
    "surrogate_pk_v3" => SURROGATE_PK_V3,
    "surrogate_pk_rev_v3" => SURROGATE_PK_REV_V3,
    "surrogate_hwm" => super::surrogate_hwm::SURROGATE_HWM,
    "surrogate_reserve_index" => super::surrogate_hwm::SURROGATE_RESERVE_INDEX,
    // ── Sync producer registry ──
    "sync_producer_hwm" => super::sync_producer::SYNC_PRODUCER_HWM,
    "sync_producers" => super::sync_producer::SYNC_PRODUCERS,
    "sync_peer_bindings" => super::sync_producer::SYNC_PEER_BINDINGS,
    "crdt_signing_keys" => super::sync_producer::CRDT_SIGNING_KEYS,
    "crdt_signing_root_metadata" => super::sync_producer::CRDT_SIGNING_ROOT_METADATA,
    "join_token_states" => super::sync_producer::JOIN_TOKEN_STATES,
    "enrollment_preauthorizations" => super::sync_producer::ENROLLMENT_PREAUTHORIZATIONS,
    // ── DDL objects ──
    "materialized_views" => MATERIALIZED_VIEWS,
    "continuous_aggregates" => CONTINUOUS_AGGREGATES,
    "functions" => FUNCTIONS,
    "procedures" => PROCEDURES,
    "triggers" => TRIGGERS,
    "arrays" => ARRAYS,
    "dependencies" => DEPENDENCIES,
    "sequences" => SEQUENCES,
    "sequence_state" => SEQUENCE_STATE,
    "synonym_groups" => SYNONYM_GROUPS,
    "custom_types" => CUSTOM_TYPES,
    "custom_type_oid_hwm" => super::custom_type_oid_hwm::CUSTOM_TYPE_OID_HWM,
    "wasm_modules" => WASM_MODULES,
    "rls_policies" => super::rls::RLS_POLICIES,
    "redaction_policies" => super::redaction::REDACTION_POLICIES,
    // ── Event Plane ──
    "change_streams" => CHANGE_STREAMS,
    "consumer_groups" => CONSUMER_GROUPS,
    "schedules" => SCHEDULES,
    "retention_policies" => RETENTION_POLICIES,
    "alert_rules" => ALERT_RULES,
    "topics_ep" => TOPICS_EP,
    "topic_messages" => TOPIC_MESSAGES,
    "streaming_mvs" => STREAMING_MVS,
    // ── Database catalog + quotas ──
    "databases" => DATABASES,
    "databases_by_name" => DATABASES_BY_NAME,
    "database_hwm" => DATABASE_HWM,
    "database_grants" => DATABASE_GRANTS,
    "database_quotas" => DATABASE_QUOTAS,
    "tenant_quotas" => TENANT_QUOTAS,
    // ── Clone CoW + mirror ──
    "clone_copyups" => CLONE_COPYUPS,
    "clone_tombstones" => CLONE_TOMBSTONES,
    "clone_kv_tombstones" => CLONE_KV_TOMBSTONES,
    "clone_lineage" => CLONE_LINEAGE,
    "mirror_collection_map" => MIRROR_COLLECTION_MAP,
    "mirror_lag" => MIRROR_LAG,
    // ── Tenant relocation ──
    "move_tenant_journal" => super::move_tenant_journal_types::MOVE_TENANT_JOURNAL,
];
