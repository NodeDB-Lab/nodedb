// SPDX-License-Identifier: BUSL-1.1

//! System catalog type façade: re-exports the records, table constants,
//! and helpers from their dedicated modules so `super::types::*` keeps
//! working for every catalog submodule.
//!
//! Layout:
//! - `tables.rs` — every `redb::TableDefinition` constant.
//! - `collection.rs` — `IndexBuildState`, `StoredIndex`, `StoredCollection`.
//! - `materialized_view.rs` — `StoredMaterializedView`.
//! - `continuous_aggregate.rs` — `StoredContinuousAggregate`.
//! - `checkpoint.rs` — `CheckpointRecord`.
//! - `checkpoints.rs` — `CheckpointDoc` plus the checkpoint operations.
//! - `auth_types.rs` — user, role, tenant, permission, audit, blacklist, owner records.
//! - `collection_constraints.rs` — constraint and field/event definitions.
//! - `system_catalog.rs` — the `SystemCatalog` struct itself.

// ── Records ───────────────────────────────────────────────────────────

pub use super::auth_types::*;
pub use super::checkpoint::CheckpointRecord;
pub use super::checkpoints::CheckpointDoc;
pub use super::collection::{IndexBuildState, StoredCollection, StoredIndex};
pub use super::collection_constraints::{
    BalancedConstraintDef, CheckConstraintDef, EventDefinition, FieldDefinition, LegalHold,
    MaterializedSumDef, PeriodLockDef, StateTransitionDef, TransitionCheckDef, TransitionRule,
};
pub use super::continuous_aggregate::StoredContinuousAggregate;
pub use super::index_record::{IndexKind, StoredIndexRecord};
pub use super::materialized_view::StoredMaterializedView;
pub use super::system_catalog::SystemCatalog;

// ── Table constants ───────────────────────────────────────────────────
//
// Re-exported so existing `super::types::{TABLE_FOO, ...}` imports keep
// working unchanged.

pub(super) use super::tables::{
    ALERT_RULES, API_KEYS, ARRAYS, AUDIT_DML_POSITIONS, AUDIT_LOG, AUTH_USERS, BLACKLIST,
    CHANGE_STREAMS, CHECKPOINTS, CLONE_COPYUPS, CLONE_KV_TOMBSTONES, CLONE_LINEAGE,
    CLONE_TOMBSTONES, COLLECTIONS, COLUMN_STATS, CONSUMER_GROUPS, CONTINUOUS_AGGREGATES,
    CUSTOM_TYPES, DATABASE_GRANTS, DATABASE_HWM, DATABASE_QUOTAS, DATABASES, DATABASES_BY_NAME,
    DEPENDENCIES, FUNCTIONS, INDEX_REGISTRY, L2_CLEANUP_QUEUE, LOCKOUT_STATE, MATERIALIZED_VIEWS,
    METADATA, MIRROR_CUT_STATE, MIRROR_PINS, ORG_MEMBERS, ORGS, OWNERS, PENDING_RECLAIM,
    PERMISSIONS, PROCEDURES, RETENTION_POLICIES, ROLES, SCHEDULES, SCOPE_GRANTS, SCOPE_QUOTAS,
    SCOPES, SEQUENCE_STATE, SEQUENCES, STREAMING_MVS, SURROGATE_PK, SURROGATE_PK_REV,
    SURROGATE_SCOPE_SEQ, SURROGATE_SCOPES, SYNONYM_GROUPS, TENANT_QUOTAS, TENANTS, TOPIC_MESSAGES,
    TOPICS_EP, TRIGGERS, USERS, VECTOR_INDEX_PARAMS, VECTOR_MODEL_METADATA, WAL_TOMBSTONES,
    WASM_MODULES,
};

// ── Helpers ───────────────────────────────────────────────────────────

pub fn catalog_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "catalog".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// Key format: "{object_type}:{database_id}:{tenant_id}:{object_name}"
pub fn owner_key(object_type: &str, database_id: u64, tenant_id: u64, object_name: &str) -> String {
    format!("{object_type}:{database_id}:{tenant_id}:{object_name}")
}
