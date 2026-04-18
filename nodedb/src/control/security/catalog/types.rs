//! System catalog type definitions: table constants, collection metadata,
//! checkpoint records, and helpers.
//!
//! - Auth types (users, roles, permissions) live in `auth_types.rs`.
//! - `SystemCatalog` struct lives in `system_catalog.rs`.
//! - Constraint definitions (balanced, CHECK, state transitions, etc.)
//!   and field/event definitions live in `collection_constraints.rs`.

use nodedb_types::Hlc;
use redb::TableDefinition;
use sonic_rs;

// Re-export types from split modules so internal `use super::types::*` still works.
pub use super::auth_types::*;
pub use super::collection_constraints::{
    BalancedConstraintDef, CheckConstraintDef, EventDefinition, FieldDefinition, LegalHold,
    MaterializedSumDef, PeriodLockDef, StateTransitionDef, TransitionCheckDef, TransitionRule,
};
pub use super::system_catalog::SystemCatalog;

// ── Table definitions ─────────────────────────────────────────────────

/// Table: username (string) -> MessagePack-serialized user record.
pub(super) const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.users");

/// Table: key_id (string) -> MessagePack-serialized API key record.
pub(super) const API_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.api_keys");

/// Table: tenant_id (string) -> MessagePack-serialized tenant record.
pub(super) const TENANTS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.tenants");

/// Table: seq (u64 as big-endian bytes) -> MessagePack-serialized audit entry.
pub(super) const AUDIT_LOG: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("_system.audit_log");

/// Table: role_name -> MessagePack-serialized custom role record.
pub(super) const ROLES: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.roles");

/// Table: "target:role_or_user" -> MessagePack-serialized permission grant.
pub(super) const PERMISSIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.permissions");

/// Table: "{object_type}:{tenant_id}:{object_name}" -> owner username.
pub(super) const OWNERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.owners");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized collection metadata.
pub(super) const COLLECTIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.collections");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized materialized view metadata.
pub(super) const MATERIALIZED_VIEWS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.materialized_views");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized user function definition.
pub(super) const FUNCTIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.functions");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized trigger definition.
pub(super) const TRIGGERS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.triggers");

/// Table: "{tenant_id}:{stream_name}" -> MessagePack-serialized ChangeStreamDef.
pub(super) const CHANGE_STREAMS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.change_streams");

/// Table: "{tenant_id}:{stream_name}:{group_name}" -> MessagePack-serialized ConsumerGroupDef.
pub(super) const CONSUMER_GROUPS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.consumer_groups");

/// Table: "{tenant_id}:{schedule_name}" -> MessagePack-serialized ScheduleDef.
pub(super) const SCHEDULES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.schedules");

/// Table: "{tenant_id}:{policy_name}" -> MessagePack-serialized RetentionPolicyDef.
pub(super) const RETENTION_POLICIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.retention_policies");

/// Table: "{tenant_id}:{alert_name}" -> MessagePack-serialized AlertDef.
pub(super) const ALERT_RULES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.alert_rules");

/// Table: "{tenant_id}:{topic_name}" -> MessagePack-serialized TopicDef.
pub(super) const TOPICS_EP: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.topics_ep");

/// Table: "{tenant_id}:{mv_name}" -> MessagePack-serialized StreamingMvDef.
pub(super) const STREAMING_MVS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.streaming_mvs");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized stored procedure definition.
pub(super) const PROCEDURES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.procedures");

/// Table: "{source_type}:{tenant_id}:{source_name}" -> MessagePack-serialized dependency list.
pub(super) const DEPENDENCIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.dependencies");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized sequence definition.
pub(super) const SEQUENCES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.sequences");

/// Table: "{tenant_id}:{name}" -> MessagePack-serialized sequence runtime state.
pub(super) const SEQUENCE_STATE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.sequence_state");

/// Table: "{tenant_id}:{collection}:{column}" -> MessagePack-serialized column statistics.
pub(super) const COLUMN_STATS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.column_stats");

/// Table: metadata key -> value bytes (counters, config).
pub(super) const METADATA: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.metadata");

/// Table: "wasm_module:{sha256_hex}" -> raw WASM binary bytes.
pub(super) const WASM_MODULES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.wasm_modules");

/// Table: blacklist key (user_id or IP) -> MessagePack-serialized blacklist entry.
pub(super) const BLACKLIST: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.blacklist");

/// Table: auth_user_id -> MessagePack-serialized auth user record (JIT-provisioned).
pub(super) const AUTH_USERS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.auth_users");

/// Table: org_id -> MessagePack-serialized org record.
pub(super) const ORGS: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.orgs");

/// Table: "{org_id}:{user_id}" -> MessagePack-serialized org membership.
pub(super) const ORG_MEMBERS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.org_members");

/// Table: scope_name -> MessagePack-serialized scope definition.
pub(super) const SCOPES: TableDefinition<&str, &[u8]> = TableDefinition::new("_system.scopes");

/// Table: "{scope_name}:{grantee_type}:{grantee_id}" -> MessagePack-serialized scope grant.
pub(super) const SCOPE_GRANTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.scope_grants");

/// Table: "{tenant_id}:{collection}:{column}" -> MessagePack-serialized VectorModelEntry.
pub(super) const VECTOR_MODEL_METADATA: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.vector_model_metadata");

/// Table: "{tenant_id}:{collection}:{doc_id}:{checkpoint_name}" -> MessagePack CheckpointRecord.
pub(super) const CHECKPOINTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.checkpoints");

// ── Helpers ───────────────────────────────────────────────────────────

pub fn catalog_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "catalog".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// Key format: "{object_type}:{tenant_id}:{object_name}"
pub fn owner_key(object_type: &str, tenant_id: u32, object_name: &str) -> String {
    format!("{object_type}:{tenant_id}:{object_name}")
}

// ── Checkpoint ────────────────────────────────────────────────────────

/// A named checkpoint: captures a version vector at a point in time.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
pub struct CheckpointRecord {
    pub tenant_id: u32,
    pub collection: String,
    pub doc_id: String,
    pub checkpoint_name: String,
    pub version_vector_json: String,
    pub created_by: String,
    pub created_at: u64,
}

impl CheckpointRecord {
    pub fn catalog_key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.tenant_id, self.collection, self.doc_id, self.checkpoint_name
        )
    }

    pub fn doc_prefix(tenant_id: u32, collection: &str, doc_id: &str) -> String {
        format!("{tenant_id}:{collection}:{doc_id}:")
    }
}

// ── Collection metadata ───────────────────────────────────────────────

/// Build state of a secondary index.
///
/// A freshly created index is `Building` until the applier-driven backfill
/// reports every vShard caught-up; a second `PutCollection` then flips it
/// to `Ready`. The planner only rewrites queries to `IndexLookup` for
/// indexes in the `Ready` state — `Building` indexes are invisible to reads
/// but receive dual-writes on new inserts so they converge.
#[derive(
    zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, Copy, PartialEq, Eq, Default,
)]
pub enum IndexBuildState {
    Building,
    #[default]
    Ready,
}

/// A secondary index declared on a document collection.
///
/// Stored inline on [`StoredCollection::indexes`]. CREATE/DROP INDEX DDL
/// mutates the vector and issues a `PutCollection`, so replication, restart
/// recovery, descriptor-lease invalidation, and DROP cascade all ride the
/// existing collection-commit pipeline.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredIndex {
    /// Index identifier, unique per tenant.
    pub name: String,
    /// Field path being indexed. Schemaless paths start with `$.`, strict
    /// column indexes are plain column names — the DDL layer normalizes.
    pub field: String,
    /// UNIQUE enforced at write-path pre-commit.
    #[msgpack(default)]
    pub unique: bool,
    /// COLLATE NOCASE / COLLATE CI — values normalized to lowercase before
    /// index put and lookup.
    #[msgpack(default)]
    pub case_insensitive: bool,
    /// Partial index predicate (raw SQL text, parsed at write-time).
    #[msgpack(default)]
    pub predicate: Option<String>,
    /// Build state — see [`IndexBuildState`].
    #[msgpack(default)]
    pub state: IndexBuildState,
    /// Owner — inherited from the owning collection at create time.
    #[msgpack(default)]
    pub owner: String,
}

/// Serializable collection metadata for redb storage.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredCollection {
    pub tenant_id: u32,
    pub name: String,
    pub owner: String,
    pub created_at: u64,
    /// Monotonic descriptor version. Starts at 1 on create, bumped on
    /// every `PutCollection` apply (which doubles as alter). A value
    /// of `0` is the sentinel for "legacy entry written before
    /// `DISTRIBUTED_CATALOG_VERSION >= 3`, version unknown" and
    /// forces resolvers to re-fetch.
    #[msgpack(default)]
    pub descriptor_version: u64,
    /// Hybrid Logical Clock timestamp assigned by the metadata
    /// applier at commit time. Strictly monotonic per descriptor.
    #[msgpack(default)]
    pub modification_hlc: Hlc,
    /// Optional field type declarations. Empty = schemaless.
    #[msgpack(default)]
    pub fields: Vec<(String, String)>,
    /// Extended field definitions with DEFAULT, VALUE (computed), ASSERT, TYPE.
    #[msgpack(default)]
    pub field_defs: Vec<FieldDefinition>,
    /// Event/trigger definitions (DEFINE EVENT).
    #[msgpack(default)]
    pub event_defs: Vec<EventDefinition>,
    /// Collection type: determines storage engine and query routing.
    #[msgpack(default)]
    pub collection_type: nodedb_types::CollectionType,
    /// Timeseries-specific configuration (JSON-serialized).
    #[msgpack(default)]
    pub timeseries_config: Option<String>,
    pub is_active: bool,
    /// Append-only: UPDATE/DELETE rejected.
    #[msgpack(default)]
    pub append_only: bool,
    /// Hash chain: each INSERT computes SHA-256 chain hash. Requires append_only.
    #[msgpack(default)]
    pub hash_chain: bool,
    /// Balanced constraint: debit/credit sums must match per group_key at commit.
    #[msgpack(default)]
    pub balanced: Option<BalancedConstraintDef>,
    /// Last hash in the chain.
    #[msgpack(default)]
    pub last_chain_hash: Option<String>,
    /// Period lock: binds a period column to a fiscal_periods status table.
    #[msgpack(default)]
    pub period_lock: Option<PeriodLockDef>,
    /// Data retention period. DELETE rejected if row age < period.
    #[msgpack(default)]
    pub retention_period: Option<String>,
    /// Active legal holds. DELETE rejected while any hold is active.
    #[msgpack(default)]
    pub legal_holds: Vec<LegalHold>,
    /// State transition constraints.
    #[msgpack(default)]
    pub state_constraints: Vec<StateTransitionDef>,
    /// Transition check predicates: OLD/NEW expression evaluated on UPDATE.
    #[msgpack(default)]
    pub transition_checks: Vec<TransitionCheckDef>,
    /// Type guard field constraints for schemaless collections.
    #[msgpack(default)]
    pub type_guards: Vec<nodedb_types::TypeGuardFieldDef>,
    /// General CHECK constraints (Control Plane enforcement, may contain subqueries).
    #[msgpack(default)]
    pub check_constraints: Vec<CheckConstraintDef>,
    /// Materialized sum definitions.
    #[msgpack(default)]
    pub materialized_sums: Vec<MaterializedSumDef>,
    /// Enable last-value cache for timeseries.
    #[msgpack(default)]
    pub lvc_enabled: bool,
    /// Permission tree definition (JSON-serialized).
    #[msgpack(default)]
    pub permission_tree_def: Option<String>,
    /// Secondary indexes declared on this collection.
    ///
    /// Mutated by CREATE/DROP INDEX DDL; the existing `PutCollection`
    /// commit pipeline handles replication + fan-out + descriptor-lease
    /// invalidation.
    #[msgpack(default)]
    pub indexes: Vec<StoredIndex>,
}

impl StoredCollection {
    /// Create a minimal collection entry (schemaless document, no fields).
    ///
    /// `descriptor_version` and `modification_hlc` are left at their
    /// defaults (`0` / `Hlc::ZERO`) and assigned by the metadata
    /// applier at commit time. Callers must NOT set them manually;
    /// the cluster-wide applied sequence determines the stamp.
    pub fn new(tenant_id: u32, name: &str, owner: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            tenant_id,
            name: name.to_string(),
            owner: owner.to_string(),
            created_at: now,
            descriptor_version: 0,
            modification_hlc: Hlc::ZERO,
            fields: Vec::new(),
            field_defs: Vec::new(),
            event_defs: Vec::new(),
            collection_type: nodedb_types::CollectionType::document(),
            timeseries_config: None,
            is_active: true,
            append_only: false,
            hash_chain: false,
            balanced: None,
            last_chain_hash: None,
            period_lock: None,
            retention_period: None,
            legal_holds: Vec::new(),
            state_constraints: Vec::new(),
            transition_checks: Vec::new(),
            type_guards: Vec::new(),
            check_constraints: Vec::new(),
            materialized_sums: Vec::new(),
            lvc_enabled: false,
            permission_tree_def: None,
            indexes: Vec::new(),
        }
    }

    /// Parse the timeseries config JSON, if present.
    pub fn get_timeseries_config(&self) -> Option<serde_json::Value> {
        self.timeseries_config
            .as_ref()
            .and_then(|s| sonic_rs::from_str(s).ok())
    }
}

// ── Materialized view metadata ────────────────────────────────────────

/// A materialized view: strict → columnar CDC bridge.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredMaterializedView {
    pub tenant_id: u32,
    pub name: String,
    pub source: String,
    pub query_sql: String,
    #[msgpack(default = "default_refresh_mode")]
    pub refresh_mode: String,
    pub owner: String,
    pub created_at: u64,
    /// Monotonic descriptor version. See [`StoredCollection::descriptor_version`].
    #[msgpack(default)]
    pub descriptor_version: u64,
    /// HLC assigned by the metadata applier. See [`StoredCollection::modification_hlc`].
    #[msgpack(default)]
    pub modification_hlc: Hlc,
}

fn default_refresh_mode() -> String {
    "auto".into()
}
