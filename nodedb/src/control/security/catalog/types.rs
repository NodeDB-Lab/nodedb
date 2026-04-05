//! System catalog type definitions: table constants, collection metadata,
//! constraint types, and field definitions.
//!
//! Auth types (users, roles, permissions) are in `auth_types.rs`.
//! SystemCatalog struct is in `system_catalog.rs`.

use redb::TableDefinition;

// Re-export types from split modules so internal `use super::types::*` still works.
pub use super::auth_types::*;
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
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
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

/// Serializable collection metadata for redb storage.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
pub struct StoredCollection {
    pub tenant_id: u32,
    pub name: String,
    pub owner: String,
    pub created_at: u64,
    /// Optional field type declarations. Empty = schemaless.
    #[serde(default)]
    pub fields: Vec<(String, String)>,
    /// Extended field definitions with DEFAULT, VALUE (computed), ASSERT, TYPE.
    #[serde(default)]
    pub field_defs: Vec<FieldDefinition>,
    /// Event/trigger definitions (DEFINE EVENT).
    #[serde(default)]
    pub event_defs: Vec<EventDefinition>,
    /// Collection type: determines storage engine and query routing.
    #[serde(default)]
    pub collection_type: nodedb_types::CollectionType,
    /// Timeseries-specific configuration (JSON-serialized).
    #[serde(default)]
    pub timeseries_config: Option<String>,
    pub is_active: bool,
    /// Append-only: UPDATE/DELETE rejected.
    #[serde(default)]
    pub append_only: bool,
    /// Hash chain: each INSERT computes SHA-256 chain hash. Requires append_only.
    #[serde(default)]
    pub hash_chain: bool,
    /// Balanced constraint: debit/credit sums must match per group_key at commit.
    #[serde(default)]
    pub balanced: Option<BalancedConstraintDef>,
    /// Last hash in the chain.
    #[serde(default)]
    pub last_chain_hash: Option<String>,
    /// Period lock: binds a period column to a fiscal_periods status table.
    #[serde(default)]
    pub period_lock: Option<PeriodLockDef>,
    /// Data retention period. DELETE rejected if row age < period.
    #[serde(default)]
    pub retention_period: Option<String>,
    /// Active legal holds. DELETE rejected while any hold is active.
    #[serde(default)]
    pub legal_holds: Vec<LegalHold>,
    /// State transition constraints.
    #[serde(default)]
    pub state_constraints: Vec<StateTransitionDef>,
    /// Transition check predicates: OLD/NEW expression evaluated on UPDATE.
    #[serde(default)]
    pub transition_checks: Vec<TransitionCheckDef>,
    /// Materialized sum definitions.
    #[serde(default)]
    pub materialized_sums: Vec<MaterializedSumDef>,
    /// Enable last-value cache for timeseries.
    #[serde(default)]
    pub lvc_enabled: bool,
    /// Permission tree definition (JSON-serialized).
    #[serde(default)]
    pub permission_tree_def: Option<String>,
}

impl StoredCollection {
    /// Create a minimal collection entry (schemaless document, no fields).
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
            materialized_sums: Vec::new(),
            lvc_enabled: false,
            permission_tree_def: None,
        }
    }

    /// Parse the timeseries config JSON, if present.
    pub fn get_timeseries_config(&self) -> Option<serde_json::Value> {
        self.timeseries_config
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
    }
}

// ── Field + event definitions ─────────────────────────────────────────

/// Extended field definition supporting DEFAULT, VALUE, ASSERT, and TYPE constraints.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct FieldDefinition {
    pub name: String,
    /// Type constraint: "int", "float", "string", etc. Empty = any.
    #[serde(default)]
    pub field_type: String,
    /// Default expression (evaluated when field is missing on insert).
    #[serde(default)]
    pub default_expr: String,
    /// Computed value expression (evaluated on every read, not stored).
    #[serde(default)]
    pub value_expr: String,
    /// Assertion expression (must evaluate to true for writes to succeed).
    #[serde(default)]
    pub assert_expr: String,
    /// Whether the field is read-only (cannot be set by user).
    #[serde(default)]
    pub readonly: bool,
    /// Sequence name for auto-generated values on INSERT.
    #[serde(default)]
    pub sequence_name: Option<String>,
    /// If true, this field is a stored generated column (materialized on write).
    /// `value_expr` contains the serialized SqlExpr for write-time evaluation.
    #[serde(default)]
    pub is_generated: bool,
    /// Column names this generated column depends on (for UPDATE recomputation).
    #[serde(default)]
    pub generated_deps: Vec<String>,
}

/// Table event/trigger definition.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EventDefinition {
    pub name: String,
    pub collection: String,
    pub when_condition: String,
    pub then_action: String,
}

/// A materialized view: strict → columnar CDC bridge.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
pub struct StoredMaterializedView {
    pub tenant_id: u32,
    pub name: String,
    pub source: String,
    pub query_sql: String,
    #[serde(default = "default_refresh_mode")]
    pub refresh_mode: String,
    pub owner: String,
    pub created_at: u64,
}

fn default_refresh_mode() -> String {
    "auto".into()
}

// ── Constraint types ──────────────────────────────────────────────────

/// Double-entry balance constraint.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
pub struct BalancedConstraintDef {
    pub group_key_column: String,
    pub debit_value: String,
    pub credit_value: String,
    pub amount_column: String,
    pub entry_type_column: String,
}

/// Period lock: binds a period column to a reference table for status checks.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
pub struct PeriodLockDef {
    pub period_column: String,
    pub ref_table: String,
    pub ref_pk: String,
    pub status_column: String,
    pub allowed_statuses: Vec<String>,
}

/// A legal hold tag preventing deletion.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
pub struct LegalHold {
    pub tag: String,
    pub created_at: u64,
    pub created_by: String,
}

/// State transition constraint: column value can only change along declared paths.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
pub struct StateTransitionDef {
    pub name: String,
    pub column: String,
    pub transitions: Vec<TransitionRule>,
}

/// A single allowed state transition, optionally guarded by a role.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
pub struct TransitionRule {
    pub from: String,
    pub to: String,
    pub required_role: Option<String>,
}

/// Transition check predicate: evaluated on UPDATE with OLD and NEW access.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
pub struct TransitionCheckDef {
    pub name: String,
    pub predicate: crate::bridge::expr_eval::SqlExpr,
}

/// Materialized sum: on INSERT to source, atomically add value_expr to target balance.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
)]
pub struct MaterializedSumDef {
    pub target_collection: String,
    pub target_column: String,
    pub source_collection: String,
    pub join_column: String,
    pub value_expr: crate::bridge::expr_eval::SqlExpr,
}
