// SPDX-License-Identifier: BUSL-1.1

//! Collection metadata records persisted in the system catalog.

use nodedb_types::{CloneOrigin, CloneStatus, DatabaseId, Hlc};

use super::collection_constraints::{
    BalancedConstraintDef, CheckConstraintDef, EventDefinition, FieldDefinition, LegalHold,
    MaterializedSumDef, PeriodLockDef, StateTransitionDef, TransitionCheckDef,
};

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
    pub unique: bool,
    /// COLLATE NOCASE / COLLATE CI — values normalized to lowercase before
    /// index put and lookup.
    pub case_insensitive: bool,
    /// Partial index predicate (raw SQL text, parsed at write-time).
    pub predicate: Option<String>,
    /// Build state — see [`IndexBuildState`].
    pub state: IndexBuildState,
    /// Owner — inherited from the owning collection at create time.
    pub owner: String,
}

/// Serializable collection metadata for redb storage.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredCollection {
    pub tenant_id: u64,
    pub name: String,
    pub owner: String,
    pub created_at: u64,
    /// Monotonic descriptor version. Starts at 1 on create, bumped on
    /// every `PutCollection` apply (which doubles as alter). `0` is the
    /// sentinel for "no version assigned", which forces resolvers to
    /// re-fetch rather than trust the row.
    pub descriptor_version: u64,
    /// Monotonic constraint-set version. Bumped by the metadata stamp
    /// ONLY when the DERIVED CRDT constraint set (`collection_constraints`)
    /// actually changes — NOT on every `PutCollection` like
    /// `descriptor_version`. `0` means the collection has no constraints.
    /// `N` is constraint-set revision N. This is
    /// the fence key the CRDT apply path uses so an unrelated ALTER never
    /// transiently rejects in-flight deltas.
    pub constraint_version: u64,
    /// Whether externally synchronized CRDT deltas require an HMAC signature
    /// and monotonic device sequence.
    pub crdt_signing_required: bool,
    /// Hybrid Logical Clock timestamp assigned by the metadata
    /// applier at commit time. Strictly monotonic per descriptor.
    pub modification_hlc: Hlc,
    /// The collection's identity across ALTER, UNDROP and MOVE TENANT: the
    /// `modification_hlc` of the put that created it. A drop and a same-name
    /// create start a new one. Every replicated write carries the incarnation
    /// it was planned against, and a replica applies it only while the
    /// collection still holds it.
    pub incarnation: Hlc,
    /// Optional field type declarations. Empty = schemaless.
    pub fields: Vec<(String, String)>,
    /// Extended field definitions with DEFAULT, VALUE (computed), ASSERT, TYPE.
    pub field_defs: Vec<FieldDefinition>,
    /// Event/trigger definitions (DEFINE EVENT).
    pub event_defs: Vec<EventDefinition>,
    /// Collection type: determines storage engine and query routing.
    pub collection_type: nodedb_types::CollectionType,
    /// Timeseries-specific configuration (JSON-serialized).
    pub timeseries_config: Option<String>,
    pub is_active: bool,
    /// Append-only: UPDATE/DELETE rejected.
    pub append_only: bool,
    /// Hash chain: each INSERT computes SHA-256 chain hash. Requires append_only.
    pub hash_chain: bool,
    /// Balanced constraint: debit/credit sums must match per group_key at commit.
    pub balanced: Option<BalancedConstraintDef>,
    /// Last hash in the chain.
    pub last_chain_hash: Option<String>,
    /// Period lock: binds a period column to a fiscal_periods status table.
    pub period_lock: Option<PeriodLockDef>,
    /// Data retention period. DELETE rejected if row age < period.
    pub retention_period: Option<String>,
    /// Active legal holds. DELETE rejected while any hold is active.
    pub legal_holds: Vec<LegalHold>,
    /// State transition constraints.
    pub state_constraints: Vec<StateTransitionDef>,
    /// Transition check predicates: OLD/NEW expression evaluated on UPDATE.
    pub transition_checks: Vec<TransitionCheckDef>,
    /// Type guard field constraints for schemaless collections.
    pub type_guards: Vec<nodedb_types::TypeGuardFieldDef>,
    /// General CHECK constraints (Control Plane enforcement, can contain subqueries).
    pub check_constraints: Vec<CheckConstraintDef>,
    /// Materialized sum definitions.
    pub materialized_sums: Vec<MaterializedSumDef>,
    /// Enable last-value cache for timeseries.
    pub lvc_enabled: bool,
    /// Bitemporal storage: every write is appended as an immutable version
    /// keyed by its system-time ordinal, enabling `FOR SYSTEM_TIME AS OF` /
    /// `FOR VALID_TIME` queries. Only honored for document engines today;
    /// other engines ignore it.
    pub bitemporal: bool,
    /// Whether this collection uses CRDT (Loro) storage for offline-first
    /// sync. Document-engine-only: `CREATE COLLECTION ... WITH (crdt=true)`
    /// is rejected at DDL time for any other collection type, so a `true`
    /// value here always implies a document collection.
    pub crdt: bool,
    /// Durable CRDT conflict-resolution policy (JSON-serialized
    /// `nodedb_crdt::policy::CollectionPolicy`), set via
    /// `ALTER COLLECTION ... SET ON CONFLICT ... FOR ...`. `None` = no
    /// explicit policy persisted; the per-core `PolicyRegistry` falls back
    /// to `CollectionPolicy::ephemeral()`. Rehydrated into every Data Plane
    /// core's registry via `DocumentOp::Register` on both live DDL apply and
    /// boot rehydration, so the policy survives a restart.
    pub conflict_policy: Option<String>,
    /// Permission tree definition (JSON-serialized).
    pub permission_tree_def: Option<String>,
    /// Secondary indexes declared on this collection.
    ///
    /// Mutated by CREATE/DROP INDEX DDL; the existing `PutCollection`
    /// commit pipeline handles replication + fan-out + descriptor-lease
    /// invalidation.
    pub indexes: Vec<StoredIndex>,
    /// Primary engine hint — which engine is the hot access path.
    pub primary: nodedb_types::PrimaryEngine,
    /// Vector-primary configuration, present only when `primary == Vector`.
    pub vector_primary: Option<nodedb_types::VectorPrimaryConfig>,
    /// How this collection's rows are distributed across vShards.
    pub partition_strategy: nodedb_types::PartitionStrategy,
    /// Best-effort estimate of this collection's on-core data size in
    /// bytes. Summed across every engine's in-memory state for the
    /// `(tenant, collection)` pair on the node that most recently
    /// refreshed it. Populated lazily by the `_system.dropped_collections`
    /// view via a `MetaOp::QueryCollectionSize` dispatch, and surfaces
    /// in the view's `size_bytes_estimate` column so operators can
    /// see how much storage a soft-deleted collection will reclaim
    /// when the GC sweeper hard-deletes it. `0` = never refreshed
    /// yet. Not authoritative across cluster nodes (each node's
    /// local Data Plane is queried) — it's an operator hint, not a
    /// billable source of truth.
    pub size_bytes_estimate: u64,
    /// Database namespace this collection belongs to.
    pub database_id: DatabaseId,

    /// Present when this collection is a copy-on-write clone of a source
    /// collection in another database.  `None` for non-cloned collections.
    ///
    /// The read planner consults this field to decide whether source
    /// delegation is needed; `cloned_from = None` short-circuits the
    /// lookup with zero overhead.
    pub cloned_from: Option<CloneOrigin>,

    /// Materialization state of this clone. A non-clone collection has
    /// `cloned_from = None`, so `clone_status` is never consulted.
    pub clone_status: CloneStatus,

    /// `true` once at least one implicit graph edge (a schemaless document
    /// carrying `_from`/`_to`) — or an explicit `GRAPH INSERT EDGE` — has been
    /// written into this collection. Set idempotently at the edge-creation
    /// chokepoints (see `mark_collection_edge_bearing`).
    ///
    /// This is the routing gate for implicit-edge DELETE cleanup: a
    /// single-collection predicate `DELETE` is otherwise a one-task, one-vshard
    /// `SingleShard` dispatch that bypasses the OLLP/Calvin path entirely, so
    /// the implicit edge-delete derivation never runs. The gate routes such a
    /// `DELETE` through OLLP only when its collection is edge-bearing, leaving
    /// non-edge collections on the fast path.
    pub has_implicit_edges: bool,

    /// Declared `PRIMARY KEY` column name from the `CREATE COLLECTION` /
    /// `CREATE TABLE` column list, when one was present. Can differ from
    /// the built-in `id` field on schemaless document collections (which
    /// otherwise always uses `id` as its document key). `None` means no
    /// PRIMARY KEY was declared and the engine falls back to its default
    /// (`id` for schemaless documents).
    pub declared_primary_key: Option<String>,

    /// Wall-clock nanoseconds when this collection was soft-deleted
    /// (`DROP COLLECTION`), stamped from the same `modification_hlc.wall_ns`
    /// the drop assigns. `0` means the collection was never dropped.
    ///
    /// Readers MUST gate on `is_active` first: this field is never read for
    /// an active row, and `UNDROP COLLECTION` never resets it back to `0`
    /// because a row with `is_active == true` never consults it.
    pub deactivated_at_ns: u64,
}

impl StoredCollection {
    /// Create a minimal collection entry (schemaless document, no fields).
    ///
    /// `descriptor_version` and `modification_hlc` are left at their
    /// defaults (`0` / `Hlc::ZERO`) and assigned by the metadata
    /// applier at commit time. Callers must NOT set them manually;
    /// the cluster-wide applied sequence determines the stamp.
    pub fn new(tenant_id: u64, name: &str, owner: &str) -> Self {
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
            constraint_version: 0,
            crdt_signing_required: false,
            modification_hlc: Hlc::ZERO,
            incarnation: Hlc::ZERO,
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
            bitemporal: false,
            crdt: false,
            conflict_policy: None,
            permission_tree_def: None,
            indexes: Vec::new(),
            size_bytes_estimate: 0,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::default_for_collection_type(
                &nodedb_types::CollectionType::document(),
            ),
            database_id: DatabaseId::DEFAULT,
            cloned_from: None,
            clone_status: CloneStatus::default(),
            has_implicit_edges: false,
            declared_primary_key: None,
            deactivated_at_ns: 0,
        }
    }

    /// A collection a test fixture writes straight to the catalog, stamped as
    /// the proposer stamps a create: a fresh incarnation and descriptor
    /// version 1. Each call names a new incarnation.
    #[cfg(test)]
    pub fn stamped_for_test(tenant_id: u64, name: &str, owner: &str) -> Self {
        static CLOCK: std::sync::OnceLock<nodedb_types::HlcClock> = std::sync::OnceLock::new();
        let hlc = CLOCK.get_or_init(nodedb_types::HlcClock::new).now();
        Self {
            descriptor_version: 1,
            modification_hlc: hlc,
            incarnation: hlc,
            ..Self::new(tenant_id, name, owner)
        }
    }

    /// Parse the timeseries config JSON, if present.
    pub fn get_timeseries_config(&self) -> Option<serde_json::Value> {
        self.timeseries_config
            .as_ref()
            .and_then(|s| sonic_rs::from_str(s).ok())
    }
}
