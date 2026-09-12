// SPDX-License-Identifier: Apache-2.0

//! Document / sparse engine operations dispatched to the Data Plane.

use nodedb_types::columnar::StrictSchema;

pub use crate::physical_plan::document::UpdateValue;

/// Storage encoding mode for a document collection.
///
/// Determines how documents are serialized before storage in the sparse engine.
/// Propagated from the Control Plane catalog to the Data Plane via
/// `DocumentOp::Register`.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum StorageMode {
    /// Schemaless: documents stored as MessagePack blobs. Self-describing,
    /// supports arbitrary nested fields. Default for collections without a schema.
    #[default]
    Schemaless,
    /// Strict: documents stored as Binary Tuples with O(1) field extraction.
    /// Schema-enforced — all fields must match declared types. 3-4x better
    /// cache density than MessagePack.
    Strict { schema: StrictSchema },
}

/// Collection enforcement options propagated from catalog to Data Plane.
///
/// These flags are cached by the Data Plane in `CollectionConfig` and checked
/// on every write operation (INSERT, UPDATE, DELETE).
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EnforcementOptions {
    /// Reject UPDATE/DELETE operations.
    #[serde(default)]
    pub append_only: bool,
    /// Maintain SHA-256 hash chain on INSERT.
    #[serde(default)]
    pub hash_chain: bool,
    /// Balanced constraint definition (debit/credit sums must match per group_key).
    #[serde(default)]
    pub balanced: Option<BalancedDef>,
    /// Period lock: cross-collection lookup to check if the period is open.
    #[serde(default)]
    pub period_lock: Option<PeriodLockConfig>,
    /// Data retention duration. DELETE rejected if row age < this.
    /// Uses calendar-accurate arithmetic (months/years not approximated).
    #[serde(default)]
    pub retention: Option<crate::physical_plan::document::RetentionDuration>,
    /// Whether any legal hold is active. DELETE unconditionally rejected.
    #[serde(default)]
    pub has_legal_hold: bool,
    /// State transition constraints: column value transitions must follow declared paths.
    #[serde(default)]
    pub state_constraints: Vec<crate::physical_plan::document::StateTransitionDef>,
    /// Transition check predicates: OLD/NEW expressions evaluated on UPDATE.
    #[serde(default)]
    pub transition_checks: Vec<crate::physical_plan::document::TransitionCheckDef>,
    /// Materialized sum bindings where THIS collection is the source.
    /// On INSERT, each binding triggers an atomic balance update on the target.
    #[serde(default)]
    pub materialized_sum_sources: Vec<MaterializedSumBinding>,
    /// Stored generated (computed) columns materialized on write.
    /// On INSERT: evaluate expression, store result alongside other columns.
    /// On UPDATE: re-evaluate if any `depends_on` column changed.
    #[serde(default)]
    pub generated_columns: Vec<GeneratedColumnSpec>,
}

impl EnforcementOptions {
    /// Whether this collection has any stateless PUT-path enforcement
    /// configured (append-only, period lock, state transitions, or
    /// transition-check predicates). Used to decide whether the caller
    /// needs a pre-write read of the prior document to feed those checks.
    pub fn has_put_checks(&self) -> bool {
        self.append_only
            || self.period_lock.is_some()
            || !self.state_constraints.is_empty()
            || !self.transition_checks.is_empty()
    }

    /// Whether this collection declares enforcement that folds a write's PRE-
    /// and POST-images: materialized-sum bindings, or a BALANCED constraint.
    ///
    /// Distinct from [`Self::has_put_checks`], which asks whether the stateless
    /// admission checks need the prior row. This asks whether the prior row must
    /// be DECODED into a document, which is a strictly stronger requirement: a
    /// stored body is only guaranteed to be a readable document for a collection
    /// that declares what its columns mean. A caller that decodes the pre-image
    /// unconditionally fails every write to a collection carrying an opaque
    /// body, including collections that declare no constraints at all.
    pub fn has_image_enforcement(&self) -> bool {
        !self.materialized_sum_sources.is_empty() || self.balanced.is_some()
    }
}

/// A stored generated column: expression evaluated at write time.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct GeneratedColumnSpec {
    /// Column name for the generated field.
    pub name: String,
    /// Expression to evaluate against the document.
    pub expr: nodedb_query::expr::SqlExpr,
    /// Column names this expression depends on (for UPDATE recomputation).
    pub depends_on: Vec<String>,
}

/// A materialized sum binding: when a row is INSERTed into this (source)
/// collection, evaluate `value_expr` and atomically add the result to
/// `target_column` on the matching row in `target_collection`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct MaterializedSumBinding {
    /// Target collection holding the balance column (e.g. `accounts`).
    pub target_collection: String,
    /// Column on target to update (e.g. `balance`).
    pub target_column: String,
    /// Column on source row that joins to target's document ID (e.g. `account_id`).
    pub join_column: String,
    /// Expression evaluated against the source INSERT row to compute the delta.
    pub value_expr: nodedb_query::expr::SqlExpr,
    /// The TARGET collection's declared `PRIMARY KEY` column, when it has
    /// one. Resolved from the catalog at plan time. Names the target row in
    /// the event and redo entry its balance write produces.
    #[serde(default)]
    pub declared_primary_key: Option<String>,
}

/// Period lock configuration propagated to Data Plane.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PeriodLockConfig {
    /// Column in this collection identifying the period (e.g. `fiscal_period`).
    pub period_column: String,
    /// Reference collection holding period statuses (e.g. `fiscal_periods`).
    pub ref_table: String,
    /// Primary key column in the ref table (e.g. `period_key`).
    pub ref_pk: String,
    /// Status column in the ref table (e.g. `status`).
    pub status_column: String,
    /// Status values that allow writes (e.g. `["OPEN", "ADJUSTING"]`).
    pub allowed_statuses: Vec<String>,
}

/// Bridge-level balanced constraint definition (mirrors catalog BalancedConstraintDef).
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct BalancedDef {
    /// Column used to group entries (e.g. `journal_id`).
    pub group_key_column: String,
    /// Column that distinguishes debits from credits (e.g. `entry_type`).
    pub entry_type_column: String,
    /// Value in `entry_type_column` that marks a debit (e.g. `"DEBIT"`).
    pub debit_value: String,
    /// Value in `entry_type_column` that marks a credit (e.g. `"CREDIT"`).
    pub credit_value: String,
    /// Column containing the monetary amount (e.g. `amount`).
    pub amount_column: String,
}

/// A single item in a RETURNING projection list.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReturningItem {
    /// The bare column name (e.g. `"id"` or `"stock"`).
    pub name: String,
    /// Optional alias (`RETURNING stock AS s` → `alias = Some("s")`).
    pub alias: Option<String>,
}

/// The set of columns projected by a RETURNING clause.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ReturningColumns {
    /// `RETURNING *` — all fields in the collection.
    Star,
    /// `RETURNING col1, col2 [AS alias], ...`
    Named(Vec<ReturningItem>),
}

/// Parsed representation of a RETURNING clause carried through the bridge.
///
/// Produced by the Control Plane's `strip_returning()` and injected into
/// `PointUpdate`, `BulkUpdate`, `PointDelete`, and `BulkDelete` variants
/// before crossing the SPSC bridge.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReturningSpec {
    pub columns: ReturningColumns,
}

impl ReturningSpec {
    /// Return the output column names (alias when present, else name).
    pub fn output_names(&self) -> Option<Vec<String>> {
        match &self.columns {
            ReturningColumns::Star => None,
            ReturningColumns::Named(items) => Some(
                items
                    .iter()
                    .map(|item| item.alias.clone().unwrap_or_else(|| item.name.clone()))
                    .collect(),
            ),
        }
    }

    /// Return the source column names (ignoring aliases) for field extraction.
    pub fn source_names(&self) -> Option<Vec<String>> {
        match &self.columns {
            ReturningColumns::Star => None,
            ReturningColumns::Named(items) => {
                Some(items.iter().map(|item| item.name.clone()).collect())
            }
        }
    }
}

/// Build state for a secondary index propagated from catalog to the Data
/// Plane. Mirrors the catalog `IndexBuildState` but lives in the bridge so
/// the Data Plane doesn't depend on catalog types.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RegisteredIndexState {
    Building,
    Ready,
}

/// One secondary index spec propagated from the catalog into the Data
/// Plane via [`DocumentOp::Register`]. The write path consults these to:
///
/// - extract values on put (path / is_array)
/// - apply UNIQUE pre-commit checks (unique)
/// - normalize case (case_insensitive)
/// - skip `Building` indexes from planner lookups while still dual-writing
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RegisteredIndex {
    pub name: String,
    pub path: String,
    pub unique: bool,
    pub case_insensitive: bool,
    pub state: RegisteredIndexState,
    /// Partial-index predicate as raw SQL text (`WHERE <expr>` body,
    /// without the `WHERE` keyword). `None` for full indexes.
    /// The write path parses and evaluates this per document; rows
    /// where the predicate is false are not inserted into the index
    /// and do not participate in UNIQUE enforcement.
    #[serde(default)]
    pub predicate: Option<String>,
}
