//! Document / sparse engine operations dispatched to the Data Plane.

use nodedb_types::columnar::StrictSchema;

/// Right-hand side of an UPDATE ... SET field = <...> assignment.
///
/// The planner turns each assignment into one of these before it crosses
/// the SPSC bridge:
///
/// - `Literal` — pre-encoded msgpack bytes for constant RHS. This is the
///   fast path: the Data Plane can merge these at the binary level for
///   non-strict collections without decoding the current row.
/// - `Expr` — a `SqlExpr` that must be evaluated against the *current*
///   document at apply time. Used for arithmetic (`col + 1`), functions
///   (`LOWER(col)`, `NOW()`), `CASE`, concatenation, and anything else
///   whose result depends on the row being updated.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum UpdateValue {
    Literal(Vec<u8>),
    Expr(crate::bridge::expr_eval::SqlExpr),
}

impl zerompk::ToMessagePack for UpdateValue {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        match self {
            UpdateValue::Literal(bytes) => {
                writer.write_u8(0)?;
                bytes.write(writer)
            }
            UpdateValue::Expr(expr) => {
                writer.write_u8(1)?;
                expr.write(writer)
            }
        }
    }
}

impl<'a> zerompk::FromMessagePack<'a> for UpdateValue {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(2)?;
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(UpdateValue::Literal(Vec::<u8>::read(reader)?)),
            1 => Ok(UpdateValue::Expr(crate::bridge::expr_eval::SqlExpr::read(
                reader,
            )?)),
            _ => Err(zerompk::Error::InvalidMarker(tag)),
        }
    }
}

/// Storage encoding mode for a document collection.
///
/// Determines how documents are serialized before storage in the sparse engine.
/// Propagated from the Control Plane catalog to the Data Plane via
/// `DocumentOp::Register`.
#[derive(Debug, Clone, Default)]
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
#[derive(Debug, Clone, Default)]
pub struct EnforcementOptions {
    /// Reject UPDATE/DELETE operations.
    pub append_only: bool,
    /// Maintain SHA-256 hash chain on INSERT.
    pub hash_chain: bool,
    /// Balanced constraint definition (debit/credit sums must match per group_key).
    pub balanced: Option<BalancedDef>,
    /// Period lock: cross-collection lookup to check if the period is open.
    pub period_lock: Option<PeriodLockConfig>,
    /// Data retention duration. DELETE rejected if row age < this.
    /// Uses calendar-accurate arithmetic (months/years not approximated).
    pub retention: Option<crate::data::executor::enforcement::retention::RetentionDuration>,
    /// Whether any legal hold is active. DELETE unconditionally rejected.
    pub has_legal_hold: bool,
    /// State transition constraints: column value transitions must follow declared paths.
    pub state_constraints: Vec<crate::control::security::catalog::types::StateTransitionDef>,
    /// Transition check predicates: OLD/NEW expressions evaluated on UPDATE.
    pub transition_checks: Vec<crate::control::security::catalog::types::TransitionCheckDef>,
    /// Materialized sum bindings where THIS collection is the source.
    /// On INSERT, each binding triggers an atomic balance update on the target.
    pub materialized_sum_sources: Vec<MaterializedSumBinding>,
    /// Stored generated (computed) columns materialized on write.
    /// On INSERT: evaluate expression, store result alongside other columns.
    /// On UPDATE: re-evaluate if any `depends_on` column changed.
    pub generated_columns: Vec<GeneratedColumnSpec>,
}

/// A stored generated column: expression evaluated at write time.
#[derive(Debug, Clone)]
pub struct GeneratedColumnSpec {
    /// Column name for the generated field.
    pub name: String,
    /// Expression to evaluate against the document.
    pub expr: crate::bridge::expr_eval::SqlExpr,
    /// Column names this expression depends on (for UPDATE recomputation).
    pub depends_on: Vec<String>,
}

/// A materialized sum binding: when a row is INSERTed into this (source)
/// collection, evaluate `value_expr` and atomically add the result to
/// `target_column` on the matching row in `target_collection`.
#[derive(Debug, Clone)]
pub struct MaterializedSumBinding {
    /// Target collection holding the balance column (e.g. `accounts`).
    pub target_collection: String,
    /// Column on target to update (e.g. `balance`).
    pub target_column: String,
    /// Column on source row that joins to target's document ID (e.g. `account_id`).
    pub join_column: String,
    /// Expression evaluated against the source INSERT row to compute the delta.
    pub value_expr: crate::bridge::expr_eval::SqlExpr,
}

/// Period lock configuration propagated to Data Plane.
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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

/// Document engine physical operations (schemaless + strict + DML).
#[derive(Debug, Clone)]
pub enum DocumentOp {
    /// Point lookup by document ID.
    PointGet {
        collection: String,
        document_id: String,
        /// RLS post-fetch filters (serialized `Vec<ScanFilter>`).
        /// If non-empty, the Data Plane evaluates these after fetching
        /// the document. Returns `NOT_FOUND` on denial (no info leak).
        /// Injected by the Control Plane planner from RLS policies.
        #[allow(clippy::doc_markdown)]
        rls_filters: Vec<u8>,
    },

    /// Point write: insert/update a document.
    PointPut {
        collection: String,
        document_id: String,
        value: Vec<u8>,
    },

    /// Point delete: remove a document.
    PointDelete {
        collection: String,
        document_id: String,
    },

    /// Point update: read-modify-write with field-level changes.
    PointUpdate {
        collection: String,
        document_id: String,
        /// Field name → assignment RHS (literal bytes or row-scope expression).
        updates: Vec<(String, UpdateValue)>,
        /// If true, return the post-update document as payload (for RETURNING clause).
        returning: bool,
    },

    /// Full collection scan with filtering, sorting, and pagination.
    Scan {
        collection: String,
        limit: usize,
        offset: usize,
        sort_keys: Vec<(String, bool)>,
        /// Filter predicates serialized as JSON.
        filters: Vec<u8>,
        distinct: bool,
        projection: Vec<String>,
        /// Serialized `Vec<ComputedColumn>`.
        computed_columns: Vec<u8>,
        /// Serialized `Vec<WindowFuncSpec>`.
        window_functions: Vec<u8>,
    },

    /// Batch insert documents in a single redb transaction.
    BatchInsert {
        collection: String,
        /// (document_id, value_bytes) pairs.
        documents: Vec<(String, Vec<u8>)>,
    },

    /// Range scan on a sparse/metadata index.
    RangeScan {
        collection: String,
        field: String,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        limit: usize,
    },

    /// Register collection with secondary index paths and storage mode (DDL).
    Register {
        collection: String,
        index_paths: Vec<String>,
        crdt_enabled: bool,
        /// Storage encoding mode. Determines how documents are serialized.
        storage_mode: StorageMode,
        /// Collection enforcement options propagated from catalog (boxed to reduce enum size).
        enforcement: Box<EnforcementOptions>,
    },

    /// Lookup documents by secondary index value.
    IndexLookup {
        collection: String,
        path: String,
        value: String,
    },

    /// Drop all secondary index entries for a field.
    DropIndex { collection: String, field: String },

    /// Truncate: delete ALL documents in a collection.
    /// If `restart_identity` is true, sequences attached to this collection's
    /// fields are reset to their start value after truncation.
    Truncate {
        collection: String,
        restart_identity: bool,
    },

    /// Estimate count via HLL cardinality stats.
    EstimateCount { collection: String, field: String },

    /// INSERT ... SELECT: copy documents from source to target.
    InsertSelect {
        target_collection: String,
        source_collection: String,
        source_filters: Vec<u8>,
        source_limit: usize,
    },

    /// Upsert: insert or merge. When `on_conflict_updates` is non-empty,
    /// the conflict branch evaluates those assignments against the
    /// *existing* document instead of merging the inserted value —
    /// the `INSERT ... ON CONFLICT DO UPDATE SET ...` path.
    Upsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        on_conflict_updates: Vec<(String, UpdateValue)>,
    },

    /// Bulk update: scan + apply field updates to all matches.
    BulkUpdate {
        collection: String,
        filters: Vec<u8>,
        updates: Vec<(String, UpdateValue)>,
        /// If true, return updated documents as JSON array payload (for RETURNING clause).
        returning: bool,
    },

    /// Bulk delete: scan + delete all matches.
    BulkDelete {
        collection: String,
        filters: Vec<u8>,
    },
}
