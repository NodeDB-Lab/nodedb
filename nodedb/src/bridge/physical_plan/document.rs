//! Document / sparse engine operations dispatched to the Data Plane.

use nodedb_types::columnar::StrictSchema;

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

/// Accounting enforcement options propagated from catalog to Data Plane.
///
/// These flags are cached by the Data Plane in `CollectionConfig` and checked
/// on every write operation (INSERT, UPDATE, DELETE).
#[derive(Debug, Clone, Default)]
pub struct AccountingOptions {
    /// Reject UPDATE/DELETE operations.
    pub append_only: bool,
    /// Maintain SHA-256 hash chain on INSERT.
    pub hash_chain: bool,
    /// Balanced constraint definition (debit/credit sums must match per group_key).
    pub balanced: Option<BalancedDef>,
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
        /// Field name → new JSON value.
        updates: Vec<(String, Vec<u8>)>,
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
        /// Accounting enforcement options propagated from catalog.
        accounting: AccountingOptions,
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
    Truncate { collection: String },

    /// Estimate count via HLL cardinality stats.
    EstimateCount { collection: String, field: String },

    /// INSERT ... SELECT: copy documents from source to target.
    InsertSelect {
        target_collection: String,
        source_collection: String,
        source_filters: Vec<u8>,
        source_limit: usize,
    },

    /// Upsert: insert or merge.
    Upsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
    },

    /// Bulk update: scan + apply field updates to all matches.
    BulkUpdate {
        collection: String,
        filters: Vec<u8>,
        updates: Vec<(String, Vec<u8>)>,
    },

    /// Bulk delete: scan + delete all matches.
    BulkDelete {
        collection: String,
        filters: Vec<u8>,
    },
}
