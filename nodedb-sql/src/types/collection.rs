// SPDX-License-Identifier: Apache-2.0

//! Collection and column metadata types for query planning.

use super::query::EngineType;
use crate::types_expr::SqlDataType;

/// Metadata about a collection for query planning.
#[derive(Debug, Clone)]
pub struct CollectionInfo {
    pub name: String,
    pub engine: EngineType,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Option<String>,
    pub has_auto_tier: bool,
    /// Secondary indexes available for planner rewrites. Populated by the
    /// catalog adapter from `StoredCollection.indexes`. `Building` entries
    /// are included so the planner can see them but MUST be skipped when
    /// choosing an index lookup — only `Ready` indexes back query rewrites.
    pub indexes: Vec<IndexSpec>,
    /// When `true`, this collection stores every write as an immutable
    /// version keyed by its system time. Enables `FOR SYSTEM_TIME AS OF`
    /// and `FOR VALID_TIME` queries. Only meaningful for document engines
    /// today; other engines ignore this flag.
    pub bitemporal: bool,
    /// Primary engine hint from the catalog.
    pub primary: nodedb_types::PrimaryEngine,
    /// Vector-primary configuration. `Some` only when
    /// `primary == PrimaryEngine::Vector`.
    pub vector_primary: Option<nodedb_types::VectorPrimaryConfig>,
    /// How this collection's rows are distributed across vShards.
    ///
    /// Authoritative per-collection partition metadata. Future routing layers
    /// read this instead of inferring distribution from engine type.
    pub partition_strategy: nodedb_types::PartitionStrategy,
    /// Whether a name outside `columns` resolves in this relation.
    ///
    /// A stored collection derives it from its engine. A synthesized relation
    /// sets it independently: a derived, LATERAL, or CTE alias is closed when
    /// its projection list is inferable, open only when it is `*` over an open
    /// source. `pg_catalog` relations are open because NodeDB models a subset
    /// of the columns clients ask for.
    pub open_schema: bool,
}

impl CollectionInfo {
    /// The openness a stored collection on `engine` carries.
    pub fn open_schema_for(engine: EngineType) -> bool {
        crate::engine_rules::resolve_engine_rules(engine).accepts_undeclared_columns()
    }
}

/// Secondary index metadata surfaced to the SQL planner.
#[derive(Debug, Clone)]
pub struct IndexSpec {
    pub name: String,
    /// Canonical field path (`$.email`, `$.user.name`, or plain column name
    /// for strict documents — the catalog layer stores them uniformly).
    pub field: String,
    pub unique: bool,
    pub case_insensitive: bool,
    /// Build state. Only `Ready` indexes drive query rewrites.
    pub state: IndexState,
    /// Partial-index predicate as raw SQL text (`WHERE <expr>` body
    /// without the keyword), or `None` for full indexes. The planner
    /// uses this to reject rewrites whose WHERE clause doesn't entail
    /// the predicate — matching against such a partial index would
    /// omit rows the index didn't cover.
    pub predicate: Option<String>,
}

/// Planner-facing index state. Mirrors the catalog variant but lives here
/// so the SQL crate doesn't depend on `nodedb` internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    Building,
    Ready,
}

/// Metadata about a single column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
    pub is_primary_key: bool,
    /// Default value expression (e.g. "UUID_V7", "ULID", "NANOID(10)", "0", "'active'").
    pub default: Option<String>,
    /// Raw type string as stored in the catalog (e.g. `"JSON"`, `"TEXT"`, `"FLOAT64"`).
    /// `None` for columns synthesized by the planner (e.g. auto-injected `id`).
    /// Columnar INSERT converters use this to reconstruct the exact `ColumnType`
    /// so JSON / Geometry / UUID columns are not incorrectly inferred as String.
    /// Write coercion reads a `VECTOR(dim)` declaration from it on a column
    /// whose `data_type` advertises text.
    pub raw_type: Option<String>,
    /// Declared width of an integer column, resolved once from the catalog at
    /// adapter-construction time. `None` for non-integer columns and for
    /// integer columns whose declared type carried no width.
    ///
    /// This is deliberately a resolved [`IntWidth`] rather than another raw
    /// string: it is read on both the write path (range validation) and the
    /// read path (`RowDescription` OID and binary payload width), and those
    /// two must agree exactly. Resolving once at the catalog boundary is what
    /// makes disagreement unrepresentable.
    pub int_width: Option<nodedb_types::columnar::IntWidth>,
    /// Declared width of a floating-point column, resolved once from the
    /// catalog at adapter-construction time. `None` for non-float columns and
    /// for float columns whose declared type the catalog never recorded.
    ///
    /// The float analogue of [`ColumnInfo::int_width`], and resolved from the
    /// same source. Read on the read path to select the `RowDescription` OID
    /// (700 vs 701) and the binary payload width (4 vs 8 bytes), and read on
    /// the write path by `check_declared_float_ranges` — but only for the
    /// narrower failure mode a float has: narrowing an `f64` to `f32` rounds
    /// rather than wraps (PostgreSQL accepts-and-rounds a `double` literal
    /// into a `real` column, and so does nodedb), so only a finite value
    /// that would overflow to infinity is rejected, not the full out-of-range
    /// check `int_width` gets.
    pub float_width: Option<nodedb_types::columnar::FloatWidth>,
}
