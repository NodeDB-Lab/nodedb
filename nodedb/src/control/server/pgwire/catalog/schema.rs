// SPDX-License-Identifier: BUSL-1.1

//! Identity-independent schema for every catalog relation, plus the canonical
//! list of relation names the catalog data source serves.
//!
//! The schema here is the single source of truth consumed by the planner (to
//! resolve a catalog relation to a `CollectionInfo`) and by the row producers
//! (each row is a msgpack map whose keys are exactly these column names, in
//! this order). Column order and names match what generic Postgres clients
//! expect from the real `pg_catalog`.

use nodedb_sql::types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType};

/// Logical column type for a catalog relation. Mirrors the closed set the old
/// virtual-table layer supported; translated into [`SqlDataType`] for the
/// planner and used to choose the msgpack value kind for each cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogColType {
    Bool,
    /// 4-byte integer in PostgreSQL terms; carried as `Int64` in the planner.
    Int4,
    /// 8-byte integer.
    Int8,
    Text,
}

impl CatalogColType {
    fn to_sql(self) -> SqlDataType {
        match self {
            CatalogColType::Bool => SqlDataType::Bool,
            CatalogColType::Int4 | CatalogColType::Int8 => SqlDataType::Int64,
            CatalogColType::Text => SqlDataType::String,
        }
    }
}

/// One catalog column: static name + logical type.
pub struct CatalogColumn {
    pub name: &'static str,
    pub ty: CatalogColType,
}

const fn col(name: &'static str, ty: CatalogColType) -> CatalogColumn {
    CatalogColumn { name, ty }
}

/// Canonical names of every relation served by the catalog data source.
pub const KNOWN_TABLES: &[&str] = &[
    "pg_database",
    "pg_namespace",
    "pg_type",
    "pg_class",
    "pg_attribute",
    "pg_attrdef",
    "pg_collation",
    "pg_index",
    "pg_range",
    "pg_authid",
    "_system.audit_log",
    "_system.dropped_collections",
    "_system.l2_cleanup_queue",
];

/// Map a relation name to its canonical static name (case-insensitive).
pub fn known_table(name: &str) -> Option<&'static str> {
    KNOWN_TABLES
        .iter()
        .copied()
        .find(|t| t.eq_ignore_ascii_case(name))
}

/// Static column spec for a known catalog relation, in output order. Returns
/// `None` for any name that is not a catalog relation.
pub fn catalog_columns(table: &str) -> Option<Vec<CatalogColumn>> {
    use CatalogColType::{Bool, Int4, Int8, Text};
    Some(match table {
        "pg_database" => vec![
            col("oid", Int8),
            col("datname", Text),
            col("datdba", Text),
            col("encoding", Text),
        ],
        "pg_namespace" => vec![
            col("oid", Int8),
            col("nspname", Text),
            col("nspowner", Int8),
        ],
        "pg_type" => vec![
            col("oid", Int8),
            col("typname", Text),
            col("typnamespace", Int8),
            col("typlen", Int4),
            col("typbyval", Bool),
            col("typtype", Text),
            col("typcategory", Text),
            col("typispreferred", Bool),
            col("typisdefined", Bool),
            col("typdelim", Text),
            col("typrelid", Int8),
            col("typelem", Int8),
            col("typarray", Int8),
            col("typnotnull", Bool),
            col("typinput", Text),
            col("typbasetype", Int8),
            col("typcollation", Int8),
        ],
        "pg_class" => vec![
            col("oid", Int8),
            col("relname", Text),
            col("relnamespace", Int8),
            col("reltype", Int8),
            col("relam", Int8),
            col("relfilenode", Int8),
            col("relpages", Int4),
            col("relkind", Text),
            col("relnatts", Int4),
            col("relchecks", Int4),
            col("relhasindex", Bool),
            col("relisshared", Bool),
            col("relpersistence", Text),
            col("relhasrules", Bool),
            col("relhastriggers", Bool),
            col("relhassubclass", Bool),
            col("relrowsecurity", Bool),
            col("relispartition", Bool),
            col("relreplident", Text),
            col("relowner", Int8),
        ],
        "pg_attribute" => vec![
            col("attrelid", Int8),
            col("attname", Text),
            col("atttypid", Int8),
            col("attstattarget", Int4),
            col("attlen", Int4),
            col("attnum", Int4),
            col("attndims", Int4),
            col("attcacheoff", Int4),
            col("atttypmod", Int4),
            col("attbyval", Bool),
            col("attstorage", Text),
            col("attalign", Text),
            col("attnotnull", Bool),
            col("atthasdef", Bool),
            col("attidentity", Text),
            col("attgenerated", Text),
            col("attisdropped", Bool),
            col("attislocal", Bool),
            col("attinhcount", Int4),
            col("attcollation", Int8),
        ],
        "pg_attrdef" => vec![
            col("oid", Int8),
            col("adrelid", Int8),
            col("adnum", Int4),
            col("adbin", Text),
        ],
        "pg_collation" => vec![col("oid", Int8), col("collname", Text)],
        "pg_index" => vec![
            col("indexrelid", Int8),
            col("indrelid", Int8),
            col("indisunique", Bool),
            col("indisprimary", Bool),
        ],
        "pg_range" => vec![col("rngtypid", Int8), col("rngsubtype", Int8)],
        "pg_authid" => vec![
            col("oid", Int8),
            col("rolname", Text),
            col("rolsuper", Bool),
            col("rolcanlogin", Bool),
        ],
        "_system.audit_log" => vec![
            col("seq", Int8),
            col("timestamp_us", Int8),
            col("event", Text),
            col("tenant_id", Int8),
            col("source", Text),
            col("detail", Text),
            col("prev_hash", Text),
        ],
        "_system.dropped_collections" => vec![
            col("tenant_id", Int8),
            col("name", Text),
            col("owner", Text),
            col("engine_type", Text),
            col("deactivated_at_ns", Int8),
            col("retention_expires_at_ns", Int8),
            col("size_bytes_estimate", Int8),
            col("partition_strategy", Text),
        ],
        "_system.l2_cleanup_queue" => vec![
            col("database_id", Int8),
            col("tenant_id", Int8),
            col("name", Text),
            col("purge_lsn", Int8),
            col("enqueued_at_ns", Int8),
            col("bytes_pending", Int8),
            col("last_error", Text),
            col("attempts", Int4),
        ],
        _ => return None,
    })
}

/// Identity-independent `CollectionInfo` for a known catalog relation, so the
/// SQL planner can resolve `pg_class`, `pg_type`, `_system.audit_log`, etc. as
/// ordinary read-only relations. Returns `None` for any non-catalog name.
///
/// Catalog relations are synthetic, read-only, and have no primary key,
/// indexes, or temporal columns; their rows are produced per-request by
/// [`super::catalog_rows`] and consumed as a data source.
pub fn catalog_collection_info(name: &str) -> Option<CollectionInfo> {
    let table = known_table(name)?;
    let columns = catalog_columns(table)?
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.to_string(),
            data_type: c.ty.to_sql(),
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
        })
        .collect();
    Some(CollectionInfo {
        name: table.to_string(),
        engine: EngineType::DocumentSchemaless,
        columns,
        primary_key: None,
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::default(),
        vector_primary: None,
        // Catalog relations are synthetic, read-only, and never sharded.
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
    })
}
