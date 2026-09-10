// SPDX-License-Identifier: BUSL-1.1

//! The declared shape of a timeseries collection, as seen by the Data Plane.
//!
//! A timeseries collection's storage layout comes from its DDL: the column
//! list in declaration order, and the designated `TIME_KEY` column that
//! becomes the memtable's timestamp column. Both travel from the catalog to
//! every core on `DocumentOp::Register` (live DDL and boot rehydration alike)
//! and land in `doc_configs`.
//!
//! Every path that needs to know which column carries the collection's time
//! reads [`CoreLoop::declared_ts_time_key`]. None may guess it from a column
//! name: a user is free to call the time key `ts`, `captured_at`, or
//! `reading_moment`, and free to have an ordinary column called `timestamp`.
//!
//! Collections ingested over the raw ILP protocol have no DDL at all — for
//! those the schema is still inferred from the first batch, with the ILP
//! line's own timestamp as the time column.

use nodedb_physical::physical_plan::TimeseriesSchema;

use crate::engine::timeseries::columnar_memtable::{ColumnType, ColumnarSchema};
use crate::types::{DatabaseId, TenantId};

use super::state::CoreLoop;

/// The name schema inference gives the designated time column when a
/// measurement arrives with no DDL behind it (raw ILP protocol ingest).
pub(in crate::data::executor) const INFERRED_TIME_COLUMN: &str = "timestamp";

impl CoreLoop {
    /// The declared timeseries shape for a collection, or `None` when the
    /// collection is not a timeseries collection (or predates registration).
    pub(in crate::data::executor) fn declared_timeseries(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> Option<&TimeseriesSchema> {
        let key = (database_id, tid, collection.to_string());
        self.doc_configs
            .get(&key)
            .and_then(|c| c.timeseries.as_deref())
    }

    /// The declared `TIME_KEY` column name for a timeseries collection.
    pub(in crate::data::executor) fn declared_ts_time_key(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> Option<&str> {
        self.declared_timeseries(database_id, tid, collection)
            .map(|ts| ts.time_key.as_str())
    }

    /// The name of the column that carries this collection's time.
    ///
    /// The DDL-declared `TIME_KEY` when there is one. A measurement ingested
    /// over the raw ILP protocol has no declaration, so its schema was
    /// inferred — the resident memtable's own designated column is then the
    /// authority, and `INFERRED_TIME_COLUMN` is the name inference assigns
    /// before any memtable exists.
    pub(in crate::data::executor) fn ts_time_column(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> String {
        if let Some(declared) = self.declared_ts_time_key(database_id, tid, collection) {
            return declared.to_string();
        }
        let key = (database_id, tid, collection.to_string());
        self.columnar_memtables
            .get(&key)
            .and_then(|mt| {
                let schema = mt.schema();
                schema
                    .columns
                    .get(schema.timestamp_idx)
                    .map(|(name, _)| name.clone())
            })
            .unwrap_or_else(|| INFERRED_TIME_COLUMN.to_string())
    }

    /// Build the memtable schema a timeseries collection declared.
    ///
    /// Columns keep their declared order and the time key keeps its declared
    /// position, so `SELECT *` projects exactly what the user wrote. Returns
    /// `None` when the collection has no declared shape, or when the catalog
    /// record is inconsistent (time key absent from the column list) — the
    /// caller then falls back to inference rather than building a memtable
    /// with no timestamp column.
    pub(in crate::data::executor) fn declared_ts_memtable_schema(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> Option<ColumnarSchema> {
        let declared = self.declared_timeseries(database_id, tid, collection)?;
        let timestamp_idx = declared.time_key_index()?;
        let columns: Vec<(String, ColumnType)> = declared
            .columns
            .iter()
            .enumerate()
            .map(|(i, (name, type_str))| {
                (
                    name.clone(),
                    memtable_column_type(type_str, i == timestamp_idx),
                )
            })
            .collect();
        Some(ColumnarSchema {
            codecs: vec![nodedb_codec::ColumnCodec::Auto; columns.len()],
            columns,
            timestamp_idx,
        })
    }

    /// How each named GROUP BY column of a timeseries collection renders.
    ///
    /// A grouped column must carry the type it carries ungrouped, so this
    /// resolves the same declared shape row emission reads. A declared
    /// collection answers from its DDL; a measurement ingested over the raw
    /// ILP protocol has no DDL, so its resident memtable schema answers.
    /// A column present in neither renders as text.
    pub(in crate::data::executor) fn ts_group_key_kinds(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        group_by: &[String],
    ) -> Vec<TsGroupKeyKind> {
        if let Some(declared) = self.declared_timeseries(database_id, tid, collection) {
            let time_key_index = declared.time_key_index();
            return group_by
                .iter()
                .map(|name| {
                    let Some(index) = declared.columns.iter().position(|(c, _)| c == name) else {
                        return TsGroupKeyKind::Text;
                    };
                    let declared_type = declared.columns[index].1.as_str();
                    if declared_type_is_instant(declared_type) {
                        return TsGroupKeyKind::Instant;
                    }
                    kind_of_storage(memtable_column_type(
                        declared_type,
                        Some(index) == time_key_index,
                    ))
                })
                .collect();
        }

        let key = (database_id, tid, collection.to_string());
        let Some(memtable) = self.columnar_memtables.get(&key) else {
            return vec![TsGroupKeyKind::Text; group_by.len()];
        };
        let schema = memtable.schema();
        group_by
            .iter()
            .map(|name| {
                schema
                    .columns
                    .iter()
                    .find(|(c, _)| c == name)
                    .map(|(_, ty)| kind_of_storage(*ty))
                    .unwrap_or(TsGroupKeyKind::Text)
            })
            .collect()
    }

    /// Declared columns of a timeseries collection that carry an instant.
    ///
    /// A column declared `TIMESTAMP` or `TIMESTAMPTZ` is one. The memtable
    /// keeps every timestamp column in epoch milliseconds, while a client
    /// reads a `TIMESTAMP` cell as epoch microseconds, so row emission scales
    /// exactly these columns.
    ///
    /// A `BIGINT TIME_KEY` shares the same millisecond column and is absent
    /// from this list: its declared type is an integer, so it hands back the
    /// number that was inserted.
    ///
    /// An undeclared measurement (raw ILP protocol ingest) has no entry and
    /// yields an empty list — the planner types its columns as text, so no
    /// cell of it is read as an instant.
    pub(in crate::data::executor) fn ts_instant_columns(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> Vec<String> {
        let Some(declared) = self.declared_timeseries(database_id, tid, collection) else {
            return Vec::new();
        };
        declared
            .columns
            .iter()
            .filter(|(_, type_str)| declared_type_is_instant(type_str))
            .map(|(name, _)| name.clone())
            .collect()
    }
}

/// How one grouped timeseries column renders in an aggregate result row.
///
/// The grouped scan reduces every key to a string, so emission has to put the
/// column's own type back. The variants name the four shapes a stored
/// timeseries column can take on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum TsGroupKeyKind {
    /// A declared `TIMESTAMP` / `TIMESTAMPTZ` column: epoch microseconds.
    Instant,
    /// An integer column, including a `BIGINT TIME_KEY`, in its stored unit.
    Integer,
    /// A floating-point column.
    Float,
    /// A dictionary symbol, or a column with no resolvable storage type.
    Text,
}

/// The wire shape a memtable storage type renders as.
///
/// `Timestamp` maps to `Integer` here: the instant case is decided from the
/// declared DDL type before this runs, so what reaches it is a `BIGINT`
/// time key or a system-time column, both of which render as the number
/// storage holds.
fn kind_of_storage(storage: ColumnType) -> TsGroupKeyKind {
    match storage {
        ColumnType::Int64 | ColumnType::Timestamp => TsGroupKeyKind::Integer,
        ColumnType::Float64 => TsGroupKeyKind::Float,
        ColumnType::Symbol => TsGroupKeyKind::Text,
    }
}

/// Whether a declared DDL type makes a column an instant on the wire.
///
/// Mirrors the two spellings the planner resolves to `SqlDataType::Timestamp`
/// in `control::planner::catalog_adapter::type_convert::parse_type_str`. The
/// two must name the same set: the planner decides how a cell is READ, this
/// decides the unit it is WRITTEN in.
///
/// `SYSTEM_TIMESTAMP` is deliberately absent — the planner types it as text.
fn declared_type_is_instant(declared_type: &str) -> bool {
    let bare = declared_type.split_whitespace().next().unwrap_or("");
    matches!(
        bare.parse::<nodedb_types::columnar::ColumnType>(),
        Ok(nodedb_types::columnar::ColumnType::Timestamp)
            | Ok(nodedb_types::columnar::ColumnType::Timestamptz)
    )
}

/// Map a declared SQL type onto the memtable's storage type.
///
/// The designated time key is always the memtable's `Timestamp` column
/// regardless of how it was spelled — `TIMESTAMP`, `TIMESTAMPTZ`, and
/// `BIGINT` time keys all store epoch milliseconds.
fn memtable_column_type(declared_type: &str, is_time_key: bool) -> ColumnType {
    if is_time_key {
        return ColumnType::Timestamp;
    }
    let bare = declared_type.split_whitespace().next().unwrap_or("");
    match bare.parse::<nodedb_types::columnar::ColumnType>() {
        Ok(nodedb_types::columnar::ColumnType::Timestamp)
        | Ok(nodedb_types::columnar::ColumnType::Timestamptz)
        | Ok(nodedb_types::columnar::ColumnType::SystemTimestamp) => ColumnType::Timestamp,
        Ok(nodedb_types::columnar::ColumnType::Int64) => ColumnType::Int64,
        // The memtable has no boolean column; ILP ingest has always widened
        // booleans to f64, so a declared BOOLEAN lands in the same place.
        Ok(nodedb_types::columnar::ColumnType::Float64)
        | Ok(nodedb_types::columnar::ColumnType::Bool)
        | Ok(nodedb_types::columnar::ColumnType::Decimal { .. }) => ColumnType::Float64,
        // Everything else — TEXT, UUID, JSON, and any type the memtable
        // cannot represent natively — is stored as a dictionary symbol.
        _ => ColumnType::Symbol,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_key_is_the_timestamp_column_whatever_its_declared_type() {
        assert_eq!(
            memtable_column_type("BIGINT TIME_KEY", true),
            ColumnType::Timestamp
        );
        assert_eq!(
            memtable_column_type("TIMESTAMP TIME_KEY", true),
            ColumnType::Timestamp
        );
        assert_eq!(
            memtable_column_type("TIMESTAMPTZ", true),
            ColumnType::Timestamp
        );
    }

    #[test]
    fn declared_types_map_onto_memtable_storage() {
        assert_eq!(memtable_column_type("BIGINT", false), ColumnType::Int64);
        assert_eq!(
            memtable_column_type("INT NOT NULL", false),
            ColumnType::Int64
        );
        assert_eq!(memtable_column_type("FLOAT", false), ColumnType::Float64);
        assert_eq!(memtable_column_type("BOOLEAN", false), ColumnType::Float64);
        assert_eq!(memtable_column_type("TEXT", false), ColumnType::Symbol);
        assert_eq!(memtable_column_type("VARCHAR", false), ColumnType::Symbol);
        assert_eq!(memtable_column_type("UUID", false), ColumnType::Symbol);
    }

    #[test]
    fn a_second_timestamp_column_is_still_a_timestamp_column() {
        // Only the designated key drives partitioning, but a non-key
        // timestamp column keeps timestamp storage — its value comes from the
        // row, not from the ingest line's clock.
        assert_eq!(
            memtable_column_type("TIMESTAMP", false),
            ColumnType::Timestamp
        );
    }

    /// The declared types the planner reads back as instants are exactly the
    /// ones emission scales. A `BIGINT` time key is stored in the same
    /// millisecond column and must NOT be scaled.
    #[test]
    fn only_declared_timestamp_types_are_instants() {
        assert!(declared_type_is_instant("TIMESTAMP TIME_KEY"));
        assert!(declared_type_is_instant("TIMESTAMPTZ"));
        assert!(declared_type_is_instant("timestamp"));
        assert!(!declared_type_is_instant("BIGINT TIME_KEY"));
        assert!(!declared_type_is_instant("INT"));
        assert!(!declared_type_is_instant("TEXT"));
        assert!(!declared_type_is_instant("SYSTEM_TIMESTAMP"));
        assert!(!declared_type_is_instant(""));
    }

    #[test]
    fn unknown_types_fall_back_to_symbol() {
        assert_eq!(
            memtable_column_type("SOMETHING_ELSE", false),
            ColumnType::Symbol
        );
        assert_eq!(memtable_column_type("", false), ColumnType::Symbol);
    }
}
