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
use nodedb_types::InstantKind;

use crate::engine::timeseries::columnar_memtable::{ColumnType, ColumnarSchema, TimeKind};
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

    /// Run `f` against the memtable schema that types this collection's
    /// columns: the resident memtable's when one exists, else the declared
    /// one. A memtable is created on first ingest, so a node serving only
    /// flushed partitions after a restart has none, and the declared shape
    /// is the same schema that memtable would have been built from. `None`
    /// when the collection has neither.
    fn with_ts_schema<R>(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        f: impl FnOnce(&ColumnarSchema) -> R,
    ) -> Option<R> {
        let key = (database_id, tid, collection.to_string());
        if let Some(memtable) = self.columnar_memtables.get(&key) {
            return Some(f(memtable.schema()));
        }
        self.declared_ts_memtable_schema(database_id, tid, collection)
            .map(|schema| f(&schema))
    }

    /// How each named GROUP BY column of a timeseries collection renders.
    ///
    /// A grouped column must carry the type it carries ungrouped, so this
    /// answers from the memtable schema, whose column types carry the time
    /// kind. A column absent from the schema, or a collection with no schema
    /// at all, renders as text.
    pub(in crate::data::executor) fn ts_group_key_kinds(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        group_by: &[String],
    ) -> Vec<TsGroupKeyKind> {
        self.with_ts_schema(database_id, tid, collection, |schema| {
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
        })
        .unwrap_or_else(|| vec![TsGroupKeyKind::Text; group_by.len()])
    }

    /// Columns of a timeseries collection whose memtable type is an instant.
    ///
    /// The memtable keeps every time column in epoch milliseconds, while a
    /// client reads a `TIMESTAMP` cell as epoch microseconds, so row emission
    /// scales exactly these columns.
    ///
    /// A `BIGINT TIME_KEY` shares the same millisecond storage but its kind
    /// is `Millis`, so it is absent from this list and hands back the number
    /// that was inserted. A collection with no schema yields an empty list.
    pub(in crate::data::executor) fn ts_instant_columns(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) -> Vec<String> {
        self.with_ts_schema(database_id, tid, collection, |schema| {
            schema
                .columns
                .iter()
                .filter(|(_, ty)| matches!(ty, ColumnType::Timestamp(TimeKind::Instant(_))))
                .map(|(name, _)| name.clone())
                .collect()
        })
        .unwrap_or_default()
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
    Instant(InstantKind),
    /// An integer column, including a `BIGINT TIME_KEY`, in its stored unit.
    Integer,
    /// A floating-point column.
    Float,
    /// A dictionary symbol, or a column with no resolvable storage type.
    Text,
}

/// The wire shape a memtable storage type renders as.
///
/// A time column answers from its own kind: an instant renders as one, a
/// `Millis` column (a `BIGINT` time key or the system-time column) renders
/// as the number storage holds.
fn kind_of_storage(storage: ColumnType) -> TsGroupKeyKind {
    match storage {
        ColumnType::Timestamp(TimeKind::Instant(kind)) => TsGroupKeyKind::Instant(kind),
        ColumnType::Int64 | ColumnType::Timestamp(TimeKind::Millis) => TsGroupKeyKind::Integer,
        ColumnType::Float64 => TsGroupKeyKind::Float,
        ColumnType::Symbol => TsGroupKeyKind::Text,
    }
}

/// Map a declared SQL type onto the memtable's storage type.
///
/// The designated time key is always a memtable `Timestamp` column. Its
/// kind comes from the declared type: `TIMESTAMP` and `TIMESTAMPTZ` are
/// instants, while a `BIGINT` time key (or any other spelling) is `Millis`.
/// The engine-assigned system-time column is `Millis` too.
fn memtable_column_type(declared_type: &str, is_time_key: bool) -> ColumnType {
    use nodedb_types::columnar::ColumnType as DeclaredType;

    match DeclaredType::from_declared_type(declared_type) {
        Some(DeclaredType::Timestamp) => {
            ColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive))
        }
        Some(DeclaredType::Timestamptz) => {
            ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc))
        }
        Some(DeclaredType::SystemTimestamp) => ColumnType::Timestamp(TimeKind::Millis),
        _ if is_time_key => ColumnType::Timestamp(TimeKind::Millis),
        Some(DeclaredType::Int64) => ColumnType::Int64,
        // The memtable has no boolean column; ILP ingest widens booleans to
        // f64, so a declared BOOLEAN lands in the same place.
        Some(DeclaredType::Float64 | DeclaredType::Bool | DeclaredType::Decimal { .. }) => {
            ColumnType::Float64
        }
        // Everything else — TEXT, UUID, JSON, and any type the memtable
        // cannot represent natively — is stored as a dictionary symbol.
        _ => ColumnType::Symbol,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MILLIS: ColumnType = ColumnType::Timestamp(TimeKind::Millis);
    const NAIVE: ColumnType = ColumnType::Timestamp(TimeKind::Instant(InstantKind::Naive));
    const UTC: ColumnType = ColumnType::Timestamp(TimeKind::Instant(InstantKind::Utc));

    #[test]
    fn time_key_is_the_timestamp_column_whatever_its_declared_type() {
        assert!(memtable_column_type("BIGINT TIME_KEY", true).is_time());
        assert!(memtable_column_type("TIMESTAMP TIME_KEY", true).is_time());
        assert!(memtable_column_type("TIMESTAMPTZ", true).is_time());
        assert!(memtable_column_type("TEXT", true).is_time());
    }

    /// The kind follows the declared type: a `BIGINT` time key reads back as
    /// the integer stored, a `TIMESTAMP` as a naive instant, a `TIMESTAMPTZ`
    /// as a UTC instant. The system-time column is engine-assigned and reads
    /// back as milliseconds.
    #[test]
    fn time_kind_follows_the_declared_type() {
        assert_eq!(memtable_column_type("BIGINT TIME_KEY", true), MILLIS);
        assert_eq!(memtable_column_type("TIMESTAMP TIME_KEY", true), NAIVE);
        assert_eq!(memtable_column_type("timestamp", true), NAIVE);
        assert_eq!(memtable_column_type("TIMESTAMPTZ", true), UTC);
        assert_eq!(memtable_column_type("TIMESTAMPTZ", false), UTC);
        assert_eq!(memtable_column_type("SYSTEM_TIMESTAMP", false), MILLIS);
        assert_eq!(memtable_column_type("SYSTEM_TIMESTAMP", true), MILLIS);
    }

    #[test]
    fn group_key_kind_comes_from_the_column_kind() {
        assert_eq!(
            kind_of_storage(NAIVE),
            TsGroupKeyKind::Instant(InstantKind::Naive)
        );
        assert_eq!(
            kind_of_storage(UTC),
            TsGroupKeyKind::Instant(InstantKind::Utc)
        );
        assert_eq!(kind_of_storage(MILLIS), TsGroupKeyKind::Integer);
        assert_eq!(kind_of_storage(ColumnType::Int64), TsGroupKeyKind::Integer);
        assert_eq!(kind_of_storage(ColumnType::Float64), TsGroupKeyKind::Float);
        assert_eq!(kind_of_storage(ColumnType::Symbol), TsGroupKeyKind::Text);
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
        assert_eq!(memtable_column_type("TIMESTAMP", false), NAIVE);
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
