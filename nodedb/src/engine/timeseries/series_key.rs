// SPDX-License-Identifier: BUSL-1.1

//! The series a stored timeseries row belongs to.

use nodedb_types::timeseries::{SeriesCatalog, SeriesKey};
use nodedb_types::{CollectionKey, CollectionKeyError, DatabaseId};

use super::columnar_memtable::{ColumnType, ColumnValue, ColumnarSchema};

/// The series key of a stored row: its measurement's bare collection name
/// and every non-NULL `Symbol` cell it stores, as `(column, value)`.
///
/// The measurement names the routed collection in `database_id`. A routed
/// batch carries it qualified. A client's ILP line carries it bare. A series
/// catalog belongs to one collection, so the key names the bare collection:
/// a row moved or restored into another database keeps the series its
/// catalog names. A measurement of any other form is an error.
///
/// The column type decides, never how a writer spelled the value. A
/// `Symbol` column holds an ILP tag and an ILP string field alike, and a
/// SQL, JSON or msgpack row carries no tag set at all. A key taken from the
/// line's tag set would split one collection's rows into different series
/// by protocol. A row read back and written again (a restore, a clone copy)
/// also gets the key it had. A `Text` cell is a field and names no tag, so
/// a high-cardinality string never mints a series.
///
/// `values` are the row's cells in `schema` order, as `line_row` and the
/// resolved ingest build them.
pub fn row_series_key(
    database_id: DatabaseId,
    measurement: &str,
    schema: &ColumnarSchema,
    values: &[ColumnValue],
) -> Result<SeriesKey, CollectionKeyError> {
    let collection = CollectionKey::from_relation_name(database_id, measurement)?;
    let tags: Vec<(String, String)> = schema
        .columns
        .iter()
        .zip(values)
        .filter_map(|((name, column_type), value)| match (column_type, value) {
            (ColumnType::Symbol, ColumnValue::Symbol(text)) => Some((name.clone(), text.clone())),
            _ => None,
        })
        .collect();
    Ok(SeriesKey::new(collection.name(), tags))
}

/// The series ID of `key`, resolved through `catalog`.
///
/// Resolve through the catalog, never `to_series_id(0)` directly. The natural
/// hash can collide, and both consumers of the ID (`memtable.ingest_row`
/// row-count stats and the last-value cache) key on it. A collision taken at
/// face value folds one series' rows and last value into an unrelated series.
pub fn resolve_series(
    catalog: &mut SeriesCatalog,
    key: &SeriesKey,
) -> nodedb_types::timeseries::SeriesId {
    let resolved = catalog.resolve_detailed(key);
    if resolved.newly_registered && resolved.rehash_attempt > 0 {
        tracing::warn!(
            metric = %key.metric,
            series_id = resolved.id,
            rehash_attempt = resolved.rehash_attempt,
            "SeriesId hash collision resolved by rehash"
        );
    }
    resolved.id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::TimeKind;

    fn schema() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("v".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        }
    }

    fn values() -> [ColumnValue; 3] {
        [
            ColumnValue::Timestamp(1_000),
            ColumnValue::Symbol("a".into()),
            ColumnValue::Float64(1.0),
        ]
    }

    /// One row keys one series in every database: a series catalog belongs
    /// to its collection, so a moved or restored row keeps its series.
    #[test]
    fn a_row_keys_the_same_series_in_every_database() {
        let (schema, values) = (schema(), values());
        let key = |db: u64, measurement: &str| {
            row_series_key(DatabaseId::new(db), measurement, &schema, &values)
                .expect("measurement of this database")
        };
        let in_default = key(0, "cpu");
        assert_eq!(in_default, key(1024, "1024/cpu"));
        assert_eq!(in_default, key(1025, "1025/cpu"));
        assert_eq!(in_default, key(1025, "cpu"));
        assert_eq!(
            in_default,
            SeriesKey::new("cpu", vec![("host".into(), "a".into())])
        );
    }

    #[test]
    fn a_measurement_of_another_database_is_refused() {
        let (schema, values) = (schema(), values());
        for (db, measurement) in [(1024, "1025/cpu"), (0, "1024/cpu"), (1024, "1024/a/b")] {
            assert!(
                row_series_key(DatabaseId::new(db), measurement, &schema, &values).is_err(),
                "{db} {measurement}"
            );
        }
    }
}
