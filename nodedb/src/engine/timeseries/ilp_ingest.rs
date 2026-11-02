// SPDX-License-Identifier: BUSL-1.1

//! ILP → columnar memtable ingestion bridge.
//!
//! Accumulates parsed ILP lines into batches and flushes them to the
//! columnar memtable. Schema inference / evolution lives in the sibling
//! `ilp_schema` module.

use std::borrow::Cow;

use super::columnar_memtable::{ColumnType, ColumnValue, ColumnarMemtable};
use super::ilp::{FieldValue, IlpLine};
use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use nodedb_types::timeseries::{IngestResult, SeriesCatalog, SeriesKey};

pub use super::ilp_schema::{
    ensure_bitemporal_columns, ensure_system_column, evolve_schema, infer_schema, new_columns,
};

/// The system-time stamp every row of one ingest batch stores.
///
/// `system_ordinal` fills `_ts_system` and is always engine-assigned: a
/// client-supplied value is ignored. On a bitemporal collection the
/// valid-time pair comes from the line's `_ts_valid_from` /
/// `_ts_valid_until` fields, or defaults to the open interval.
#[derive(Clone, Copy)]
pub struct RowStamp {
    /// The system-time ordinal of the batch.
    pub system_ordinal: i64,
    /// Whether the collection is `bitemporal=true`, so the valid-time
    /// columns take their open-interval defaults.
    pub bitemporal: bool,
}

/// What an ILP batch ingest produced.
///
/// The row indices are reported alongside the counts, not derived from them,
/// because only this loop knows which lines actually landed: a rejected line
/// advances `rejected` and appends nothing, so position in the input is not
/// position in the memtable.
pub struct IngestBatchOutcome {
    pub accepted: usize,
    pub rejected: usize,
    /// Why the FIRST rejected row was rejected. Kept so a caller that cannot
    /// tolerate a partial batch can say what went wrong rather than only how
    /// many rows vanished.
    pub first_rejection: Option<String>,
    /// Memtable row index of each accepted row, in insert order. Empty unless
    /// the caller asked for it — a large batch would otherwise pay a `usize`
    /// per row for something almost every ingest discards.
    pub accepted_row_indices: Vec<usize>,
    /// Input line index of each accepted row, index-aligned with
    /// `accepted_row_indices`. Empty under the same condition.
    pub accepted_line_indices: Vec<usize>,
}

/// Inputs to [`ingest_batch_with_lvc`].
pub struct IngestBatchArgs<'a, 'l> {
    pub memtable: &'a mut ColumnarMemtable,
    pub lines: &'a [IlpLine<'l>],
    pub catalog: &'a mut SeriesCatalog,
    pub default_timestamp_ms: i64,
    pub lvc: Option<&'a mut super::last_value_cache::LastValueCache>,
    /// The batch's system-time stamp. `None` stores `_ts_system` from the
    /// line's own fields, as any other column.
    pub stamp: Option<RowStamp>,
    /// Record where each accepted row landed, so the caller can read those
    /// exact rows back through the ordinary scan projection.
    pub collect_row_indices: bool,
}

/// Ingest a batch of parsed ILP lines into a columnar memtable.
///
/// The memtable's schema must already be set. Tag/field values are mapped
/// to the schema's column order.
///
/// Returns (accepted_count, rejected_count).
pub fn ingest_batch(
    memtable: &mut ColumnarMemtable,
    lines: &[IlpLine<'_>],
    catalog: &mut SeriesCatalog,
    default_timestamp_ms: i64,
) -> (usize, usize) {
    let outcome = ingest_batch_with_lvc(IngestBatchArgs {
        memtable,
        lines,
        catalog,
        default_timestamp_ms,
        lvc: None,
        stamp: None,
        collect_row_indices: false,
    });
    (outcome.accepted, outcome.rejected)
}

/// Ingest a batch of ILP lines with optional last-value cache update.
///
/// When `stamp` is `Some`, every row stores its `system_ordinal` in the
/// `_ts_system` column. On a bitemporal stamp `_ts_valid_from` /
/// `_ts_valid_until` come from the line's field set when present, and
/// default to the open interval `[i64::MIN, i64::MAX)`.
pub fn ingest_batch_with_lvc(args: IngestBatchArgs<'_, '_>) -> IngestBatchOutcome {
    let IngestBatchArgs {
        memtable,
        lines,
        catalog,
        default_timestamp_ms,
        mut lvc,
        stamp,
        collect_row_indices,
    } = args;
    let schema = memtable.schema().clone();
    let mut accepted = 0;
    let mut rejected = 0;
    let mut first_rejection: Option<String> = None;
    let mut accepted_row_indices: Vec<usize> = Vec::new();
    let mut accepted_line_indices: Vec<usize> = Vec::new();

    for (line_index, line) in lines.iter().enumerate() {
        // A value of another type than its column is refused whole: storing
        // it coerces a string to NaN or 0, or a number to an empty
        // symbol.
        if let Some(conflict) = line_type_conflict(&schema, line) {
            rejected += 1;
            first_rejection.get_or_insert(conflict);
            continue;
        }
        let (ts_ms, values) = line_row(&schema, line, default_timestamp_ms, stamp);
        let key = row_series_key(line.measurement.as_ref(), &schema, &values);
        let series_id = resolve_series(catalog, &key);

        // `ingest_row` appends at the tail of every column and rolls the
        // partial row back on error, so the row this call lands at is exactly
        // the row count observed before it. Read before the call, because the
        // count has already moved by the time it returns.
        let landing_index = memtable.row_count() as usize;
        match memtable.ingest_row(series_id, &values) {
            Ok(IngestResult::Rejected) => {
                rejected += 1;
                first_rejection.get_or_insert_with(|| {
                    "memtable rejected the row: memory budget exhausted".to_string()
                });
            }
            Ok(_) => {
                accepted += 1;
                if collect_row_indices {
                    accepted_row_indices.push(landing_index);
                    accepted_line_indices.push(line_index);
                }
                if let Some(ref mut cache) = lvc {
                    update_last_value(cache, &schema, series_id, ts_ms, &values);
                }
            }
            Err(error) => {
                rejected += 1;
                // The engine's own message names the column and the rule it
                // broke ("type mismatch at column N", "tag cardinality limit
                // exceeded for column 'x'"). Keeping it is what lets a caller
                // report why a row was dropped instead of only that one was.
                first_rejection.get_or_insert_with(|| error.to_string());
            }
        }
    }

    IngestBatchOutcome {
        accepted,
        rejected,
        first_rejection,
        accepted_row_indices,
        accepted_line_indices,
    }
}

/// Why `line` cannot be stored under `schema`: a field or tag whose value is
/// of another type than its column. `None` when every value fits.
///
/// - A numeric column (`Float64`, `Int64`) takes a numeric or boolean field.
///   A string field or a tag conflicts.
/// - A `Symbol` or `Text` column takes a tag or a string field, stored as
///   the column's type. A numeric or boolean field conflicts.
/// - A `Timestamp` column takes a numeric field or a datetime string.
/// - An `Int64` or `Timestamp` column refuses an unsigned field past
///   `i64::MAX`.
pub fn line_type_conflict(
    schema: &super::columnar_memtable::ColumnarSchema,
    line: &IlpLine<'_>,
) -> Option<String> {
    let column_type = |name: &str| {
        schema
            .columns
            .iter()
            .find(|(column, _)| column == name)
            .map(|(_, column_type)| *column_type)
    };
    for (key, _) in &line.tags {
        if let Some(column_type @ (ColumnType::Float64 | ColumnType::Int64)) =
            column_type(key.as_ref())
        {
            return Some(format!(
                "type conflict on '{key}': a tag cannot be stored in a {column_type:?} column"
            ));
        }
    }
    for (key, value) in &line.fields {
        let Some(column_type) = column_type(key.as_ref()) else {
            continue;
        };
        // An `Int64` or `Timestamp` cell is an `i64`: an unsigned field past
        // `i64::MAX` would wrap to a negative number.
        if let (ColumnType::Int64 | ColumnType::Timestamp(_), FieldValue::UInt(u)) =
            (column_type, value)
            && i64::try_from(*u).is_err()
        {
            return Some(format!(
                "value {u} of '{key}' is out of range for a {column_type:?} column"
            ));
        }
        let fits = match column_type {
            ColumnType::Float64 | ColumnType::Int64 => !matches!(value, FieldValue::Str(_)),
            ColumnType::Symbol | ColumnType::Text => matches!(value, FieldValue::Str(_)),
            ColumnType::Timestamp(_) => true,
        };
        if !fits {
            return Some(format!(
                "type conflict on '{key}': a {} field cannot be stored in a {column_type:?} \
                 column",
                field_kind(value)
            ));
        }
    }
    None
}

/// The ILP type name of `value`, for a type-conflict reason.
fn field_kind(value: &FieldValue<'_>) -> &'static str {
    match value {
        FieldValue::Float(_) => "float",
        FieldValue::Int(_) => "integer",
        FieldValue::UInt(_) => "unsigned integer",
        FieldValue::Bool(_) => "boolean",
        FieldValue::Str(_) => "string",
    }
}

/// The series key of a stored row: its measurement's bare collection name
/// and every non-NULL `Symbol` cell it stores, as `(column, value)`.
///
/// The measurement is the routed collection, qualified by its database.
/// A series catalog belongs to one collection, so the key names the bare
/// collection: a row moved or restored into another database keeps the
/// series its catalog names.
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
    measurement: &str,
    schema: &super::columnar_memtable::ColumnarSchema,
    values: &[ColumnValue],
) -> SeriesKey {
    let tags: Vec<(String, String)> = schema
        .columns
        .iter()
        .zip(values)
        .filter_map(|((name, column_type), value)| match (column_type, value) {
            (super::columnar_memtable::ColumnType::Symbol, ColumnValue::Symbol(text)) => {
                Some((name.clone(), text.clone()))
            }
            _ => None,
        })
        .collect();
    SeriesKey::new(
        nodedb_types::QualifiedCollection::bare_of_stored(measurement),
        tags,
    )
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

/// Record a stored row in the last-value cache: its first Float64 value
/// that is not NaN (`0.0` when none), at the system-time ordinal its
/// `_ts_system` cell holds (`0` without one). A float column stores SQL
/// NULL as NaN, so a stored row reads back the same value
/// ([`super::last_value_cache::row_last_value`]).
pub fn update_last_value(
    cache: &mut super::last_value_cache::LastValueCache,
    schema: &super::columnar_memtable::ColumnarSchema,
    series_id: nodedb_types::timeseries::SeriesId,
    ts_ms: i64,
    values: &[ColumnValue],
) {
    let value = values
        .iter()
        .find_map(|v| match v {
            ColumnValue::Float64(f) if !f.is_nan() => Some(*f),
            _ => None,
        })
        .unwrap_or(0.0);
    let ordinal = match schema.ts_system_idx().and_then(|idx| values.get(idx)) {
        Some(ColumnValue::Int64(ordinal)) => *ordinal,
        _ => 0,
    };
    cache.update(series_id, ts_ms, value, ordinal);
}

/// The timestamp and the column values `line` stores under `schema`, in
/// schema order.
pub fn line_row(
    schema: &super::columnar_memtable::ColumnarSchema,
    line: &IlpLine<'_>,
    default_timestamp_ms: i64,
    stamp: Option<RowStamp>,
) -> (i64, Vec<ColumnValue>) {
    let ts_ms = line
        .timestamp_ns
        .map(|ns| ns / 1_000_000) // ns → ms
        .unwrap_or(default_timestamp_ms);
    let mut values: Vec<ColumnValue> = Vec::with_capacity(schema.columns.len());
    {
        for (col_idx, (col_name, col_type)) in schema.columns.iter().enumerate() {
            match col_type {
                // Only the designated time column takes the line's timestamp.
                // Any other timestamp column is an ordinary column and carries
                // whatever the row itself supplied.
                ColumnType::Timestamp(_) if col_idx == schema.timestamp_idx => {
                    values.push(ColumnValue::Timestamp(ts_ms));
                }
                ColumnType::Timestamp(_) => {
                    values.push(ColumnValue::Timestamp(find_field_timestamp_ms(
                        &line.fields,
                        col_name,
                    )));
                }
                ColumnType::Symbol | ColumnType::Text => {
                    // Look up tag value first (tags are borrowed &str), then
                    // string field value (now an owned String after ILP
                    // unescape). A line that names neither stores NULL, never
                    // an empty string: `''` is a value of its own.
                    let val = line
                        .tags
                        .iter()
                        .find(|(key, _)| key.as_ref() == col_name)
                        .map(|(_, value)| value.to_string())
                        .or_else(|| find_field_str(&line.fields, col_name));
                    values.push(val.map_or(ColumnValue::Null, ColumnValue::Symbol));
                }
                ColumnType::Float64 => {
                    let val = find_field_f64(&line.fields, col_name);
                    values.push(ColumnValue::Float64(val));
                }
                ColumnType::Int64 => {
                    let val = match (stamp, col_name.as_str()) {
                        (Some(s), TS_SYSTEM) => s.system_ordinal,
                        (Some(s), TS_VALID_FROM) if s.bitemporal => {
                            find_field_i64_opt(&line.fields, col_name).unwrap_or(i64::MIN)
                        }
                        (Some(s), TS_VALID_UNTIL) if s.bitemporal => {
                            find_field_i64_opt(&line.fields, col_name).unwrap_or(i64::MAX)
                        }
                        _ => find_field_i64(&line.fields, col_name),
                    };
                    values.push(ColumnValue::Int64(val));
                }
            }
        }
    }
    (ts_ms, values)
}

/// Read a non-designated timestamp column's value from the line's field set.
///
/// Numeric fields are epoch milliseconds; string fields are parsed as a
/// datetime literal. A field that is absent or unparseable yields 0 — the same
/// "no value supplied" floor the numeric column readers use.
fn find_field_timestamp_ms<'a>(fields: &[(Cow<'a, str>, FieldValue<'a>)], name: &str) -> i64 {
    for (key, value) in fields {
        if key.as_ref() != name {
            continue;
        }
        return match value {
            FieldValue::Int(i) => *i,
            FieldValue::UInt(u) => *u as i64,
            FieldValue::Float(f) => *f as i64,
            FieldValue::Bool(_) => 0,
            FieldValue::Str(text) => nodedb_types::datetime::NdbDateTime::parse(text.as_ref())
                .map(|dt| dt.unix_millis())
                .unwrap_or(0),
        };
    }
    0
}

fn find_field_str<'a>(fields: &[(Cow<'a, str>, FieldValue<'a>)], name: &str) -> Option<String> {
    find_field_str_ref(fields, name).map(str::to_string)
}

/// Borrow a string field's value instead of copying it.
///
/// The ingest path needs an owned `String` because `ColumnValue::Symbol` owns
/// its value, but the admission gate only needs to PROBE the symbol
/// dictionaries with it. Both go through this one lookup so the gate can never
/// answer about a different value than the ingest would insert.
pub(crate) fn find_field_str_ref<'f, 'a>(
    fields: &'f [(Cow<'a, str>, FieldValue<'a>)],
    name: &str,
) -> Option<&'f str> {
    for (key, value) in fields {
        if key.as_ref() == name
            && let FieldValue::Str(value) = value
        {
            return Some(value.as_ref());
        }
    }
    None
}

fn find_field_f64<'a>(fields: &[(Cow<'a, str>, FieldValue<'a>)], name: &str) -> f64 {
    for (key, value) in fields {
        if key.as_ref() == name {
            return match value {
                FieldValue::Float(f) => *f,
                FieldValue::Int(i) => *i as f64,
                FieldValue::UInt(u) => *u as f64,
                FieldValue::Bool(b) => {
                    if *b {
                        1.0
                    } else {
                        0.0
                    }
                }
                FieldValue::Str(_) => f64::NAN,
            };
        }
    }
    f64::NAN
}

fn find_field_i64<'a>(fields: &[(Cow<'a, str>, FieldValue<'a>)], name: &str) -> i64 {
    find_field_i64_opt(fields, name).unwrap_or(0)
}

/// The `_ts_system` ordinal `line` carries, as a clone copy's line carries
/// its source version's ordinal. `None` when the line names none.
pub fn line_system_ordinal(line: &IlpLine<'_>) -> Option<i64> {
    find_field_i64_opt(&line.fields, TS_SYSTEM)
}

fn find_field_i64_opt<'a>(fields: &[(Cow<'a, str>, FieldValue<'a>)], name: &str) -> Option<i64> {
    for (key, value) in fields {
        if key.as_ref() == name {
            return Some(match value {
                FieldValue::Int(i) => *i,
                FieldValue::UInt(u) => *u as i64,
                FieldValue::Float(f) => *f as i64,
                FieldValue::Bool(b) => {
                    if *b {
                        1
                    } else {
                        0
                    }
                }
                FieldValue::Str(_) => 0,
            });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnData, ColumnarMemtableConfig, TimeKind,
    };
    use crate::engine::timeseries::ilp::parse_batch;

    fn default_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 10_000,
        }
    }

    /// One row keys one series in every database: a series catalog belongs
    /// to its collection, so a moved or restored row keeps its series.
    #[test]
    fn a_row_keys_the_same_series_in_every_database() {
        let schema = super::super::columnar_memtable::ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("v".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let values = [
            ColumnValue::Timestamp(1_000),
            ColumnValue::Symbol("a".into()),
            ColumnValue::Float64(1.0),
        ];
        let in_default = row_series_key("cpu", &schema, &values);
        assert_eq!(in_default, row_series_key("1024/cpu", &schema, &values));
        assert_eq!(in_default, row_series_key("1025/cpu", &schema, &values));
        assert_eq!(
            in_default,
            nodedb_types::timeseries::SeriesKey::new("cpu", vec![("host".into(), "a".into())])
        );
    }

    #[test]
    fn infer_schema_from_ilp() {
        let input = "cpu,host=a,dc=us value=0.64,count=100i 1000000000\n\
                     cpu,host=b,dc=eu value=0.55,count=200i 2000000000";
        let lines = parse_batch(input).expect("valid ILP batch").into_lines();
        let schema = infer_schema(&lines);

        // timestamp + 2 tags + 2 fields = 5 columns.
        assert_eq!(schema.columns.len(), 5);
        assert_eq!(
            schema.columns[0],
            ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis))
        );
        assert_eq!(schema.columns[1].1, ColumnType::Symbol); // host
        assert_eq!(schema.columns[2].1, ColumnType::Symbol); // dc
        assert_eq!(schema.columns[3].1, ColumnType::Float64); // value
        assert_eq!(schema.columns[4].1, ColumnType::Int64); // count
    }

    /// An unsigned field past `i64::MAX` is refused by an `Int64` column
    /// instead of wrapping to a negative number. One in range is stored.
    #[test]
    fn unsigned_field_past_i64_max_is_refused_not_wrapped() {
        let schema = infer_schema(
            &parse_batch("m count=1i 1000000000")
                .expect("valid ILP batch")
                .into_lines(),
        );
        let lines = parse_batch(
            "m count=18446744073709551615u 1000000000\n\
             m count=9223372036854775807u 2000000000",
        )
        .expect("valid ILP batch")
        .into_lines();
        let conflict = line_type_conflict(&schema, &lines[0]).expect("out of range");
        assert!(conflict.contains("18446744073709551615"), "{conflict}");
        assert_eq!(line_type_conflict(&schema, &lines[1]), None);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut catalog = SeriesCatalog::new();
        assert_eq!(ingest_batch(&mut mt, &lines, &mut catalog, 0), (1, 1));
    }

    /// A plain collection's batch stores its ordinal in `_ts_system` and
    /// leaves every other column to the line, so no valid-time default
    /// appears.
    #[test]
    fn a_plain_batch_stamps_only_the_system_column() {
        let lines = parse_batch("temp,sensor=s1 reading=22.5 1500000000000000")
            .expect("valid ILP batch")
            .into_lines();
        let mut schema = infer_schema(&lines);
        super::super::ilp_schema::ensure_system_column(&mut schema);
        super::super::ilp_schema::ensure_system_column(&mut schema);
        assert_eq!(
            schema
                .columns
                .iter()
                .filter(|(n, _)| n == "_ts_system")
                .count(),
            1,
            "the system column is added once"
        );
        let sys_idx = schema.ts_system_idx().expect("system column");
        let stamp = Some(RowStamp {
            system_ordinal: 7_000_000_123,
            bitemporal: false,
        });
        let (_, values) = line_row(&schema, &lines[0], 0, stamp);
        assert_eq!(values[sys_idx], ColumnValue::Int64(7_000_000_123));
    }

    #[test]
    fn bitemporal_ingest_stamps_reserved_columns() {
        // Late-arriving IoT backfill: an ILP line with a user-provided
        // `_ts_valid_from` reflecting when the measurement was taken, but
        // the server stamps `_ts_system` at ingest time. A subsequent
        // `AS OF SYSTEM TIME` query before `system_now` must exclude the
        // row; an `AS OF VALID TIME` query at the event time must find it.
        let input = "temp,sensor=s1 reading=22.5,_ts_valid_from=1000i,_ts_valid_until=2000i \
                     1500000000000000";
        let lines = parse_batch(input).expect("valid ILP batch").into_lines();

        let mut schema = infer_schema(&lines);
        ensure_bitemporal_columns(&mut schema);
        // All three reserved columns must be present; `_ts_valid_from`
        // and `_ts_valid_until` may come from the line's field set (via
        // `infer_schema`) instead of being appended by
        // `ensure_bitemporal_columns`, so we check set-membership rather
        // than fixed tail-order.
        for name in ["_ts_system", "_ts_valid_from", "_ts_valid_until"] {
            assert!(
                schema.columns.iter().any(|(n, _)| n == name),
                "missing reserved column {name}"
            );
        }

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut catalog = SeriesCatalog::new();
        let stamps = Some(RowStamp {
            system_ordinal: 5_000,
            bitemporal: true,
        });
        let outcome = ingest_batch_with_lvc(IngestBatchArgs {
            memtable: &mut mt,
            lines: &lines,
            catalog: &mut catalog,
            default_timestamp_ms: 0,
            lvc: None,
            stamp: stamps,
            collect_row_indices: true,
        });
        assert_eq!((outcome.accepted, outcome.rejected), (1, 0));
        assert_eq!(
            outcome.accepted_row_indices,
            vec![0],
            "the single accepted row must be reported at memtable row 0"
        );

        // Inspect the memtable row to verify the three reserved slots
        // carry the expected stamps.
        let schema = mt.schema().clone();
        let sys_idx = schema
            .columns
            .iter()
            .position(|(n, _)| n == "_ts_system")
            .unwrap();
        let vf_idx = schema
            .columns
            .iter()
            .position(|(n, _)| n == "_ts_valid_from")
            .unwrap();
        let vu_idx = schema
            .columns
            .iter()
            .position(|(n, _)| n == "_ts_valid_until")
            .unwrap();
        let rows: Vec<Vec<i64>> = (0..mt.row_count() as usize)
            .map(|r| {
                [sys_idx, vf_idx, vu_idx]
                    .iter()
                    .map(|&c| {
                        let col = mt.column(c);
                        if let ColumnData::Int64(vals) = col {
                            vals[r]
                        } else {
                            panic!("expected Int64 column at idx {c}")
                        }
                    })
                    .collect()
            })
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], vec![5_000, 1_000, 2_000]);
    }

    #[test]
    fn ingest_ilp_batch() {
        let input = "cpu,host=server01 usage=0.64 1434055562000000000\n\
                     cpu,host=server02 usage=0.55 1434055563000000000\n\
                     cpu,host=server01 usage=0.72 1434055564000000000";
        let lines = parse_batch(input).expect("valid ILP batch").into_lines();
        let schema = infer_schema(&lines);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut catalog = SeriesCatalog::new();

        let (accepted, rejected) = ingest_batch(&mut mt, &lines, &mut catalog, 0);
        assert_eq!(accepted, 3);
        assert_eq!(rejected, 0);
        assert_eq!(mt.row_count(), 3);
        assert_eq!(catalog.len(), 2); // server01 and server02
    }

    #[test]
    fn ingest_resolves_series_through_the_catalog() {
        // Ingest must take its SeriesId from the catalog, so a hash collision is
        // rehashed away instead of folding two series' row counts and last
        // values into one. The uncontended case is the natural hash.
        let input = "cpu,host=server01 usage=0.64 1434055562000000000\n\
                     cpu,host=server02 usage=0.55 1434055563000000000";
        let lines = parse_batch(input).expect("valid ILP batch").into_lines();
        let schema = infer_schema(&lines);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut catalog = SeriesCatalog::new();
        ingest_batch(&mut mt, &lines, &mut catalog, 0);

        let s1 = SeriesKey::new("cpu", vec![("host".into(), "server01".into())]);
        let s2 = SeriesKey::new("cpu", vec![("host".into(), "server02".into())]);
        assert_eq!(catalog.get(s1.to_series_id(0)), Some(&s1));
        assert_eq!(catalog.get(s2.to_series_id(0)), Some(&s2));

        // A second batch reuses the registrations rather than re-minting IDs.
        ingest_batch(&mut mt, &lines, &mut catalog, 0);
        assert_eq!(catalog.len(), 2);
    }

    #[test]
    fn timestamp_ns_to_ms_conversion() {
        let input = "temp value=22.5 1704067200000000000"; // 2024-01-01 00:00:00 UTC in ns
        let lines = parse_batch(input).expect("valid ILP batch").into_lines();
        let schema = infer_schema(&lines);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut catalog = SeriesCatalog::new();
        ingest_batch(&mut mt, &lines, &mut catalog, 0);

        let ts = mt.column(0).as_timestamps()[0];
        assert_eq!(ts, 1_704_067_200_000); // ms
    }

    #[test]
    fn missing_timestamp_uses_default() {
        let input = "temp value=22.5"; // no timestamp
        let lines = parse_batch(input).expect("valid ILP batch").into_lines();
        let schema = infer_schema(&lines);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut catalog = SeriesCatalog::new();
        let default_ts = 9999;
        ingest_batch(&mut mt, &lines, &mut catalog, default_ts);

        let ts = mt.column(0).as_timestamps()[0];
        assert_eq!(ts, 9999);
    }

    #[test]
    fn mixed_field_types() {
        let input = "sensor temp=72.5,humidity=45i,active=true 1000000000";
        let lines = parse_batch(input).expect("valid ILP batch").into_lines();
        let schema = infer_schema(&lines);

        let mut mt = ColumnarMemtable::new(schema, default_config());
        let mut catalog = SeriesCatalog::new();
        ingest_batch(&mut mt, &lines, &mut catalog, 0);
        assert_eq!(mt.row_count(), 1);
    }

    #[test]
    fn string_fields_stored_as_text() {
        let input =
            r#"dns,client=10.0.0.1 qname="bigquery.googleapis.com",elapsed_ms=12.5 1000000000"#; // gitleaks:allow
        let lines = parse_batch(input).expect("valid ILP batch").into_lines();
        let schema = infer_schema(&lines);

        // qname is a text field: not Float64, and not a Symbol dimension.
        let qname_col = schema.columns.iter().find(|(n, _)| n == "qname").unwrap();
        assert_eq!(qname_col.1, ColumnType::Text);
        let client_col = schema.columns.iter().find(|(n, _)| n == "client").unwrap();
        assert_eq!(client_col.1, ColumnType::Symbol);

        // Ingest and verify the string value is recoverable.
        let mut mt = ColumnarMemtable::new(schema.clone(), default_config());
        let mut catalog = SeriesCatalog::new();
        ingest_batch(&mut mt, &lines, &mut catalog, 0);
        assert_eq!(mt.row_count(), 1);

        // Find qname column index and resolve symbol.
        let col_idx = schema
            .columns
            .iter()
            .position(|(n, _)| n == "qname")
            .unwrap();
        let col_data = mt.column(col_idx);
        if let crate::engine::timeseries::columnar_memtable::ColumnData::Symbol(ids) = col_data {
            let dict = mt.symbol_dict(col_idx).unwrap();
            let resolved = dict.get(ids[0]).unwrap();
            assert_eq!(resolved, "bigquery.googleapis.com");
        } else {
            panic!("expected Symbol column data for qname");
        }
    }

    /// A string field is not a series dimension. A thousand distinct field
    /// values over one tag value give one series, and the field column is
    /// not held to the tag cardinality cap.
    #[test]
    fn distinct_string_field_values_share_one_series() {
        let input: String = (0..1000)
            .map(|i| {
                let ts_ns = 1_000_000 * (i + 1);
                format!("log,host=a msg=\"request {i}\",value={i}i {ts_ns}\n")
            })
            .collect();
        let lines = parse_batch(&input).expect("valid ILP batch").into_lines();
        let schema = infer_schema(&lines);
        let msg_col = schema.columns.iter().find(|(n, _)| n == "msg").unwrap();
        assert_eq!(msg_col.1, ColumnType::Text);

        let mut mt = ColumnarMemtable::new(
            schema,
            ColumnarMemtableConfig {
                max_memory_bytes: 10 * 1024 * 1024,
                hard_memory_limit: 20 * 1024 * 1024,
                max_tag_cardinality: 10,
            },
        );
        let mut catalog = SeriesCatalog::new();
        ingest_batch(&mut mt, &lines, &mut catalog, 0);

        assert_eq!(mt.row_count(), 1000, "no row hits the tag cap");
        assert_eq!(mt.series_count(), 1, "one tag value is one series");
        assert_eq!(catalog.entries().count(), 1, "the catalog holds one series");
    }

    /// A rejected row reports WHY, not just that it happened.
    ///
    /// The reason is what lets a caller that cannot tolerate a partial batch —
    /// a projecting ingest, whose row set has nowhere to carry a `rejected`
    /// count — fail with something actionable instead of silently returning
    /// fewer rows than were submitted.
    #[test]
    fn a_rejected_row_records_its_reason_and_is_not_counted_as_accepted() {
        // One tag value, and a dictionary that admits none of them: the
        // cardinality ceiling is the reachable rejection that does not depend
        // on a malformed line.
        let input = "cpu,host=a value=1.0 1000000000\n";
        let lines = parse_batch(input).expect("valid ILP batch").into_lines();
        let schema = infer_schema(&lines);
        let mut mt = ColumnarMemtable::new(
            schema,
            ColumnarMemtableConfig {
                max_memory_bytes: 10 * 1024 * 1024,
                hard_memory_limit: 20 * 1024 * 1024,
                max_tag_cardinality: 0,
            },
        );
        let mut catalog = SeriesCatalog::new();

        let outcome = ingest_batch_with_lvc(IngestBatchArgs {
            memtable: &mut mt,
            lines: &lines,
            catalog: &mut catalog,
            default_timestamp_ms: 0,
            lvc: None,
            stamp: None,
            collect_row_indices: true,
        });

        assert_eq!(
            (outcome.accepted, outcome.rejected),
            (0, 1),
            "the row must be rejected, not quietly accepted"
        );
        assert!(
            outcome.accepted_row_indices.is_empty(),
            "a rejected row must not be reported as stored: {:?}",
            outcome.accepted_row_indices
        );
        let reason = outcome
            .first_rejection
            .expect("a rejection must carry its reason");
        assert!(
            reason.contains("cardinality"),
            "the reason must name the rule that was broken; got {reason}"
        );
    }

    /// A symbol column the line does not name is NULL. An empty string the
    /// line names is the value `''`.
    #[test]
    fn an_unnamed_symbol_is_null_and_an_empty_one_is_a_value() {
        let schema = super::super::columnar_memtable::ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("note".into(), ColumnType::Symbol),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let parsed = parse_batch("cpu note=\"\",value=1.5 1000000000")
            .expect("parse line")
            .into_lines();
        let line = parsed.first().expect("one line");
        let (_, values) = line_row(&schema, line, 0, None);
        assert_eq!(values[1], ColumnValue::Null, "absent tag is NULL");
        assert_eq!(
            values[2],
            ColumnValue::Symbol(String::new()),
            "'' is a value"
        );
    }
}
