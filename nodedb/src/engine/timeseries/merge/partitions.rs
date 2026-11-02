// SPDX-License-Identifier: BUSL-1.1

//! The rows of several sealed partitions of one collection, combined into the
//! rows of one partition.
//!
//! A merge moves rows and never changes them. The result holds exactly the
//! rows of its sources:
//!
//! - **Schema.** The union of the source schemas: the first source's columns
//!   in its order, then every column a later source adds. A cell a source
//!   has no column for takes the null its type stores in a memtable (NaN,
//!   the null symbol, 0), the way a column added by schema evolution
//!   backfills older rows.
//! - **Symbols.** One dictionary per symbol column, the union of the
//!   sources' dictionaries, with no cardinality cap: the tag cap bounds one
//!   memtable generation, not a partition.
//! - **Order.** Rows are sorted by time with a stable sort over the sources
//!   in the order given, so rows with equal timestamps keep that order.
//! - **Identity.** `interval_ms` is the sources' common interval. The WAL
//!   LSN is the highest of the sources'. `max_system_ts` is the highest of
//!   the sources'.
//!
//! Sources that cannot combine without changing a row (different time
//! columns, a column whose type differs, different intervals) are refused
//! as [`MergeOutcome::Incompatible`]; the caller leaves them unmerged.

use std::collections::HashMap;

use nodedb_types::timeseries::{PartitionMeta, SymbolDictionary};

use crate::engine::timeseries::columnar_memtable::{
    ColumnData, ColumnType, ColumnarDrainResult, ColumnarSchema, NULL_SYMBOL_ID,
};

/// One source partition, read whole.
pub struct MergeSource {
    pub schema: ColumnarSchema,
    /// One column per schema column, in schema order.
    pub columns: Vec<ColumnData>,
    /// Dictionary of each symbol column that has one, by column index.
    pub symbol_dicts: HashMap<usize, SymbolDictionary>,
    pub meta: PartitionMeta,
}

/// The combined rows, ready for the partition writer.
pub struct MergedPartition {
    pub drain: ColumnarDrainResult,
    pub interval_ms: u64,
    pub last_flushed_wal_lsn: u64,
}

/// What a merge of a group produced.
pub enum MergeOutcome {
    Merged(MergedPartition),
    /// The sources cannot combine without changing a row. The text says why.
    Incompatible(String),
}

/// Combine `sources`, in the order given, into one partition's rows.
///
/// `Err` is corruption: a column whose length is not the partition's row
/// count, a symbol id no dictionary holds, or a symbol dictionary that
/// reached the id space.
pub fn merge_sources(sources: Vec<MergeSource>) -> crate::Result<MergeOutcome> {
    let Some(first) = sources.first() else {
        return Ok(MergeOutcome::Incompatible("no source partitions".into()));
    };
    let interval_ms = first.meta.interval_ms;
    if let Some(other) = sources.iter().find(|s| s.meta.interval_ms != interval_ms) {
        return Ok(MergeOutcome::Incompatible(format!(
            "partition intervals differ: {interval_ms} and {}",
            other.meta.interval_ms
        )));
    }
    let schemas: Vec<&ColumnarSchema> = sources.iter().map(|s| &s.schema).collect();
    let schema = match union_schema(&schemas) {
        Ok(schema) => schema,
        Err(reason) => return Ok(MergeOutcome::Incompatible(reason)),
    };

    let mut columns: Vec<ColumnData> = schema
        .columns
        .iter()
        .map(|(_, ty)| empty_column(*ty))
        .collect();
    let mut symbol_dicts: HashMap<usize, SymbolDictionary> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, (_, ty))| ty.is_dictionary())
        .map(|(i, _)| (i, SymbolDictionary::new()))
        .collect();

    let mut row_count = 0u64;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut max_system_ts = 0i64;
    let mut last_flushed_wal_lsn = 0u64;

    for source in &sources {
        let rows = usize::try_from(source.meta.row_count).map_err(|_| {
            corrupt(format!(
                "a partition row count {} does not fit this platform",
                source.meta.row_count
            ))
        })?;
        if source.columns.len() != source.schema.columns.len() {
            return Err(corrupt(format!(
                "a partition holds {} columns and its schema names {}",
                source.columns.len(),
                source.schema.columns.len()
            )));
        }
        for (j, (name, ty)) in schema.columns.iter().enumerate() {
            let Some(src_idx) = source.schema.columns.iter().position(|(n, _)| n == name) else {
                push_nulls(&mut columns[j], rows);
                continue;
            };
            let data = &source.columns[src_idx];
            if data.len() != rows {
                return Err(corrupt(format!(
                    "column '{name}' holds {} rows and its partition {rows}",
                    data.len()
                )));
            }
            if ty.is_dictionary() {
                let dict = symbol_dicts.get_mut(&j).ok_or_else(|| {
                    corrupt(format!("symbol column '{name}' has no merged dictionary"))
                })?;
                let remapped = remap_symbols(name, data, source.symbol_dicts.get(&src_idx), dict)?;
                append(&mut columns[j], &remapped, name)?;
            } else {
                append(&mut columns[j], data, name)?;
            }
        }
        row_count += source.meta.row_count;
        min_ts = min_ts.min(source.meta.min_ts);
        max_ts = max_ts.max(source.meta.max_ts);
        max_system_ts = max_system_ts.max(source.meta.max_system_ts);
        last_flushed_wal_lsn = last_flushed_wal_lsn.max(source.meta.last_flushed_wal_lsn);
    }

    sort_by_time(&mut columns, schema.timestamp_idx)?;

    Ok(MergeOutcome::Merged(MergedPartition {
        drain: ColumnarDrainResult {
            columns,
            schema,
            symbol_dicts,
            row_count,
            min_ts,
            max_ts,
            max_system_ts,
            series_row_counts: HashMap::new(),
        },
        interval_ms,
        last_flushed_wal_lsn,
    }))
}

/// Why partitions with these schemas do not combine, or `None` when they
/// do. A caller checks this on the schemas alone before it reads columns.
pub fn schema_incompatibility(schemas: &[&ColumnarSchema]) -> Option<String> {
    union_schema(schemas).err()
}

/// The union of the source schemas, or why the sources do not combine.
fn union_schema(schemas: &[&ColumnarSchema]) -> Result<ColumnarSchema, String> {
    let Some((first, rest)) = schemas.split_first() else {
        return Err("no source partitions".into());
    };
    let time_name = time_column(first)?;
    let mut columns = first.columns.clone();
    let mut codecs: Vec<nodedb_codec::ColumnCodec> =
        (0..first.columns.len()).map(|i| first.codec(i)).collect();
    for schema in rest {
        let name = time_column(schema)?;
        if name != time_name {
            return Err(format!("time columns differ: '{time_name}' and '{name}'"));
        }
        for (i, (col, ty)) in schema.columns.iter().enumerate() {
            match columns.iter().find(|(n, _)| n == col) {
                Some((_, held)) if held != ty => {
                    return Err(format!(
                        "column '{col}' is {held:?} in one partition and {ty:?} in another"
                    ));
                }
                Some(_) => {}
                None => {
                    columns.push((col.clone(), *ty));
                    codecs.push(schema.codec(i));
                }
            }
        }
    }
    Ok(ColumnarSchema {
        columns,
        timestamp_idx: first.timestamp_idx,
        codecs,
    })
}

/// The name of `schema`'s time column.
fn time_column(schema: &ColumnarSchema) -> Result<&str, String> {
    match schema.columns.get(schema.timestamp_idx) {
        Some((name, ty)) if ty.is_time() => Ok(name.as_str()),
        _ => Err("a partition schema names no time column".into()),
    }
}

fn empty_column(ty: ColumnType) -> ColumnData {
    match ty {
        ColumnType::Timestamp(_) => ColumnData::Timestamp(Vec::new()),
        ColumnType::Float64 => ColumnData::Float64(Vec::new()),
        ColumnType::Int64 => ColumnData::Int64(Vec::new()),
        ColumnType::Symbol | ColumnType::Text => ColumnData::Symbol(Vec::new()),
    }
}

/// Append `rows` nulls, as a memtable backfills a column it gains.
fn push_nulls(column: &mut ColumnData, rows: usize) {
    match column {
        ColumnData::Timestamp(v) | ColumnData::Int64(v) => v.extend(std::iter::repeat_n(0, rows)),
        ColumnData::Float64(v) => v.extend(std::iter::repeat_n(f64::NAN, rows)),
        ColumnData::Symbol(v) => v.extend(std::iter::repeat_n(NULL_SYMBOL_ID, rows)),
        ColumnData::DictEncoded { ids, valid, .. } => {
            ids.extend(std::iter::repeat_n(NULL_SYMBOL_ID, rows));
            valid.extend(std::iter::repeat_n(false, rows));
        }
    }
}

/// Append `data` to `column`. The two must hold the same type.
fn append(column: &mut ColumnData, data: &ColumnData, name: &str) -> crate::Result<()> {
    match (column, data) {
        (ColumnData::Timestamp(a), ColumnData::Timestamp(b))
        | (ColumnData::Int64(a), ColumnData::Int64(b)) => a.extend_from_slice(b),
        (ColumnData::Float64(a), ColumnData::Float64(b)) => a.extend_from_slice(b),
        (ColumnData::Symbol(a), ColumnData::Symbol(b)) => a.extend_from_slice(b),
        _ => {
            return Err(corrupt(format!(
                "column '{name}' read back in a form its schema type does not store"
            )));
        }
    }
    Ok(())
}

/// The ids of a source symbol column, renumbered into `merged`.
fn remap_symbols(
    name: &str,
    data: &ColumnData,
    source_dict: Option<&SymbolDictionary>,
    merged: &mut SymbolDictionary,
) -> crate::Result<ColumnData> {
    let ColumnData::Symbol(ids) = data else {
        return Err(corrupt(format!(
            "symbol column '{name}' read back in a form a symbol column does not store"
        )));
    };
    // The id space stops below the null id, so no symbol takes it.
    let remap = match source_dict {
        Some(dict) => merged.merge(dict, NULL_SYMBOL_ID),
        None => Vec::new(),
    };
    if remap.contains(&NULL_SYMBOL_ID) {
        return Err(crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("merge: symbol column '{name}' reached the symbol id space"),
        });
    }
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        if id == NULL_SYMBOL_ID {
            out.push(NULL_SYMBOL_ID);
            continue;
        }
        let mapped = remap.get(id as usize).copied().ok_or_else(|| {
            corrupt(format!(
                "symbol column '{name}' holds id {id}, which its dictionary does not"
            ))
        })?;
        out.push(mapped);
    }
    Ok(ColumnData::Symbol(out))
}

/// Sort every column by the time column, stably.
fn sort_by_time(columns: &mut [ColumnData], time_idx: usize) -> crate::Result<()> {
    let order: Vec<usize> = {
        let Some(ColumnData::Timestamp(times)) = columns.get(time_idx) else {
            return Err(corrupt("the time column read back as no time column"));
        };
        let mut order: Vec<usize> = (0..times.len()).collect();
        order.sort_by_key(|&i| times[i]);
        order
    };
    for column in columns.iter_mut() {
        permute(column, &order);
    }
    Ok(())
}

fn permute(column: &mut ColumnData, order: &[usize]) {
    fn apply<T: Copy>(values: &mut Vec<T>, order: &[usize]) {
        *values = order.iter().map(|&i| values[i]).collect();
    }
    match column {
        ColumnData::Timestamp(v) | ColumnData::Int64(v) => apply(v, order),
        ColumnData::Float64(v) => apply(v, order),
        ColumnData::Symbol(v) => apply(v, order),
        ColumnData::DictEncoded { ids, valid, .. } => {
            apply(ids, order);
            apply(valid, order);
        }
    }
}

fn corrupt(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::SegmentCorrupted {
        detail: format!("timeseries merge: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::PartitionState;

    use super::*;
    use crate::engine::timeseries::columnar_memtable::TimeKind;

    const MILLIS: ColumnType = ColumnType::Timestamp(TimeKind::Millis);

    fn meta(min_ts: i64, max_ts: i64, rows: u64, lsn: u64) -> PartitionMeta {
        PartitionMeta {
            min_ts,
            max_ts,
            row_count: rows,
            size_bytes: 0,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: 3_600_000,
            last_flushed_wal_lsn: lsn,
            column_stats: HashMap::new(),
            max_system_ts: 0,
            ended_at: None,
        }
    }

    fn source(columns: Vec<(&str, ColumnType, ColumnData)>, meta: PartitionMeta) -> MergeSource {
        let (schema_cols, data): (Vec<(String, ColumnType)>, Vec<ColumnData>) = columns
            .into_iter()
            .map(|(n, t, d)| ((n.to_string(), t), d))
            .unzip();
        MergeSource {
            schema: ColumnarSchema {
                columns: schema_cols,
                timestamp_idx: 0,
                codecs: Vec::new(),
            },
            columns: data,
            symbol_dicts: HashMap::new(),
            meta,
        }
    }

    fn merged(outcome: MergeOutcome) -> MergedPartition {
        match outcome {
            MergeOutcome::Merged(m) => m,
            MergeOutcome::Incompatible(reason) => panic!("refused: {reason}"),
        }
    }

    /// A column only a later source has keeps its values. The earlier
    /// source's rows read it as null.
    #[test]
    fn the_schema_is_the_union_of_the_sources() {
        let a = source(
            vec![
                ("ts", MILLIS, ColumnData::Timestamp(vec![1, 3])),
                (
                    "v",
                    ColumnType::Float64,
                    ColumnData::Float64(vec![1.0, 3.0]),
                ),
            ],
            meta(1, 3, 2, 10),
        );
        let b = source(
            vec![
                ("ts", MILLIS, ColumnData::Timestamp(vec![2])),
                ("v", ColumnType::Float64, ColumnData::Float64(vec![2.0])),
                ("n", ColumnType::Int64, ColumnData::Int64(vec![7])),
            ],
            meta(2, 2, 1, 20),
        );
        let m = merged(merge_sources(vec![a, b]).expect("merges"));
        let names: Vec<&str> = m
            .drain
            .schema
            .columns
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();
        assert_eq!(names, vec!["ts", "v", "n"]);
        assert!(matches!(&m.drain.columns[0], ColumnData::Timestamp(v) if v == &[1, 2, 3]));
        assert!(matches!(&m.drain.columns[2], ColumnData::Int64(v) if v == &[0, 7, 0]));
        assert_eq!(m.drain.row_count, 3);
        assert_eq!((m.drain.min_ts, m.drain.max_ts), (1, 3));
        assert_eq!(m.last_flushed_wal_lsn, 20);
        assert_eq!(m.interval_ms, 3_600_000);
    }

    /// Rows with equal timestamps keep the order of their sources.
    #[test]
    fn equal_timestamps_keep_source_order() {
        let a = source(
            vec![
                ("ts", MILLIS, ColumnData::Timestamp(vec![5])),
                ("v", ColumnType::Float64, ColumnData::Float64(vec![1.0])),
            ],
            meta(5, 5, 1, 1),
        );
        let b = source(
            vec![
                ("ts", MILLIS, ColumnData::Timestamp(vec![5])),
                ("v", ColumnType::Float64, ColumnData::Float64(vec![2.0])),
            ],
            meta(5, 5, 1, 2),
        );
        let m = merged(merge_sources(vec![a, b]).expect("merges"));
        assert!(matches!(&m.drain.columns[1], ColumnData::Float64(v) if v == &[1.0, 2.0]));
    }

    /// A merged dictionary larger than one memtable generation's tag cap
    /// keeps every value.
    #[test]
    fn symbols_merge_without_a_cardinality_cap() {
        const PER_SOURCE: u32 = 60_000;
        let make = |offset: u32, lsn: u64| {
            let mut dict = SymbolDictionary::new();
            let mut ids = Vec::new();
            for i in 0..PER_SOURCE {
                ids.push(
                    dict.resolve(&format!("host-{}", offset + i), u32::MAX)
                        .expect("resolves"),
                );
            }
            let mut s = source(
                vec![
                    (
                        "ts",
                        MILLIS,
                        ColumnData::Timestamp((0..i64::from(PER_SOURCE)).collect()),
                    ),
                    ("host", ColumnType::Symbol, ColumnData::Symbol(ids)),
                ],
                meta(0, i64::from(PER_SOURCE) - 1, u64::from(PER_SOURCE), lsn),
            );
            s.symbol_dicts.insert(1, dict);
            s
        };
        let m = merged(merge_sources(vec![make(0, 1), make(PER_SOURCE, 2)]).expect("merges"));
        let dict = m.drain.symbol_dicts.get(&1).expect("dictionary");
        assert_eq!(dict.len(), 2 * PER_SOURCE as usize);
        let ColumnData::Symbol(ids) = &m.drain.columns[1] else {
            panic!("symbol column");
        };
        assert!(ids.iter().all(|&id| id != NULL_SYMBOL_ID));
    }

    /// A null symbol stays null, and a symbol keeps its string.
    #[test]
    fn symbols_keep_their_strings_and_nulls() {
        let make = |host: &str, ts: i64, lsn: u64| {
            let mut dict = SymbolDictionary::new();
            let id = dict.resolve(host, u32::MAX).expect("resolves");
            let mut s = source(
                vec![
                    ("ts", MILLIS, ColumnData::Timestamp(vec![ts, ts + 1])),
                    (
                        "host",
                        ColumnType::Symbol,
                        ColumnData::Symbol(vec![id, NULL_SYMBOL_ID]),
                    ),
                ],
                meta(ts, ts + 1, 2, lsn),
            );
            s.symbol_dicts.insert(1, dict);
            s
        };
        let m = merged(merge_sources(vec![make("a", 0, 1), make("b", 10, 2)]).expect("merges"));
        let dict = m.drain.symbol_dicts.get(&1).expect("dictionary");
        let ColumnData::Symbol(ids) = &m.drain.columns[1] else {
            panic!("symbol column");
        };
        let strings: Vec<Option<&str>> = ids
            .iter()
            .map(|&id| (id != NULL_SYMBOL_ID).then(|| dict.get(id)).flatten())
            .collect();
        assert_eq!(strings, vec![Some("a"), None, Some("b"), None]);
    }

    /// Sources with different intervals, time columns or column types are
    /// left unmerged.
    #[test]
    fn incompatible_sources_are_refused() {
        let base = || {
            source(
                vec![
                    ("ts", MILLIS, ColumnData::Timestamp(vec![1])),
                    ("v", ColumnType::Float64, ColumnData::Float64(vec![1.0])),
                ],
                meta(1, 1, 1, 1),
            )
        };
        let mut other_interval = base();
        other_interval.meta.interval_ms = 1;
        assert!(matches!(
            merge_sources(vec![base(), other_interval]),
            Ok(MergeOutcome::Incompatible(_))
        ));
        let other_type = source(
            vec![
                ("ts", MILLIS, ColumnData::Timestamp(vec![1])),
                ("v", ColumnType::Int64, ColumnData::Int64(vec![1])),
            ],
            meta(1, 1, 1, 1),
        );
        assert!(matches!(
            merge_sources(vec![base(), other_type]),
            Ok(MergeOutcome::Incompatible(_))
        ));
        let other_time = source(
            vec![("t2", MILLIS, ColumnData::Timestamp(vec![1]))],
            meta(1, 1, 1, 1),
        );
        assert!(matches!(
            merge_sources(vec![base(), other_time]),
            Ok(MergeOutcome::Incompatible(_))
        ));
    }

    /// A column whose length is not its partition's row count is corruption.
    #[test]
    fn a_short_column_is_corruption() {
        let bad = source(
            vec![("ts", MILLIS, ColumnData::Timestamp(vec![1]))],
            meta(1, 1, 2, 1),
        );
        assert!(matches!(
            merge_sources(vec![bad]),
            Err(crate::Error::SegmentCorrupted { .. })
        ));
    }
}
