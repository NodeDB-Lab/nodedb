//! Columnar memtable for timeseries ingest.
//!
//! Replaces the per-series `HashMap<SeriesId, SeriesBuffer>` with per-column
//! vectors for SIMD-friendly aggregation and efficient columnar flush.
//!
//! Layout: timestamps, values, and tag symbol IDs are stored in separate,
//! contiguous vectors. A `row_index` maps each row to its series for
//! per-series queries.
//!
//! NOT thread-safe — lives on a single Data Plane core (!Send by design).

use std::collections::HashMap;

use nodedb_types::timeseries::{IngestResult, MetricSample, SeriesId, SymbolDictionary};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Column data type in a columnar memtable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// Designated timestamp column (i64 millis).
    Timestamp,
    /// Floating-point metric value.
    Float64,
    /// Integer metric value.
    Int64,
    /// Tag column — stored as u32 symbol IDs.
    Symbol,
}

/// Schema for a columnar memtable (column names + types, in order).
#[derive(Debug, Clone)]
pub struct ColumnarSchema {
    pub columns: Vec<(String, ColumnType)>,
    /// Index of the designated timestamp column.
    pub timestamp_idx: usize,
    /// Per-column codec selection. When empty or shorter than `columns`,
    /// missing entries default to `Auto`.
    pub codecs: Vec<nodedb_codec::ColumnCodec>,
}

impl ColumnarSchema {
    /// Create a schema with a timestamp + one f64 value column (simplest case).
    pub fn metric_default() -> Self {
        Self {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![
                nodedb_codec::ColumnCodec::Auto,
                nodedb_codec::ColumnCodec::Auto,
            ],
        }
    }

    /// Get the codec for column at index `i`. Returns `Auto` if not specified.
    pub fn codec(&self, i: usize) -> nodedb_codec::ColumnCodec {
        self.codecs
            .get(i)
            .copied()
            .unwrap_or(nodedb_codec::ColumnCodec::Auto)
    }

    /// Index of the reserved `_ts_system` column, or `None` for non-bitemporal.
    pub fn ts_system_idx(&self) -> Option<usize> {
        self.columns.iter().position(|(n, _)| n == "_ts_system")
    }
}

// ---------------------------------------------------------------------------
// Column storage
// ---------------------------------------------------------------------------

/// A single column of data in the memtable.
#[derive(Debug)]
pub enum ColumnData {
    Timestamp(Vec<i64>),
    Float64(Vec<f64>),
    Int64(Vec<i64>),
    Symbol(Vec<u32>),
    /// Dictionary-encoded string column for low-cardinality tags.
    ///
    /// Predicates operate on compact integer IDs rather than string bytes —
    /// an `O(dict_size * string_len + N)` win over `O(N * string_len)`.
    DictEncoded {
        /// Row-level symbol IDs (index into `dictionary`).
        ids: Vec<u32>,
        /// ID → string value.
        dictionary: Vec<String>,
        /// Reverse lookup: string → ID.
        reverse: std::collections::HashMap<String, u32>,
        /// Validity bitmap: false = null row.
        valid: Vec<bool>,
    },
}

impl ColumnData {
    fn new(ty: ColumnType) -> Self {
        match ty {
            ColumnType::Timestamp => Self::Timestamp(Vec::with_capacity(4096)),
            ColumnType::Float64 => Self::Float64(Vec::with_capacity(4096)),
            ColumnType::Int64 => Self::Int64(Vec::with_capacity(4096)),
            ColumnType::Symbol => Self::Symbol(Vec::with_capacity(4096)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Timestamp(v) => v.len(),
            Self::Float64(v) => v.len(),
            Self::Int64(v) => v.len(),
            Self::Symbol(v) => v.len(),
            Self::DictEncoded { ids, .. } => ids.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate memory usage in bytes.
    fn memory_bytes(&self) -> usize {
        match self {
            Self::Timestamp(v) => v.capacity() * 8,
            Self::Float64(v) => v.capacity() * 8,
            Self::Int64(v) => v.capacity() * 8,
            Self::Symbol(v) => v.capacity() * 4,
            Self::DictEncoded {
                ids,
                dictionary,
                reverse,
                valid,
            } => {
                ids.capacity() * 4
                    + dictionary.iter().map(|s| s.len() + 24).sum::<usize>()
                    + reverse.len() * 56 // rough HashMap entry cost
                    + valid.capacity()
            }
        }
    }

    /// Deep clone the column data.
    pub fn clone_data(&self) -> Self {
        match self {
            Self::Timestamp(v) => Self::Timestamp(v.clone()),
            Self::Float64(v) => Self::Float64(v.clone()),
            Self::Int64(v) => Self::Int64(v.clone()),
            Self::Symbol(v) => Self::Symbol(v.clone()),
            Self::DictEncoded {
                ids,
                dictionary,
                reverse,
                valid,
            } => Self::DictEncoded {
                ids: ids.clone(),
                dictionary: dictionary.clone(),
                reverse: reverse.clone(),
                valid: valid.clone(),
            },
        }
    }

    /// Get timestamp column as slice. Panics if wrong type.
    pub fn as_timestamps(&self) -> &[i64] {
        match self {
            Self::Timestamp(v) => v,
            _ => panic!("column is not Timestamp"),
        }
    }

    /// Get f64 column as slice. Panics if wrong type.
    pub fn as_f64(&self) -> &[f64] {
        match self {
            Self::Float64(v) => v,
            _ => panic!("column is not Float64"),
        }
    }

    /// Get i64 column as slice. Panics if wrong type.
    pub fn as_i64(&self) -> &[i64] {
        match self {
            Self::Int64(v) => v,
            _ => panic!("column is not Int64"),
        }
    }

    /// Get symbol column as slice. Panics if wrong type.
    pub fn as_symbols(&self) -> &[u32] {
        match self {
            Self::Symbol(v) => v,
            _ => panic!("column is not Symbol"),
        }
    }
}

// ---------------------------------------------------------------------------
// Columnar memtable
// ---------------------------------------------------------------------------

/// Configuration for the columnar memtable.
#[derive(Debug, Clone)]
pub struct ColumnarMemtableConfig {
    /// Maximum memory usage before flush is triggered (bytes).
    pub max_memory_bytes: usize,
    /// Hard memory ceiling — ingest is rejected above this.
    pub hard_memory_limit: usize,
    /// Maximum tag cardinality per symbol column.
    pub max_tag_cardinality: u32,
}

impl Default for ColumnarMemtableConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: super::memtable::DEFAULT_MEMTABLE_BUDGET_BYTES,
            hard_memory_limit: 80 * 1024 * 1024,
            max_tag_cardinality: 100_000,
        }
    }
}

/// Columnar memtable: per-column vectors instead of per-series hash maps.
///
/// Each row is a flat tuple of (timestamp, value, tag1, tag2, ...).
/// Series identity is derived from the tag columns at query time.
/// This layout is SIMD-friendly: aggregation functions operate on
/// contiguous `&[f64]` or `&[i64]` slices.
pub struct ColumnarMemtable {
    schema: ColumnarSchema,
    columns: Vec<ColumnData>,
    /// Per-series row count for quick cardinality checks.
    series_row_counts: HashMap<SeriesId, u64>,
    /// Per-tag-column symbol dictionary.
    symbol_dicts: HashMap<usize, SymbolDictionary>,
    row_count: u64,
    memory_bytes: usize,
    config: ColumnarMemtableConfig,
    min_ts: i64,
    max_ts: i64,
}

impl ColumnarMemtable {
    /// Create a new columnar memtable with the given schema.
    pub fn new(schema: ColumnarSchema, config: ColumnarMemtableConfig) -> Self {
        let columns: Vec<ColumnData> = schema
            .columns
            .iter()
            .map(|(_, ty)| ColumnData::new(*ty))
            .collect();

        // Initialize symbol dicts for tag columns.
        let mut symbol_dicts = HashMap::new();
        for (i, (_, ty)) in schema.columns.iter().enumerate() {
            if *ty == ColumnType::Symbol {
                symbol_dicts.insert(i, SymbolDictionary::new());
            }
        }

        Self {
            schema,
            columns,
            series_row_counts: HashMap::new(),
            symbol_dicts,
            row_count: 0,
            memory_bytes: 0,
            config,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
        }
    }

    /// Create a simple metrics memtable (timestamp + f64 value, no tags).
    pub fn new_metric(config: ColumnarMemtableConfig) -> Self {
        Self::new(ColumnarSchema::metric_default(), config)
    }

    /// Ingest a metric sample into the default (timestamp, value) layout.
    ///
    /// For the simple 2-column schema. For multi-column schemas with tags,
    /// use `ingest_row()` instead.
    pub fn ingest_metric(&mut self, series_id: SeriesId, sample: MetricSample) -> IngestResult {
        if self.memory_bytes >= self.config.hard_memory_limit {
            return IngestResult::Rejected;
        }

        // Push to timestamp column.
        if let ColumnData::Timestamp(ref mut v) = self.columns[self.schema.timestamp_idx] {
            v.push(sample.timestamp_ms);
        }

        // Push to value column (assume index 1 for default schema).
        if self.columns.len() > 1
            && let ColumnData::Float64(ref mut v) = self.columns[1]
        {
            v.push(sample.value);
        }

        self.update_stats(series_id, sample.timestamp_ms, 16);
        self.check_flush_state()
    }

    /// Ingest a row with explicit column values.
    ///
    /// `values` must match the schema length. Tag string values are resolved
    /// to symbol IDs via the per-column dictionary.
    pub fn ingest_row(
        &mut self,
        series_id: SeriesId,
        values: &[ColumnValue<'_>],
    ) -> crate::Result<IngestResult> {
        if self.memory_bytes >= self.config.hard_memory_limit {
            return Ok(IngestResult::Rejected);
        }

        let col_types: Vec<(String, ColumnType)> = self.schema.columns.clone();

        if values.len() != col_types.len() {
            return Err(crate::Error::BadRequest {
                detail: format!("expected {} columns, got {}", col_types.len(), values.len()),
            });
        }

        let mut ts = 0i64;
        let mut row_bytes = 0usize;
        let max_card = self.config.max_tag_cardinality;

        for (i, (val, (col_name, col_type))) in values.iter().zip(col_types.iter()).enumerate() {
            match (val, col_type) {
                (ColumnValue::Timestamp(t), ColumnType::Timestamp) => {
                    if let ColumnData::Timestamp(ref mut v) = self.columns[i] {
                        v.push(*t);
                    }
                    ts = *t;
                    row_bytes += 8;
                }
                (ColumnValue::Float64(f), ColumnType::Float64) => {
                    if let ColumnData::Float64(ref mut v) = self.columns[i] {
                        v.push(*f);
                    }
                    row_bytes += 8;
                }
                (ColumnValue::Int64(n), ColumnType::Int64) => {
                    if let ColumnData::Int64(ref mut v) = self.columns[i] {
                        v.push(*n);
                    }
                    row_bytes += 8;
                }
                (ColumnValue::Symbol(s), ColumnType::Symbol) => {
                    let dict =
                        self.symbol_dicts
                            .get_mut(&i)
                            .ok_or_else(|| crate::Error::BadRequest {
                                detail: format!(
                                    "internal error: symbol dict missing for column {i}"
                                ),
                            })?;
                    match dict.resolve(s, max_card) {
                        Some(sym_id) => {
                            if let ColumnData::Symbol(ref mut v) = self.columns[i] {
                                v.push(sym_id);
                            }
                        }
                        None => {
                            self.rollback_partial_row(i);
                            return Err(crate::Error::BadRequest {
                                detail: format!(
                                    "tag cardinality limit ({max_card}) exceeded for column '{col_name}'"
                                ),
                            });
                        }
                    }
                    row_bytes += 4;
                }
                _ => {
                    self.rollback_partial_row(i);
                    return Err(crate::Error::BadRequest {
                        detail: format!("type mismatch at column {i}: expected {col_type:?}"),
                    });
                }
            }
        }

        self.update_stats(series_id, ts, row_bytes);
        Ok(self.check_flush_state())
    }

    /// Roll back a partially written row (called on error during `ingest_row`).
    fn rollback_partial_row(&mut self, columns_written: usize) {
        for col in self.columns.iter_mut().take(columns_written) {
            match col {
                ColumnData::Timestamp(v) => {
                    v.pop();
                }
                ColumnData::Float64(v) => {
                    v.pop();
                }
                ColumnData::Int64(v) => {
                    v.pop();
                }
                ColumnData::Symbol(v) => {
                    v.pop();
                }
                ColumnData::DictEncoded { ids, valid, .. } => {
                    ids.pop();
                    valid.pop();
                }
            }
        }
    }

    fn update_stats(&mut self, series_id: SeriesId, ts: i64, row_bytes: usize) {
        *self.series_row_counts.entry(series_id).or_insert(0) += 1;
        self.row_count += 1;
        self.memory_bytes += row_bytes;
        if ts < self.min_ts {
            self.min_ts = ts;
        }
        if ts > self.max_ts {
            self.max_ts = ts;
        }
    }

    fn check_flush_state(&self) -> IngestResult {
        if self.memory_bytes >= self.config.max_memory_bytes {
            IngestResult::FlushNeeded
        } else {
            IngestResult::Ok
        }
    }

    /// Drain all data from the memtable, resetting it for reuse.
    ///
    /// Returns the column data, schema, symbol dicts, and stats.
    pub fn drain(&mut self) -> ColumnarDrainResult {
        let mut drained_columns = Vec::with_capacity(self.columns.len());
        for col in &mut self.columns {
            // DictEncoded columns are drained by swapping in a fresh Symbol
            // placeholder. The flusher converts Symbol → DictEncoded during
            // segment encoding once it has enough cardinality data.
            let col_type = match col {
                ColumnData::Timestamp(_) => ColumnType::Timestamp,
                ColumnData::Float64(_) => ColumnType::Float64,
                ColumnData::Int64(_) => ColumnType::Int64,
                ColumnData::Symbol(_) => ColumnType::Symbol,
                ColumnData::DictEncoded { .. } => ColumnType::Symbol,
            };
            let mut empty = ColumnData::new(col_type);
            std::mem::swap(col, &mut empty);
            drained_columns.push(empty);
        }

        let drained_dicts = std::mem::take(&mut self.symbol_dicts);
        // Reinitialize symbol dicts.
        for (i, (_, ty)) in self.schema.columns.iter().enumerate() {
            if *ty == ColumnType::Symbol {
                self.symbol_dicts.insert(i, SymbolDictionary::new());
            }
        }

        // Scan `_ts_system` column (if present) for retention's system-time axis.
        // For non-bitemporal partitions this is 0 and retention falls through
        // to the existing `max_ts` (event-time) path.
        let max_system_ts = self
            .schema
            .ts_system_idx()
            .and_then(|idx| drained_columns.get(idx))
            .map(|col| match col {
                ColumnData::Timestamp(v) | ColumnData::Int64(v) => {
                    v.iter().copied().max().unwrap_or(0)
                }
                _ => 0,
            })
            .unwrap_or(0);

        let result = ColumnarDrainResult {
            columns: drained_columns,
            schema: self.schema.clone(),
            symbol_dicts: drained_dicts,
            row_count: self.row_count,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_system_ts,
            series_row_counts: std::mem::take(&mut self.series_row_counts),
        };

        self.row_count = 0;
        self.memory_bytes = 0;
        self.min_ts = i64::MAX;
        self.max_ts = i64::MIN;

        result
    }

    // -- Accessors --

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Approximate memory usage. Uses incremental tracking with periodic
    /// recomputation from column capacities for accuracy.
    pub fn memory_bytes(&self) -> usize {
        // Fast path: return incremental estimate.
        // For exact accounting, sum actual column capacities.
        let col_bytes: usize = self.columns.iter().map(|c| c.memory_bytes()).sum();
        let dict_bytes: usize = self.symbol_dicts.len() * 256; // rough estimate
        self.memory_bytes.max(col_bytes + dict_bytes)
    }

    pub fn min_ts(&self) -> i64 {
        self.min_ts
    }

    pub fn max_ts(&self) -> i64 {
        self.max_ts
    }

    pub fn series_count(&self) -> usize {
        self.series_row_counts.len()
    }

    pub fn schema(&self) -> &ColumnarSchema {
        &self.schema
    }

    /// Export memtable data for snapshot.
    ///
    /// Returns `(column_name, serialized_column_data)` pairs.
    pub fn export_snapshot(&self) -> Vec<(String, Vec<u8>)> {
        let mut result = Vec::with_capacity(self.schema.columns.len());
        for (i, (name, _ty)) in self.schema.columns.iter().enumerate() {
            if i < self.columns.len() {
                let bytes = match &self.columns[i] {
                    ColumnData::Timestamp(v) => zerompk::to_msgpack_vec(v).unwrap_or_default(),
                    ColumnData::Float64(v) => zerompk::to_msgpack_vec(v).unwrap_or_default(),
                    ColumnData::Int64(v) => zerompk::to_msgpack_vec(v).unwrap_or_default(),
                    ColumnData::Symbol(v) => zerompk::to_msgpack_vec(v).unwrap_or_default(),
                    // Snapshot the raw IDs; the dictionary is in symbol_dicts.
                    ColumnData::DictEncoded { ids, .. } => {
                        zerompk::to_msgpack_vec(ids).unwrap_or_default()
                    }
                };
                result.push((name.clone(), bytes));
            }
        }
        result
    }

    pub fn column(&self, idx: usize) -> &ColumnData {
        &self.columns[idx]
    }

    pub fn symbol_dict(&self, col_idx: usize) -> Option<&SymbolDictionary> {
        self.symbol_dicts.get(&col_idx)
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Add a new column to the memtable schema, backfilling existing rows
    /// with NULL-equivalent values.
    ///
    /// Used for ILP schema evolution: when a new field appears in a later
    /// batch, the column is added and old rows get NaN/0/null-symbol.
    pub fn add_column(&mut self, name: String, col_type: ColumnType) {
        // Don't add duplicates.
        if self.schema.columns.iter().any(|(n, _)| n == &name) {
            return;
        }
        let existing_rows = self.row_count as usize;
        let col = match col_type {
            ColumnType::Float64 => ColumnData::Float64(vec![f64::NAN; existing_rows]),
            ColumnType::Int64 => ColumnData::Int64(vec![0; existing_rows]),
            ColumnType::Symbol => ColumnData::Symbol(vec![u32::MAX; existing_rows]),
            ColumnType::Timestamp => return, // never add a second timestamp
        };
        let idx = self.columns.len();
        self.columns.push(col);
        self.schema.columns.push((name, col_type));
        self.schema.codecs.push(nodedb_codec::ColumnCodec::Auto);
        if col_type == ColumnType::Symbol {
            self.symbol_dicts.insert(idx, SymbolDictionary::new());
        }
    }
}

/// Value types for `ingest_row()`.
#[derive(Debug, Clone)]
pub enum ColumnValue<'a> {
    Timestamp(i64),
    Float64(f64),
    Int64(i64),
    Symbol(&'a str),
}

/// Result of draining the columnar memtable.
pub struct ColumnarDrainResult {
    pub columns: Vec<ColumnData>,
    pub schema: ColumnarSchema,
    pub symbol_dicts: HashMap<usize, SymbolDictionary>,
    pub row_count: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    /// Maximum `_ts_system` value across rows (bitemporal only; 0 otherwise).
    /// Populated on drain by scanning the `_ts_system` column so retention
    /// can evaluate system-time staleness without re-reading the segment.
    pub max_system_ts: i64,
    pub series_row_counts: HashMap<SeriesId, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 1024 * 1024,
            hard_memory_limit: 2 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    #[test]
    fn empty_memtable() {
        let mt = ColumnarMemtable::new_metric(default_config());
        assert_eq!(mt.row_count(), 0);
        assert!(mt.is_empty());
        assert_eq!(mt.series_count(), 0);
    }

    #[test]
    fn ingest_simple_metric() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        let result = mt.ingest_metric(
            1,
            MetricSample {
                timestamp_ms: 1000,
                value: 42.5,
            },
        );
        assert_eq!(result, IngestResult::Ok);
        assert_eq!(mt.row_count(), 1);
        assert_eq!(mt.min_ts(), 1000);
        assert_eq!(mt.max_ts(), 1000);

        let ts_col = mt.column(0).as_timestamps();
        assert_eq!(ts_col, &[1000]);
        let val_col = mt.column(1).as_f64();
        assert!((val_col[0] - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn ingest_multiple_metrics() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        for i in 0..100 {
            mt.ingest_metric(
                i % 10,
                MetricSample {
                    timestamp_ms: 1000 + i as i64,
                    value: i as f64,
                },
            );
        }
        assert_eq!(mt.row_count(), 100);
        assert_eq!(mt.series_count(), 10);
        assert_eq!(mt.min_ts(), 1000);
        assert_eq!(mt.max_ts(), 1099);
    }

    #[test]
    fn ingest_row_with_tags() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
                ("dc".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 4],
        };
        let mut mt = ColumnarMemtable::new(schema, default_config());

        let result = mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(5000),
                ColumnValue::Float64(99.9),
                ColumnValue::Symbol("prod-1"),
                ColumnValue::Symbol("us-east"),
            ],
        );
        assert!(result.is_ok());
        assert_eq!(mt.row_count(), 1);

        // Verify symbol dictionaries were populated.
        let host_dict = mt.symbol_dict(2).unwrap();
        assert_eq!(host_dict.len(), 1);
        assert_eq!(host_dict.get(0), Some("prod-1"));

        let dc_dict = mt.symbol_dict(3).unwrap();
        assert_eq!(dc_dict.get(0), Some("us-east"));
    }

    #[test]
    fn tag_cardinality_breaker() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
                ("tag".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let config = ColumnarMemtableConfig {
            max_tag_cardinality: 5,
            ..default_config()
        };
        let mut mt = ColumnarMemtable::new(schema, config);

        // First 5 unique tags work.
        for i in 0..5 {
            let tag = format!("val-{i}");
            let r = mt.ingest_row(
                i as u64,
                &[
                    ColumnValue::Timestamp(1000 + i as i64),
                    ColumnValue::Float64(1.0),
                    ColumnValue::Symbol(&tag),
                ],
            );
            assert!(r.is_ok());
        }
        assert_eq!(mt.row_count(), 5);

        // 6th unique tag is rejected.
        let r = mt.ingest_row(
            99,
            &[
                ColumnValue::Timestamp(2000),
                ColumnValue::Float64(1.0),
                ColumnValue::Symbol("one-too-many"),
            ],
        );
        assert!(r.is_err());
        // Row count didn't increase (rolled back).
        assert_eq!(mt.row_count(), 5);
    }

    #[test]
    fn drain_returns_data_and_resets() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        for i in 0..50 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1000 + i,
                    value: i as f64,
                },
            );
        }
        assert_eq!(mt.row_count(), 50);

        let result = mt.drain();
        assert_eq!(result.row_count, 50);
        assert_eq!(result.min_ts, 1000);
        assert_eq!(result.max_ts, 1049);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].len(), 50);
        assert_eq!(result.columns[1].len(), 50);

        // Memtable is reset.
        assert_eq!(mt.row_count(), 0);
        assert!(mt.is_empty());
    }

    #[test]
    fn hard_limit_rejection() {
        let config = ColumnarMemtableConfig {
            max_memory_bytes: 100,
            hard_memory_limit: 200,
            max_tag_cardinality: 1000,
        };
        let mut mt = ColumnarMemtable::new_metric(config);

        // Fill past hard limit.
        let mut rejected = false;
        for i in 0..1000 {
            let r = mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: i,
                    value: 1.0,
                },
            );
            if r == IngestResult::Rejected {
                rejected = true;
                break;
            }
        }
        assert!(rejected);
    }

    #[test]
    fn flush_needed_signal() {
        let config = ColumnarMemtableConfig {
            max_memory_bytes: 100,
            hard_memory_limit: 200,
            max_tag_cardinality: 1000,
        };
        let mut mt = ColumnarMemtable::new_metric(config);

        let mut flush_signaled = false;
        for i in 0..100 {
            let r = mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: i,
                    value: 1.0,
                },
            );
            if r == IngestResult::FlushNeeded {
                flush_signaled = true;
                break;
            }
        }
        assert!(flush_signaled);
    }

    #[test]
    fn type_mismatch_rejected() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        let mut mt = ColumnarMemtable::new(schema, default_config());

        let r = mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(1000),
                ColumnValue::Int64(42), // Wrong: schema says Float64
            ],
        );
        assert!(r.is_err());
        assert_eq!(mt.row_count(), 0); // Rolled back.
    }
}
