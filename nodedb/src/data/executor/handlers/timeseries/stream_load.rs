// SPDX-License-Identifier: BUSL-1.1

//! The load a streamed committed record writes into each timeseries
//! collection, measured as the stream validate pages it in, for the one
//! flush before the install's timeseries batches.
//!
//! A whole-record install measures the record whole (`ts_record_loads`)
//! before its first row lands, and flushes each memtable that cannot take
//! it (`group_flush`). A streamed record is never held whole, so the
//! validate folds each op into a bounded load: the bytes each collection
//! receives, and the tag values the record adds to each symbol column. A
//! column keeps at most `max_tag_cardinality + 1` new values: past that the
//! record cannot fit any generation, so more values change no verdict.
//!
//! The tag dictionaries are read as the validate runs. An install that
//! gives the core back between its batches holds the install fence from
//! before its validate, so no ingest into the record's collections and no
//! idle flush runs until it ends. No arm before the timeseries arm writes a
//! timeseries memtable, so the flush reads the same dictionaries.

use std::collections::{HashMap, HashSet};

use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_memtable::ColumnType;
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::resolved_ingest::{RESOLVED_INGEST_FORMAT, ResolvedTsBatch};
use crate::types::{DatabaseId, TenantId};
use crate::wal::{RedoSubRecord, decode_batch_record};

use super::admission;
use super::group_flush::TsKey;

/// Where a streamed record's ops apply: its database, tenant and vShard.
#[derive(Debug, Clone, Copy)]
pub(in crate::data::executor) struct TsStreamAt {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub vshard_id: u32,
}

/// What one streamed record writes into the timeseries collections of this
/// core.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct TsStreamLoads {
    loads: HashMap<TsKey, TsStreamLoad>,
}

/// What one streamed record writes into one timeseries collection.
#[derive(Debug)]
struct TsStreamLoad {
    /// Payload bytes the rows arrive as.
    bytes: usize,
    /// Some ILP sub-record does not parse. The whole-record measure then
    /// reads no line of the record.
    ilp_unparsable: bool,
    /// Per symbol column, the values the record's ILP lines add that its
    /// dictionary lacks, capped at `max_tag_cardinality + 1`.
    fresh: HashMap<String, HashSet<String>>,
    /// Every resolved batch's rows fit the dictionaries.
    resolved_fit: bool,
}

impl Default for TsStreamLoad {
    fn default() -> Self {
        Self {
            bytes: 0,
            ilp_unparsable: false,
            fresh: HashMap::new(),
            resolved_fit: true,
        }
    }
}

impl TsStreamLoads {
    /// Every timeseries collection the record writes on this core.
    pub(in crate::data::executor) fn keys(&self) -> impl Iterator<Item = &TsKey> {
        self.loads.keys()
    }
}

impl CoreLoop {
    /// Fold the op `sub` of a streamed record at `at` into `loads`. Only a
    /// timeseries ingest this core owns adds load.
    pub(in crate::data::executor) fn add_ts_stream_op(
        &self,
        loads: &mut TsStreamLoads,
        sub: &RedoSubRecord,
        at: TsStreamAt,
    ) {
        if RecordType::from_raw(sub.record_type) != Some(RecordType::TimeseriesBatch) {
            return;
        }
        let num_cores = self.redo_apply.num_cores;
        let target_core = if num_cores > 0 {
            at.vshard_id as usize % num_cores
        } else {
            0
        };
        if target_core != self.core_id {
            return;
        }
        let Ok(decoded) = decode_batch_record(&sub.payload) else {
            return;
        };
        if decoded.kind == "columnar" {
            return;
        }
        let key = (at.database_id, at.tenant_id, decoded.collection);
        let ceiling = self.ts_tuning.max_tag_cardinality;
        let memtable = self
            .columnar_memtables
            .get(&key)
            .filter(|mt| !mt.is_empty());
        let load = loads.loads.entry(key).or_default();
        let format = decoded.format.as_deref();
        if format == Some(RESOLVED_INGEST_FORMAT) {
            // A batch that does not decode fails its install, which reports it.
            if let Ok(batch) = ResolvedTsBatch::from_bytes(&decoded.payload) {
                load.bytes = load
                    .bytes
                    .saturating_add(usize::try_from(batch.resolved_bytes).unwrap_or(usize::MAX));
                if let Some(mt) = memtable
                    && !admission::rows_have_tag_headroom(mt, &batch.columns, &batch.rows, ceiling)
                {
                    load.resolved_fit = false;
                }
            }
            return;
        }
        load.bytes = load.bytes.saturating_add(decoded.payload.len());
        let lines: Vec<String> = match format {
            Some("ilp-msgpack") => {
                zerompk::from_msgpack::<Vec<String>>(&decoded.payload).unwrap_or_default()
            }
            Some("ilp") => std::str::from_utf8(&decoded.payload)
                .map(|text| text.lines().map(str::to_string).collect())
                .unwrap_or_default(),
            // A JSON or msgpack row payload carries no ILP line.
            _ => Vec::new(),
        };
        let mut text = String::new();
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            text.push_str(&line);
            text.push('\n');
        }
        if text.is_empty() || load.ilp_unparsable {
            return;
        }
        let Ok(batch) = ilp::parse_batch(&text) else {
            load.ilp_unparsable = true;
            return;
        };
        let Some(mt) = memtable else {
            return;
        };
        let lines = batch.into_lines();
        let cap = ceiling as usize;
        for (idx, (column, column_type)) in mt.schema().columns.iter().enumerate() {
            if *column_type != ColumnType::Symbol {
                continue;
            }
            let Some(dict) = mt.symbol_dict(idx) else {
                continue;
            };
            let fresh = load.fresh.entry(column.clone()).or_default();
            for line in &lines {
                if fresh.len() > cap {
                    break;
                }
                let value = admission::symbol_value(line, column);
                if dict.get_id(value).is_none() && !fresh.contains(value) {
                    fresh.insert(value.to_string());
                }
            }
        }
    }

    /// Flush every memtable that cannot take the streamed record `loads`
    /// measured whole, as the whole-record install flushes before the
    /// record's first row. A failure is the install's error.
    ///
    /// A committed flush of one of the record's collections that is not
    /// installed yet installs first, whatever the load. Its rows are older
    /// than the record, and the install's flushes between its batches must
    /// find no such flush: one installed then would move rows the
    /// install's undo puts back in memory.
    pub(in crate::data::executor) fn flush_ts_for_stream(
        &mut self,
        lsn: u64,
        loads: &TsStreamLoads,
    ) {
        if let Err(error) = self.try_flush_ts_for_stream(loads) {
            self.report_ts_record_flush_error(lsn, error);
        }
    }

    fn try_flush_ts_for_stream(&mut self, loads: &TsStreamLoads) -> crate::Result<()> {
        let now_ms = self.ingest_now_ms();
        for key in loads.loads.keys() {
            self.roll_forward_ts_flush(key, now_ms)?;
        }
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        for (key, load) in &loads.loads {
            let Some(resident) = self
                .columnar_memtables
                .get(key)
                .filter(|mt| !mt.is_empty())
                .map(|mt| mt.memory_bytes())
            else {
                continue;
            };
            let fits = resident.saturating_add(load.bytes) < soft_limit
                && !self.ts_memtable_pressed(key)
                && load.resolved_fit
                && self.ilp_tags_fit(key, load);
            if fits {
                continue;
            }
            let now_ms = self.ingest_now_ms();
            self.flush_ts_collection(key.1, key.0, &key.2, now_ms)?;
        }
        Ok(())
    }

    /// Whether every symbol column of `key`'s memtable takes the values the
    /// record's ILP lines add. Unparsable lines add none, as in the
    /// whole-record measure.
    fn ilp_tags_fit(&self, key: &TsKey, load: &TsStreamLoad) -> bool {
        let Some(mt) = self.columnar_memtables.get(key) else {
            return true;
        };
        let ceiling = self.ts_tuning.max_tag_cardinality as usize;
        for (idx, (column, column_type)) in mt.schema().columns.iter().enumerate() {
            if *column_type != ColumnType::Symbol {
                continue;
            }
            let Some(dict) = mt.symbol_dict(idx) else {
                continue;
            };
            let fresh = if load.ilp_unparsable {
                0
            } else {
                load.fresh.get(column).map_or(0, HashSet::len)
            };
            if dict.len().saturating_add(fresh) > ceiling {
                return false;
            }
        }
        true
    }
}
