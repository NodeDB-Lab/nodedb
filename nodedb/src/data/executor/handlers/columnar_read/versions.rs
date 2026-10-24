// SPDX-License-Identifier: BUSL-1.1

//! The row versions of a bitemporal columnar collection, and the gate a read
//! passes each row through.
//!
//! A bitemporal collection keeps every version of a primary key. A read at a
//! system-time cut answers, per key, the newest version stamped at or below
//! the cut. A delete closes a key with a tombstone version: a row whose valid
//! interval `[_ts_valid_from, _ts_valid_until)` is empty. A key whose newest
//! version at the cut is a tombstone reads as absent, and a read at a cut
//! never emits a tombstone row.
//!
//! The audit log (`AS OF SYSTEM TIME NULL`) emits every version, tombstones
//! included. A tombstone row carries the cells of the version it closes,
//! its own `_ts_system`, and an empty valid interval
//! (`_ts_valid_from = _ts_valid_until`), which marks it as a delete.

use std::collections::HashMap;

use nodedb_columnar::MutationEngine;
use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::value::Value;
use nodedb_types::{DatabaseId, TenantId};

use crate::data::executor::core_loop::CoreLoop;

/// The read path a corruption report names.
const SITE: &str = "columnar_row_versions";

/// One stored version of a bitemporal columnar row.
pub(in crate::data::executor) struct RowVersion {
    pub seg_id: u64,
    pub row_idx: u32,
    pub pk_bytes: Vec<u8>,
    pub sys_ts: i64,
    /// The version is a tombstone: the key is closed from `sys_ts` on.
    pub closed: bool,
}

/// The positions of the bitemporal columns and the primary key of a schema.
#[derive(Clone)]
pub(in crate::data::executor) struct VersionColumns {
    pub ts_system: usize,
    pub valid_from: Option<usize>,
    pub valid_until: Option<usize>,
    pub pk: Vec<usize>,
}

impl VersionColumns {
    /// The columns of `engine`'s schema. `None` for a collection that is not
    /// bitemporal or has no primary key.
    pub(in crate::data::executor) fn of(engine: &MutationEngine) -> Option<Self> {
        let schema = engine.schema();
        let pk = engine.pk_col_indices().to_vec();
        if pk.is_empty() {
            return None;
        }
        Some(Self {
            ts_system: schema.ts_system_idx()?,
            valid_from: schema.ts_valid_from_idx(),
            valid_until: schema.ts_valid_until_idx(),
            pk,
        })
    }

    /// Whether `row` is a tombstone version: its valid interval is empty.
    pub(in crate::data::executor) fn is_closed(&self, row: &[Value]) -> bool {
        let cell = |idx: Option<usize>| match idx.and_then(|i| row.get(i)) {
            Some(Value::Integer(v)) => Some(*v),
            _ => None,
        };
        matches!(
            (cell(self.valid_from), cell(self.valid_until)),
            (Some(from), Some(until)) if from >= until
        )
    }

    fn pk_bytes(&self, row: &[Value]) -> Option<Vec<u8>> {
        let cells = self
            .pk
            .iter()
            .map(|&i| row.get(i).cloned())
            .collect::<Option<Vec<Value>>>()?;
        Some(version_pk_bytes(&cells))
    }

    fn system_time(&self, row: &[Value]) -> Option<i64> {
        match row.get(self.ts_system) {
            Some(Value::Integer(ts)) => Some(*ts),
            _ => None,
        }
    }
}

/// The version a read emits for each primary key, by system time. A read of
/// every version (`AS OF SYSTEM TIME NULL`) admits every version, tombstones
/// included.
pub(in crate::data::executor) struct VersionGate {
    columns: VersionColumns,
    /// Primary key to the system time of its newest open version at the
    /// cut. `None` when the read admits every version.
    winners: Option<HashMap<Vec<u8>, i64>>,
}

impl VersionGate {
    /// Whether the read emits `row`.
    pub(in crate::data::executor) fn admits(&self, row: &[Value]) -> bool {
        let Some(winners) = &self.winners else {
            return true;
        };
        if self.columns.is_closed(row) {
            return false;
        }
        match (self.columns.pk_bytes(row), self.columns.system_time(row)) {
            (Some(pk), Some(ts)) => winners.get(&pk) == Some(&ts),
            _ => false,
        }
    }
}

impl CoreLoop {
    /// The gate a read of the bitemporal columnar collection at `key` passes
    /// each row through: per key, the newest version at or below
    /// `system_as_of` (every version when `None`), unless that version is a
    /// tombstone. `all_versions` admits every version, tombstones included.
    /// `None` for a collection that is not bitemporal.
    ///
    /// `Err` when a stored version does not read.
    pub(in crate::data::executor) fn columnar_version_gate(
        &self,
        key: &(DatabaseId, TenantId, String),
        system_as_of: Option<i64>,
        all_versions: bool,
    ) -> crate::Result<Option<VersionGate>> {
        let Some(engine) = self.columnar_engines.get(key) else {
            return Ok(None);
        };
        let Some(columns) = VersionColumns::of(engine) else {
            return Ok(None);
        };
        if all_versions {
            return Ok(Some(VersionGate {
                columns,
                winners: None,
            }));
        }
        let mut newest: HashMap<Vec<u8>, (i64, bool)> = HashMap::new();
        for version in self.columnar_row_versions(key, engine, &columns)? {
            let removed = engine
                .delete_bitmap(version.seg_id)
                .is_some_and(|bm| bm.is_deleted(version.row_idx));
            if removed || system_as_of.is_some_and(|cut| version.sys_ts > cut) {
                continue;
            }
            let slot = newest.entry(version.pk_bytes).or_insert((i64::MIN, true));
            if version.sys_ts >= slot.0 {
                *slot = (version.sys_ts, version.closed);
            }
        }
        let winners = newest
            .into_iter()
            .filter(|(_, (_, closed))| !closed)
            .map(|(pk, (ts, _))| (pk, ts))
            .collect();
        Ok(Some(VersionGate {
            columns,
            winners: Some(winners),
        }))
    }

    /// Every row version of the bitemporal columnar collection at `key`. A
    /// flushed row is listed whether its segment's delete bitmap marks it or
    /// not. A memtable row is listed only when live.
    ///
    /// `Err` when a segment or memtable row does not read, or a row carries
    /// no system time.
    pub(in crate::data::executor) fn columnar_row_versions(
        &self,
        key: &(DatabaseId, TenantId, String),
        engine: &MutationEngine,
        columns: &VersionColumns,
    ) -> crate::Result<Vec<RowVersion>> {
        let collection = key.2.as_str();
        let schema = engine.schema();
        // Decode the columns up to the last one a version needs.
        let column_count = columns
            .pk
            .iter()
            .copied()
            .chain([columns.ts_system])
            .chain(columns.valid_from)
            .chain(columns.valid_until)
            .max()
            .unwrap_or(columns.ts_system)
            + 1;

        let mut versions: Vec<RowVersion> = Vec::new();

        // Flushed segments: segment_id starts at 1 (memtable is id 0).
        let segments = self
            .columnar_flushed_segments
            .get(key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for (seg_idx, seg_bytes) in segments.iter().enumerate() {
            let seg_id = seg_idx as u64 + 1;
            let segment =
                self.decode_flushed_segment(collection, seg_id, seg_bytes, column_count, SITE)?;
            for row_idx in 0..segment.row_count() {
                let mut row = vec![Value::Null; column_count];
                for (idx, slot) in row.iter_mut().enumerate() {
                    let wanted = idx == columns.ts_system
                        || columns.pk.contains(&idx)
                        || Some(idx) == columns.valid_from
                        || Some(idx) == columns.valid_until;
                    if wanted {
                        *slot = segment.cell(idx, row_idx, &cell_type(schema, idx))?;
                    }
                }
                versions.push(version_of(
                    columns,
                    &row,
                    collection,
                    seg_id,
                    row_index_u32(collection, seg_id, row_idx)?,
                )?);
            }
        }

        // Memtable rows, each at its physical index.
        let memtable_seg_id = engine.memtable_segment_id();
        for row_idx in 0..engine.memtable().row_count() {
            // `None` is a row the memtable delete bitmap marks.
            let Some(row) = engine.get_memtable_row(row_idx)? else {
                continue;
            };
            versions.push(version_of(
                columns,
                &row,
                collection,
                memtable_seg_id,
                row_index_u32(collection, memtable_seg_id, row_idx)?,
            )?);
        }
        Ok(versions)
    }
}

/// The declared type of column `idx`, for decoding a segment cell.
fn cell_type(schema: &ColumnarSchema, idx: usize) -> ColumnType {
    schema
        .columns
        .get(idx)
        .map_or(ColumnType::Int64, |c| c.column_type)
}

/// The version `row` holds at `(seg_id, row_idx)`.
fn version_of(
    columns: &VersionColumns,
    row: &[Value],
    collection: &str,
    seg_id: u64,
    row_idx: u32,
) -> crate::Result<RowVersion> {
    let sys_ts = columns
        .system_time(row)
        .ok_or_else(|| crate::Error::SegmentCorrupted {
            detail: format!(
                "columnar '{collection}': row {row_idx} of segment {seg_id} holds no integer \
                 _ts_system"
            ),
        })?;
    let pk_bytes = columns
        .pk_bytes(row)
        .ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "columnar '{collection}': row {row_idx} of segment {seg_id} holds no \
                 primary-key cell"
            ),
        })?;
    Ok(RowVersion {
        seg_id,
        row_idx,
        pk_bytes,
        sys_ts,
        closed: columns.is_closed(row),
    })
}

/// Encode a version's primary key from its PK cells, in PK column order.
/// The bytes match what the engine's PK index holds for the same row.
fn version_pk_bytes(cells: &[Value]) -> Vec<u8> {
    match cells {
        [single] => nodedb_columnar::pk_index::encode_pk(single),
        _ => {
            let refs: Vec<&Value> = cells.iter().collect();
            nodedb_columnar::pk_index::encode_composite_pk(&refs)
        }
    }
}

/// `row_idx` as the `u32` row index a delete bitmap marks.
fn row_index_u32(collection: &str, seg_id: u64, row_idx: usize) -> crate::Result<u32> {
    u32::try_from(row_idx).map_err(|_| crate::Error::Internal {
        detail: format!(
            "columnar '{collection}': row {row_idx} of segment {seg_id} is past the u32 row \
             index range"
        ),
    })
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnDef;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("_ts_valid_from", ColumnType::Int64),
            ColumnDef::required("_ts_valid_until", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid")
    }

    fn version(ts: i64, id: i64, v: i64) -> Vec<Value> {
        vec![
            Value::Integer(ts),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Integer(id),
            Value::Integer(v),
        ]
    }

    fn tombstone(ts: i64, id: i64, v: i64) -> Vec<Value> {
        vec![
            Value::Integer(ts),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MIN),
            Value::Integer(id),
            Value::Integer(v),
        ]
    }

    /// A read emits each key's newest version at its cut. A key closed at or
    /// before the cut reads as absent, and one closed after it reads its
    /// version before the close. A read at a cut never emits a tombstone, and
    /// the audit log emits every version, tombstones included.
    #[test]
    fn a_read_emits_each_keys_newest_open_version_at_its_cut() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut engine = MutationEngine::new("bt".to_string(), schema());
        engine.insert(&version(100, 1, 10)).expect("insert");
        engine.insert(&version(200, 1, 20)).expect("insert");
        engine.insert(&version(150, 2, 5)).expect("insert");
        engine.close(&tombstone(300, 2, 5), None).expect("close");
        let key = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        core.columnar_engines.insert(key.clone(), engine);

        let emitted = |cut: Option<i64>| -> Vec<Vec<Value>> {
            let gate = core
                .columnar_version_gate(&key, cut, false)
                .expect("gate")
                .expect("bitemporal");
            let engine = core.columnar_engines.get(&key).expect("engine");
            engine
                .scan_memtable_rows()
                .map(|row| row.expect("read"))
                .filter(|row| gate.admits(row))
                .collect()
        };
        assert_eq!(emitted(None), vec![version(200, 1, 20)], "current");
        assert_eq!(
            emitted(Some(250)),
            vec![version(200, 1, 20), version(150, 2, 5)],
            "before the close"
        );
        assert_eq!(emitted(Some(120)), vec![version(100, 1, 10)]);

        let audit = core
            .columnar_version_gate(&key, None, true)
            .expect("gate")
            .expect("bitemporal");
        assert!(
            audit.admits(&tombstone(300, 2, 5)),
            "the audit log emits the tombstone"
        );
        assert!(audit.admits(&version(100, 1, 10)));
    }
}
