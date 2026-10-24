// SPDX-License-Identifier: BUSL-1.1

//! Plain columnar profile temporal-purge.
//!
//! Row-level audit purge on bitemporal plain-columnar collections.
//! Lists every row version (flushed segments and live memtable), groups
//! them by primary key, and marks the versions no read at or after the
//! cutoff resolves to in the engine's per-segment delete bitmap. The
//! millisecond retention horizon converts to the last ordinal of that
//! millisecond (`ms_to_ordinal_upper`), as the document and edge purges
//! convert it.
//!
//! Per PK, the head is the newest version at or below the cutoff: the one a
//! read at the cutoff resolves to. Every older version goes. The head stays,
//! so "AS OF" reads at or after the cutoff still resolve each key. A head
//! that is a tombstone resolves to absent, so it goes with its whole
//! history. Versions above the cutoff are never touched.
//!
//! A version's row index is its physical position in its segment or in the
//! memtable: the index the delete bitmap marks.

use std::collections::HashMap;

use nodedb_columnar::MutationEngine;
use nodedb_types::{DatabaseId, TenantId};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::versions::{RowVersion, VersionColumns};

impl CoreLoop {
    /// See module docs. Returns the number of rows tombstoned in
    /// per-segment delete bitmaps. No WAL records are appended here; the
    /// caller is responsible for the `RecordType::TemporalPurge` audit
    /// record that covers the batch.
    ///
    /// `Err` when a segment or memtable row does not read, or a row carries
    /// no system time. Nothing is marked then.
    pub(super) fn plain_columnar_purge(
        &mut self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<usize> {
        let key = (database_id, tid, collection.to_string());
        let victims_per_seg = {
            let Some(engine) = self.columnar_engines.get(&key) else {
                return Ok(0);
            };
            if !engine.schema().is_bitemporal() {
                return Ok(0);
            }
            let columns = VersionColumns::of(engine).ok_or_else(|| crate::Error::Storage {
                engine: "columnar".into(),
                detail: "bitemporal collection without primary key columns".into(),
            })?;
            let versions = self.columnar_row_versions(&key, engine, &columns)?;
            purge_victims(
                engine,
                versions,
                nodedb_types::ms_to_ordinal_upper(cutoff_system_ms),
            )
        };
        if victims_per_seg.is_empty() {
            return Ok(0);
        }

        let engine_mut =
            self.columnar_engines
                .get_mut(&key)
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!("columnar engine for '{collection}' is gone mid-purge"),
                })?;
        let mut total = 0usize;
        for (seg_id, mut row_indices) in victims_per_seg {
            row_indices.sort_unstable();
            row_indices.dedup();
            total += row_indices.len();
            engine_mut
                .delete_bitmap_mut(seg_id)
                .mark_deleted_batch(&row_indices);
        }
        Ok(total)
    }
}

/// The row indices to tombstone, per segment: every version older than its
/// PK's head at the ordinal `cutoff`, and the head itself when it is a
/// tombstone. A version the delete bitmap already marks reads as absent, so
/// it is neither a head nor a victim.
fn purge_victims(
    engine: &MutationEngine,
    versions: Vec<RowVersion>,
    cutoff: i64,
) -> HashMap<u64, Vec<u32>> {
    let candidates: Vec<&RowVersion> = versions
        .iter()
        .filter(|v| v.sys_ts <= cutoff)
        .filter(|v| {
            !engine
                .delete_bitmap(v.seg_id)
                .is_some_and(|bm| bm.is_deleted(v.row_idx))
        })
        .collect();
    let mut heads: HashMap<&[u8], (i64, bool)> = HashMap::new();
    for v in &candidates {
        let slot = heads
            .entry(v.pk_bytes.as_slice())
            .or_insert((v.sys_ts, v.closed));
        if v.sys_ts >= slot.0 {
            *slot = (v.sys_ts, v.closed);
        }
    }

    let mut victims_per_seg: HashMap<u64, Vec<u32>> = HashMap::new();
    for v in &candidates {
        let Some(&(head_ts, head_closed)) = heads.get(v.pk_bytes.as_slice()) else {
            continue;
        };
        if v.sys_ts < head_ts || head_closed {
            victims_per_seg.entry(v.seg_id).or_default().push(v.row_idx);
        }
    }
    victims_per_seg
}

#[cfg(test)]
mod tests {
    use nodedb_columnar::pk_index::encode_pk;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("_ts_valid_from", ColumnType::Int64),
            ColumnDef::required("_ts_valid_until", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ])
        .expect("valid")
    }

    /// An ordinal inside millisecond `ms`.
    fn at_ms(ms: i64) -> i64 {
        ms * nodedb_types::NANOS_PER_MS + 3
    }

    fn version(sys_ts: i64, id: i64) -> Vec<Value> {
        vec![
            Value::Integer(sys_ts),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Integer(id),
        ]
    }

    /// A tombstoned memtable row ahead of the superseded version does not
    /// shift the index the purge marks.
    #[test]
    fn the_purge_marks_the_physical_row_behind_a_deleted_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut engine = MutationEngine::new("bt".to_string(), schema());
        // Physical rows: 0 = id 9 (deleted below), 1 = id 1 at 10, 2 = id 1 at 50.
        engine.insert(&version(at_ms(5), 9)).expect("insert");
        engine.insert(&version(at_ms(10), 1)).expect("insert");
        engine.insert(&version(at_ms(50), 1)).expect("insert");
        engine.delete(&Value::Integer(9)).expect("delete");
        let memtable_seg = engine.memtable_segment_id();
        let key = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        core.columnar_engines.insert(key.clone(), engine);

        let purged = core
            .plain_columnar_purge(key.0, key.1, "bt", 100)
            .expect("purge");

        assert_eq!(purged, 1, "the version of id 1 at 10 is superseded");
        let engine = core.columnar_engines.get(&key).expect("engine");
        let bitmap = engine.delete_bitmap(memtable_seg).expect("bitmap");
        assert!(bitmap.is_deleted(0), "the deleted row stays deleted");
        assert!(bitmap.is_deleted(1), "the superseded version is marked");
        assert!(!bitmap.is_deleted(2), "the latest version stays live");
        assert_eq!(
            engine
                .pk_index()
                .get(&encode_pk(&Value::Integer(1)))
                .map(|loc| loc.row_index),
            Some(2)
        );
    }

    /// The horizon reaches the last ordinal of its millisecond: the version
    /// inside ms 101 is the head at horizon 101, so the version before it
    /// goes and the version after the horizon stays.
    #[test]
    fn the_horizon_reaches_the_last_ordinal_of_its_millisecond() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut engine = MutationEngine::new("bt".to_string(), schema());
        // Physical rows: 0 = id 1 in ms 100, 1 = id 1 in ms 101, 2 = id 1 in ms 102.
        engine.insert(&version(at_ms(100), 1)).expect("insert");
        engine.insert(&version(at_ms(101), 1)).expect("insert");
        engine.insert(&version(at_ms(102), 1)).expect("insert");
        let memtable_seg = engine.memtable_segment_id();
        let key = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        core.columnar_engines.insert(key.clone(), engine);

        let purged = core
            .plain_columnar_purge(key.0, key.1, "bt", 101)
            .expect("purge");

        assert_eq!(purged, 1, "only the version older than the head goes");
        let engine = core.columnar_engines.get(&key).expect("engine");
        let bitmap = engine.delete_bitmap(memtable_seg).expect("bitmap");
        assert!(bitmap.is_deleted(0));
        assert!(!bitmap.is_deleted(1), "the head inside ms 101 stays");
        assert!(!bitmap.is_deleted(2), "a version after the horizon stays");
    }

    /// A key a delete closed below the horizon loses its whole history,
    /// tombstone included. A key closed after the horizon keeps the version
    /// a read between the horizon and the close resolves to.
    #[test]
    fn the_purge_removes_a_closed_keys_history_past_the_horizon() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut engine = MutationEngine::new("bt".to_string(), schema());
        let tombstone = |sys_ts: i64, id: i64| {
            vec![
                Value::Integer(sys_ts),
                Value::Integer(i64::MIN),
                Value::Integer(i64::MIN),
                Value::Integer(id),
            ]
        };
        // Physical rows: 0 = id 1 in ms 10, 1 = id 1 closed in ms 20,
        // 2 = id 2 in ms 10, 3 = id 2 closed in ms 300.
        engine.insert(&version(at_ms(10), 1)).expect("insert");
        engine.close(&tombstone(at_ms(20), 1), None).expect("close");
        engine.insert(&version(at_ms(10), 2)).expect("insert");
        engine
            .close(&tombstone(at_ms(300), 2), None)
            .expect("close");
        let memtable_seg = engine.memtable_segment_id();
        let key = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        core.columnar_engines.insert(key.clone(), engine);

        let purged = core
            .plain_columnar_purge(key.0, key.1, "bt", 100)
            .expect("purge");

        assert_eq!(purged, 2, "id 1's version and tombstone go");
        let engine = core.columnar_engines.get(&key).expect("engine");
        let bitmap = engine.delete_bitmap(memtable_seg).expect("bitmap");
        assert!(
            bitmap.is_deleted(0) && bitmap.is_deleted(1),
            "id 1's whole history"
        );
        assert!(!bitmap.is_deleted(2), "id 2's head at the horizon stays");
        assert!(
            !bitmap.is_deleted(3),
            "id 2's tombstone after the horizon stays"
        );
    }
}
