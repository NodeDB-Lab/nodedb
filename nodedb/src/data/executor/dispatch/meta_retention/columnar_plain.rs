// SPDX-License-Identifier: BUSL-1.1

//! Plain columnar profile history purge.
//!
//! Every plain-columnar and spatial collection keeps row versions: a row an
//! overwrite or a delete ended records `sys_to`, the ending write's
//! ordinal. A purge at horizon `H` removes every version with
//! `sys_to <= H`: a later version or a delete replaced it at or below the
//! horizon, so no read at or above the horizon sees it. The newest version
//! at or below the horizon stays, so `AS OF` reads at or after the horizon
//! still resolve each key. A key whose last version ended at or below the
//! horizon loses its whole history, the bitemporal tombstone included.
//! Versions above the horizon are never touched.
//!
//! The bitemporal `audit_retain_ms` retention runs the same rule. Its
//! millisecond cutoff converts to the last ordinal of that millisecond
//! (`ms_to_ordinal_upper`), as the document and edge purges convert it.

use nodedb_types::{DatabaseId, TenantId};

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Remove every version of the columnar collection `collection` whose
    /// `sys_to` is at or below the ordinal `horizon`. Returns the number of
    /// rows purged. A collection with no engine on this core purges nothing.
    ///
    /// The purge reads and rewrites stamps only, never a segment. No WAL
    /// record is appended here: the caller owns the audit record that
    /// covers the batch.
    pub(in crate::data::executor) fn purge_columnar_versions_below(
        &mut self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        horizon: i64,
    ) -> usize {
        let key = (database_id, tid, collection.to_string());
        let Some(engine) = self.columnar_engines.get_mut(&key) else {
            return 0;
        };
        let purged = engine.purge_versions_below(horizon);
        if purged > 0 {
            self.checkpoint_coordinator.mark_dirty("columnar", purged);
            // The removed versions leave the collection's history charge.
            self.recharge_columnar_history(&key);
        }
        purged
    }

    /// The `audit_retain_ms` retention of a bitemporal plain-columnar
    /// collection: [`Self::purge_columnar_versions_below`] at the last
    /// ordinal of millisecond `cutoff_system_ms`. A collection that is not
    /// bitemporal has no audit retention and purges nothing here.
    pub(super) fn plain_columnar_purge(
        &mut self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> usize {
        let key = (database_id, tid, collection.to_string());
        let bitemporal = self
            .columnar_engines
            .get(&key)
            .is_some_and(|engine| engine.schema().is_bitemporal());
        if !bitemporal {
            return 0;
        }
        self.purge_columnar_versions_below(
            database_id,
            tid,
            collection,
            nodedb_types::ms_to_ordinal_upper(cutoff_system_ms),
        )
    }
}

#[cfg(test)]
mod tests {
    use nodedb_columnar::{MutationEngine, VersionCut};
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

    fn key() -> (DatabaseId, TenantId, String) {
        (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string())
    }

    /// `(id, _ts_system)` of every version a read at `cut` sees.
    fn seen(core: &CoreLoop, cut: VersionCut) -> Vec<(i64, i64)> {
        core.columnar_engines
            .get(&key())
            .expect("engine")
            .scan_memtable_versions(cut)
            .map(|row| match row.expect("read").2.as_slice() {
                [Value::Integer(ts), _, _, Value::Integer(id)] => (*id, *ts),
                other => panic!("unexpected cells {other:?}"),
            })
            .collect()
    }

    /// The purge removes the version superseded at or below the horizon,
    /// and a key deleted at or below it, and keeps the head.
    #[test]
    fn the_purge_removes_superseded_and_deleted_versions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut engine = MutationEngine::new("bt".to_string(), schema());
        engine.insert(&version(at_ms(5), 9), 0).expect("insert");
        engine.insert(&version(at_ms(10), 1), 0).expect("insert");
        engine.insert(&version(at_ms(50), 1), 0).expect("insert");
        engine.delete(&Value::Integer(9), at_ms(6)).expect("delete");
        core.columnar_engines.insert(key(), engine);

        let purged = core.plain_columnar_purge(key().0, key().1, "bt", 100);

        assert_eq!(purged, 2, "id 1 at 10 and the deleted id 9");
        assert_eq!(seen(&core, VersionCut::All), vec![(1, at_ms(50))]);
        assert_eq!(seen(&core, VersionCut::Current), vec![(1, at_ms(50))]);
        assert!(
            seen(&core, VersionCut::At(at_ms(20))).is_empty(),
            "no version below the horizon stays but the head"
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
        engine.insert(&version(at_ms(100), 1), 0).expect("insert");
        engine.insert(&version(at_ms(101), 1), 0).expect("insert");
        engine.insert(&version(at_ms(102), 1), 0).expect("insert");
        core.columnar_engines.insert(key(), engine);

        let purged = core.plain_columnar_purge(key().0, key().1, "bt", 101);

        assert_eq!(purged, 1, "only the version older than the head goes");
        assert_eq!(
            seen(&core, VersionCut::All),
            vec![(1, at_ms(101)), (1, at_ms(102))]
        );
        assert_eq!(
            seen(&core, VersionCut::At(at_ms(101))),
            vec![(1, at_ms(101))],
            "the head inside ms 101 answers a read at the horizon"
        );
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
        engine.insert(&version(at_ms(10), 1), 0).expect("insert");
        engine
            .close(&tombstone(at_ms(20), 1), None, 0)
            .expect("close");
        engine.insert(&version(at_ms(10), 2), 0).expect("insert");
        engine
            .close(&tombstone(at_ms(300), 2), None, 0)
            .expect("close");
        core.columnar_engines.insert(key(), engine);

        let purged = core.plain_columnar_purge(key().0, key().1, "bt", 100);

        assert_eq!(purged, 2, "id 1's version and tombstone go");
        assert_eq!(
            seen(&core, VersionCut::All),
            vec![(2, at_ms(10)), (2, at_ms(300))],
            "id 2's head and its later tombstone stay"
        );
        assert_eq!(
            seen(&core, VersionCut::At(at_ms(150))),
            vec![(2, at_ms(10))]
        );
        let engine = core.columnar_engines.get(&key()).expect("engine");
        assert_eq!(
            engine.closed_location(&nodedb_columnar::pk_index::encode_pk(&Value::Integer(1))),
            None,
            "the purged tombstone leaves the closed-key index"
        );
    }

    /// A collection that is not bitemporal keeps versions too. The horizon
    /// purge removes its superseded versions; the audit retention leaves it
    /// alone.
    #[test]
    fn a_plain_collection_purges_at_the_horizon_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid");
        let mut engine = MutationEngine::new("bt".to_string(), schema);
        engine
            .insert(&[Value::Integer(1), Value::Integer(10)], 10)
            .expect("insert");
        engine
            .insert(&[Value::Integer(1), Value::Integer(20)], 20)
            .expect("overwrite");
        core.columnar_engines.insert(key(), engine);

        assert_eq!(
            core.plain_columnar_purge(key().0, key().1, "bt", i64::MAX / 2),
            0
        );
        assert_eq!(
            core.purge_columnar_versions_below(key().0, key().1, "bt", 19),
            0
        );
        assert_eq!(
            core.purge_columnar_versions_below(key().0, key().1, "bt", 20),
            1
        );
        let engine = core.columnar_engines.get(&key()).expect("engine");
        assert_eq!(engine.scan_memtable_versions(VersionCut::All).count(), 1);
    }
}
