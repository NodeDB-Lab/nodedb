// SPDX-License-Identifier: BUSL-1.1

//! The apply chokepoints that record committed writes into the per-core
//! write-version index.

use nodedb_types::{DatabaseId, ShardVersion, TenantId, WriteVersion};

use crate::data::executor::task::ExecutionTask;
use crate::types::{Lsn, VShardId};

use super::super::CoreLoop;
use super::keys::{KeyRepr, WriteStamp};

impl CoreLoop {
    /// The stamp of the write `task` applies: its vShard, its WAL record and
    /// the log position of the data-group entry it applies. `None` for a task
    /// that carries no WAL record. A record replay or a committed redo install
    /// applies takes the stamp that record names.
    pub(in crate::data::executor) fn task_write_stamp(
        &self,
        task: &ExecutionTask,
    ) -> Option<WriteStamp> {
        task.wal_lsn()
            .map(|lsn| self.task_write_stamp_at(task, lsn))
    }

    /// The stamp of the write `task` applies from the WAL record at `lsn`,
    /// for a handler that reads the record's LSN off its plan.
    pub(in crate::data::executor) fn task_write_stamp_at(
        &self,
        task: &ExecutionTask,
        lsn: Lsn,
    ) -> WriteStamp {
        self.write_index.record_stamps.resolve(WriteStamp {
            vshard: task.request.vshard_id,
            lsn,
            entry: task.request.entry_version,
        })
    }

    /// The version a write with `stamp` records: the log position of the
    /// entry it applies, or, for a write that applies no entry, its WAL LSN on
    /// top of the latest version its vShard holds here.
    pub(in crate::data::executor) fn write_version_of(&self, stamp: WriteStamp) -> WriteVersion {
        stamp.entry.unwrap_or_else(|| {
            WriteVersion::local_after(self.write_index.latest(stamp.vshard), stamp.lsn.as_u64())
        })
    }

    /// Record a committed write into the per-core version index and advance
    /// the core watermark monotonically.
    ///
    /// Called once per written key at every Data-Plane apply chokepoint. `key`
    /// is `None` for engines whose per-key identity is internal (columnar /
    /// timeseries / array / spatial / FTS): those record only the collection
    /// version.
    ///
    /// In the install pass of a committed-redo apply the version waits in the
    /// apply scope. The apply publishes it once the record settled, so a
    /// rolled-back install leaves no version and no watermark behind.
    pub(in crate::data::executor) fn note_write(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        stamp: WriteStamp,
    ) {
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.defer_write_version(db, tenant, collection, key, stamp);
            return;
        }
        self.publish_write_version(db, tenant, collection, key, stamp);
    }

    /// Record a write version and advance the core watermark monotonically.
    ///
    /// Every applied write reaches here, so this is also where the core notes
    /// the write's LSN as applied: a checkpoint's replay stamp names it.
    pub(in crate::data::executor) fn publish_write_version(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        stamp: WriteStamp,
    ) {
        self.floors.applied_prefix.note_applied(stamp.lsn);
        let version = self.write_version_of(stamp);
        self.write_index
            .note_write(db, tenant, stamp.vshard, collection, key, version);
        if stamp.lsn > self.watermark {
            self.watermark = stamp.lsn;
        }
    }

    /// Record a committed write's collection version only (no per-key
    /// entry), if `task` carries a WAL record. Shared by the columnar-family
    /// write handlers (columnar / timeseries / array / spatial / FTS) whose
    /// per-key identity is internal: a predicate reader validates against
    /// the collection version when it owns no per-key version.
    pub(in crate::data::executor) fn note_collection_write(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
    ) {
        if let Some(stamp) = self.task_write_stamp(task) {
            self.note_write(
                task.request.database_id,
                task.request.tenant_id,
                collection,
                None,
                stamp,
            );
        }
    }

    /// Record a committed document/vector write's version, keyed by the
    /// written row's cross-engine surrogate, if `task` carries a WAL record.
    /// Shared by every per-surrogate write chokepoint (point put, point
    /// insert, point delete, bulk update, bulk delete).
    pub(in crate::data::executor) fn note_surrogate_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: u32,
    ) {
        if let Some(stamp) = self.task_write_stamp(task) {
            self.note_write(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                Some(KeyRepr::Surrogate(surrogate)),
                stamp,
            );
        }
    }

    /// Record the version of a write a replay arm applied from the WAL record
    /// at `record_lsn`. A no-op when `record_lsn == 0` (no durable record).
    /// `key` is `None` for collection-only entries (e.g. truncate) and `Some`
    /// for per-key/per-surrogate entries, exactly like [`Self::note_write`].
    ///
    /// The replay arms run for restart replay and for a committed redo
    /// install. Both register the stamp of the record they apply, so the
    /// write takes that record's vShard and entry position. A record nothing
    /// registered applies on its collection's home vShard, the vShard a live
    /// write of the collection routes to, and names no entry.
    pub(in crate::data::executor) fn note_replay_write(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: Option<KeyRepr>,
        record_lsn: u64,
    ) {
        if record_lsn == 0 {
            return;
        }
        let db = DatabaseId::new(database_id);
        let stamp = match self.write_index.record_stamps.get(record_lsn) {
            Some(stamp) => stamp,
            None => WriteStamp {
                // A name without its database's qualifier is a bare name.
                vshard: nodedb_types::CollectionKey::from_qualified_str(db, collection)
                    .unwrap_or_else(|_| nodedb_types::CollectionKey::from_bare(db, collection))
                    .vshard(),
                lsn: Lsn::new(record_lsn),
                entry: None,
            },
        };
        self.note_write(db, TenantId::new(tenant_id), collection, key, stamp);
    }

    /// Run horizon GC on the per-core version index. Invoked from the periodic
    /// maintenance hook: no dedicated timer.
    pub(in crate::data::executor) fn gc_write_index(&mut self) {
        self.write_index.gc();
    }

    /// Land a snapshot install's cut on each of `floor`'s vShards: the
    /// installed rows hold every write through the cut and carry no per-row
    /// versions.
    pub(in crate::data::executor) fn install_version_floor(&mut self, floor: &[ShardVersion]) {
        for shard in floor {
            if shard.vshard < VShardId::COUNT {
                self.write_index
                    .install(VShardId::new(shard.vshard), shard.version);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::super::keys::{CollKey, WriteKey};
    use super::super::tests::{
        doc_value, entry_task, local, make_core, make_request, point_put, wal_task,
    };
    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::handlers::graph::EdgePutParams;
    use crate::data::executor::handlers::kv::crud::KvWriteParams;

    fn vshard0() -> VShardId {
        VShardId::new(0)
    }

    fn surrogate_key(collection: &str, surrogate: u32) -> WriteKey {
        WriteKey {
            vshard: vshard0(),
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from(collection),
            key: KeyRepr::Surrogate(surrogate),
        }
    }

    fn coll_key(collection: &str) -> CollKey {
        CollKey {
            vshard: vshard0(),
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from(collection),
        }
    }

    #[test]
    fn a_write_with_no_entry_records_a_local_version_and_advances_the_watermark() {
        let (mut core, _, _, _dir) = make_core();

        let resp = point_put(&mut core, &wal_task(10), "orders", "o1", 7, "1");
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            core.write_index.key_version(&surrogate_key("orders", 7)),
            Some(local(10))
        );
        assert_eq!(
            core.write_index.collection_version(&coll_key("orders")),
            Some(local(10))
        );
        assert_eq!(core.watermark, Lsn::new(10));

        point_put(&mut core, &wal_task(20), "orders", "o1", 7, "2");
        assert_eq!(
            core.write_index.key_version(&surrogate_key("orders", 7)),
            Some(local(20))
        );
        assert_eq!(core.watermark, Lsn::new(20));

        // A lower LSN never regresses an existing entry or the watermark.
        point_put(&mut core, &wal_task(15), "orders", "o1", 7, "3");
        assert_eq!(
            core.write_index.key_version(&surrogate_key("orders", 7)),
            Some(local(20))
        );
        assert_eq!(core.watermark, Lsn::new(20));

        // A second collection tracks its own version independently.
        point_put(&mut core, &wal_task(30), "items", "i1", 9, "4");
        assert_eq!(
            core.write_index.collection_version(&coll_key("items")),
            Some(local(30))
        );
        assert_eq!(
            core.write_index.collection_version(&coll_key("orders")),
            Some(local(20))
        );
        assert_eq!(core.watermark, Lsn::new(30));
    }

    #[test]
    fn the_version_recorded_is_the_applying_entrys_log_position() {
        let (mut core, _, _, _dir) = make_core();
        let entry = WriteVersion::logged(4, 812);

        // The WAL LSN is this node's own. The version is the entry's position.
        let resp = point_put(&mut core, &entry_task(57, entry), "orders", "o1", 7, "1");
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            core.write_index.key_version(&surrogate_key("orders", 7)),
            Some(entry)
        );
        assert_eq!(
            core.write_index.collection_version(&coll_key("orders")),
            Some(entry)
        );
        assert_eq!(core.write_index.latest(vshard0()), entry);
        assert_eq!(core.watermark, Lsn::new(57));

        // A replica that applies the same entry at another WAL LSN records
        // the same version.
        let (mut replica, _, _, _replica_dir) = make_core();
        point_put(
            &mut replica,
            &entry_task(9_001, entry),
            "orders",
            "o1",
            7,
            "1",
        );
        assert_eq!(
            replica.write_index.key_version(&surrogate_key("orders", 7)),
            Some(entry)
        );
    }

    #[test]
    fn a_point_update_records_the_rows_version() {
        let (mut core, _, _, _dir) = make_core();
        point_put(
            &mut core,
            &entry_task(40, WriteVersion::logged(1, 5)),
            "orders",
            "o1",
            7,
            "1",
        );
        let raised = nodedb_types::json_to_msgpack(&serde_json::json!("2")).expect("encode");
        let updates = vec![(
            "a".to_string(),
            nodedb_physical::physical_plan::UpdateValue::Literal(raised),
        )];
        let updated = WriteVersion::logged(1, 9);
        let resp = core.execute_point_update(
            &entry_task(41, updated),
            crate::data::executor::handlers::point::update::PointUpdateParams {
                tid: 1,
                collection: "orders",
                document_id: "o1",
                surrogate: Some(Surrogate::new(7)),
                updates: &updates,
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: &[],
                declared_primary_key: None,
            },
        );
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(
            core.write_index.key_version(&surrogate_key("orders", 7)),
            Some(updated),
            "a read of the row from before the update must fail validation"
        );
        assert_eq!(
            core.write_index.collection_version(&coll_key("orders")),
            Some(updated)
        );
    }

    #[test]
    fn a_local_write_sorts_between_the_entries_around_it() {
        let (mut core, _, _, _dir) = make_core();
        point_put(
            &mut core,
            &entry_task(40, WriteVersion::logged(0, 5)),
            "orders",
            "o1",
            7,
            "1",
        );
        point_put(&mut core, &wal_task(41), "orders", "o2", 8, "1");
        let local_write = core
            .write_index
            .key_version(&surrogate_key("orders", 8))
            .expect("local write recorded");
        assert_eq!(
            local_write,
            WriteVersion::local_after(WriteVersion::logged(0, 5), 41)
        );
        assert!(local_write > WriteVersion::logged(0, 5));
        assert!(local_write < WriteVersion::logged(0, 6));
    }

    #[test]
    fn kv_put_records_kvkey_version() {
        let (mut core, _, _, _dir) = make_core();
        let resp = core.execute_kv_put(
            &wal_task(42),
            KvWriteParams {
                did: DatabaseId::DEFAULT.as_u64(),
                tid: 1,
                collection: "kv",
                key: b"k1".as_slice(),
                value: b"v1".as_slice(),
                ttl_ms: 0,
                surrogate: Surrogate::new(3),
                returning: None,
                rls_filters: &[],
            },
        );
        assert_eq!(resp.status, Status::Ok);

        let wk = WriteKey {
            vshard: vshard0(),
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("kv"),
            key: KeyRepr::KvKey(Box::from(b"k1".as_slice())),
        };
        assert_eq!(core.write_index.key_version(&wk), Some(local(42)));
        assert_eq!(
            core.write_index.collection_version(&coll_key("kv")),
            Some(local(42))
        );
        assert_eq!(core.watermark, Lsn::new(42));
    }

    #[test]
    fn an_autocommit_apply_is_named_by_the_next_replay_stamp() {
        let (mut core, _, _, _dir) = make_core();
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(40));
        let resp = core.execute_kv_put(
            &wal_task(42),
            KvWriteParams {
                did: DatabaseId::DEFAULT.as_u64(),
                tid: 1,
                collection: "kv",
                key: b"k1".as_slice(),
                value: b"v1".as_slice(),
                ttl_ms: 0,
                surrogate: Surrogate::new(3),
                returning: None,
                rls_filters: &[],
            },
        );
        assert_eq!(resp.status, Status::Ok);

        let stamp = core.floors.applied_prefix.stamp().expect("exact stamp");
        assert!(
            stamp.skips(42),
            "the applied write is in every later artifact"
        );
        assert!(
            !stamp.skips(41),
            "a record between the floor and the applied write still replays"
        );
    }

    #[test]
    fn edge_put_records_edge_version() {
        let (mut core, _, _, _dir) = make_core();
        let resp = core.execute_edge_put(
            &wal_task(50),
            EdgePutParams {
                tid: 1,
                collection: "graph",
                src_id: "a",
                label: "KNOWS",
                dst_id: "b",
                properties: &[],
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
            },
        );
        assert_eq!(resp.status, Status::Ok);

        let wk = WriteKey {
            vshard: vshard0(),
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("graph"),
            key: KeyRepr::Edge {
                src: Box::from("a"),
                label: Box::from("KNOWS"),
                dst: Box::from("b"),
            },
        };
        assert_eq!(core.write_index.key_version(&wk), Some(local(50)));
        assert_eq!(core.watermark, Lsn::new(50));
    }

    #[test]
    fn a_committed_transaction_records_its_write_versions() {
        let (mut core, _, _, _dir) = make_core();
        let task = wal_task(60);
        let plans = vec![PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "batch"),
            document_id: "d1".into(),
            value: doc_value("a", "1"),
            surrogate: Surrogate::new(11),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        })];
        let resp = core.commit_plans_for_test(&task, 1, &plans, 60);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

        assert_eq!(
            core.write_index.key_version(&surrogate_key("batch", 11)),
            Some(local(60))
        );
        assert_eq!(
            core.write_index.collection_version(&coll_key("batch")),
            Some(local(60))
        );
        assert_eq!(core.watermark, Lsn::new(60));
    }

    #[test]
    fn no_wal_lsn_records_nothing() {
        let (mut core, _, _, _dir) = make_core();
        let task = ExecutionTask::new(make_request(PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
            document_id: "y".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        })));
        point_put(&mut core, &task, "orders", "o1", 7, "1");
        assert_eq!(
            core.write_index.key_version(&surrogate_key("orders", 7)),
            None
        );
        assert_eq!(core.watermark, Lsn::ZERO);
    }

    #[test]
    fn an_install_floor_bounds_the_installed_vshard_only() {
        let (mut core, _, _, _dir) = make_core();
        point_put(&mut core, &wal_task(10), "orders", "o1", 7, "1");
        let cut = WriteVersion::logged(2, 300);
        core.install_version_floor(&[ShardVersion {
            vshard: 0,
            version: cut,
        }]);
        assert_eq!(core.write_index.latest(vshard0()), cut);
        assert_eq!(
            core.write_index.key_version(&surrogate_key("orders", 7)),
            None,
            "the install replaced the row its version described"
        );
        assert_eq!(
            core.write_index.latest(VShardId::new(1)),
            WriteVersion::ZERO
        );
    }
}
