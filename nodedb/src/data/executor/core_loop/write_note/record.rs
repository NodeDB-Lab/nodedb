// SPDX-License-Identifier: BUSL-1.1

//! The apply chokepoints that note committed writes: the newest-ordinal
//! cache commit validation reads, the applied prefix a checkpoint names, and
//! the core watermark.

use nodedb_types::{DatabaseId, TenantId};

use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;

use super::super::CoreLoop;
use super::super::newest_ordinals::collection_vshard;
use super::keys::{KeyRepr, WriteStamp};

impl CoreLoop {
    /// The stamp of the write `task` applies: its vShard and its WAL record.
    /// `None` for a task that carries no WAL record. A record replay or a
    /// committed redo install applies takes the stamp that record names.
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
        self.record_stamps.resolve(WriteStamp {
            vshard: task.request.vshard_id,
            lsn,
        })
    }

    /// Note a committed write.
    ///
    /// Called once per written key at every Data-Plane apply chokepoint. `key`
    /// names the written row for the newest-ordinal cache. It is `None` for
    /// engines whose per-key identity is internal (columnar / timeseries /
    /// array / spatial / FTS).
    ///
    /// In the install pass of a committed-redo apply the note waits in the
    /// apply scope. The apply publishes it once the record settled, so a
    /// rolled-back install leaves no note and no watermark behind.
    pub(in crate::data::executor) fn note_write(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        stamp: WriteStamp,
    ) -> crate::Result<()> {
        // A name that does not match `db` is refused here, before a deferred
        // note could carry it past the install.
        collection_vshard(db, collection)?;
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.defer_write(db, tenant, collection, key, stamp);
            return Ok(());
        }
        self.publish_write(db, tenant, collection, key, stamp)
    }

    /// Note a collection a settled redo record names, with no row key.
    ///
    /// The record is installed and durable, so a name this database does not
    /// write cannot refuse it. Every replica reads the same name, so a refusal
    /// here halts the whole cluster's sequencer on one bookkeeping value, and a
    /// re-delivery halts it again.
    ///
    /// Only the newest-ordinal cache needs the name, and it recomputes what it
    /// lacks. So the note forgets the whole cache, applies the LSN, and files a
    /// report. Nothing a replica stores depends on the cache.
    pub(in crate::data::executor) fn note_settled_collection(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        stamp: WriteStamp,
    ) -> crate::Result<()> {
        if let Err(error) = collection_vshard(db, collection) {
            tracing::error!(
                core = self.core_id,
                lsn = stamp.lsn.as_u64(),
                collection,
                %error,
                "a settled redo record names a collection outside its database; \
                 the record stays applied"
            );
            crate::diag::redo_note_misnamed(self.core_id, stamp.lsn.as_u64(), collection, &error);
            self.newest_ordinals.clear();
            self.publish_applied(stamp);
            return Ok(());
        }
        self.note_write(db, tenant, collection, None, stamp)
    }

    /// Publish a write note: the write lands in the newest-ordinal cache, and
    /// its LSN is applied.
    pub(in crate::data::executor) fn publish_write(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<KeyRepr>,
        stamp: WriteStamp,
    ) -> crate::Result<()> {
        self.track_newest_write(db, tenant, collection, key.as_ref(), stamp.vshard)?;
        self.publish_applied(stamp);
        Ok(())
    }

    /// Note the write's LSN as applied and advance the core watermark
    /// monotonically. Every applied write reaches here, so a checkpoint's
    /// replay stamp names it. A collection note whose rows a keyed note of
    /// the same apply already folded into the newest-ordinal cache publishes
    /// through here alone.
    pub(in crate::data::executor) fn publish_applied(&mut self, stamp: WriteStamp) {
        self.floors.applied_prefix.note_applied(stamp.lsn);
        if stamp.lsn > self.watermark {
            self.watermark = stamp.lsn;
        }
    }

    /// Note a committed write that removed every row of `collection`: a
    /// TRUNCATE. The collection's cached newest ordinals are forgotten.
    ///
    /// In the install pass of a committed-redo apply the note waits in the
    /// apply scope, exactly like [`Self::note_write`].
    pub(in crate::data::executor) fn note_collection_wipe(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        stamp: WriteStamp,
    ) -> crate::Result<()> {
        collection_vshard(db, collection)?;
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.defer_collection_wipe(db, tenant, collection, stamp);
            return Ok(());
        }
        self.publish_collection_wipe(db, tenant, collection, stamp)
    }

    /// Publish a wipe note: forget the collection's cached newest ordinals,
    /// and apply its LSN.
    pub(in crate::data::executor) fn publish_collection_wipe(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        stamp: WriteStamp,
    ) -> crate::Result<()> {
        self.newest_ordinals.forget_collection(
            db,
            tenant,
            collection,
            collection_vshard(db, collection)?,
        );
        self.newest_ordinals.forget_vshard(stamp.vshard);
        self.publish_applied(stamp);
        Ok(())
    }

    /// Note a committed whole-collection removal, if `task` carries a WAL
    /// record. See [`Self::note_collection_wipe`].
    pub(in crate::data::executor) fn note_task_collection_wipe(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
    ) -> crate::Result<()> {
        if let Some(stamp) = self.task_write_stamp(task) {
            self.note_collection_wipe(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                stamp,
            )?;
        }
        Ok(())
    }

    /// Note a committed write with no row key, if `task` carries a WAL
    /// record. Shared by the columnar-family write handlers (columnar /
    /// timeseries / array / spatial / FTS) whose per-key identity is
    /// internal.
    pub(in crate::data::executor) fn note_collection_write(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
    ) -> crate::Result<()> {
        if let Some(stamp) = self.task_write_stamp(task) {
            self.note_write(
                task.request.database_id,
                task.request.tenant_id,
                collection,
                None,
                stamp,
            )?;
        }
        Ok(())
    }

    /// Note a committed document/vector write, keyed by the written row's
    /// cross-engine surrogate, if `task` carries a WAL record. Shared by
    /// every per-surrogate write chokepoint (point put, point insert, point
    /// delete, bulk update, bulk delete).
    pub(in crate::data::executor) fn note_surrogate_write(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: u32,
    ) -> crate::Result<()> {
        if let Some(stamp) = self.task_write_stamp(task) {
            self.note_write(
                task.request.database_id,
                TenantId::new(tid),
                collection,
                Some(KeyRepr::Surrogate(surrogate)),
                stamp,
            )?;
        }
        Ok(())
    }

    /// Note a write a replay arm applied from the WAL record at
    /// `record_lsn`. A no-op when `record_lsn == 0` (no durable record).
    /// `key` is `None` for collection-only entries (e.g. truncate) and `Some`
    /// for per-key/per-surrogate entries, exactly like [`Self::note_write`].
    ///
    /// The replay arms run for restart replay and for a committed redo
    /// install. Both register the stamp of the record they apply, so the
    /// write takes that record's vShard. A record nothing registered applies
    /// on its collection's home vShard, the vShard a live write of the
    /// collection routes to.
    pub(in crate::data::executor) fn note_replay_write(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: Option<KeyRepr>,
        record_lsn: u64,
    ) {
        let db = DatabaseId::new(database_id);
        let noted = match self.replay_stamp(db, collection, record_lsn) {
            Some(Ok(stamp)) => {
                self.note_write(db, TenantId::new(tenant_id), collection, key, stamp)
            }
            Some(Err(error)) => Err(error),
            None => Ok(()),
        };
        if let Err(error) = noted {
            self.replay_record_unapplied(
                "write_note",
                "collection key",
                record_lsn,
                &error.to_string(),
            );
        }
    }

    /// Note a whole-collection removal a replay arm applied from the WAL
    /// record at `record_lsn`, stamped like [`Self::note_replay_write`].
    pub(in crate::data::executor) fn note_replay_wipe(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        record_lsn: u64,
    ) {
        let db = DatabaseId::new(database_id);
        let noted = match self.replay_stamp(db, collection, record_lsn) {
            Some(Ok(stamp)) => {
                self.note_collection_wipe(db, TenantId::new(tenant_id), collection, stamp)
            }
            Some(Err(error)) => Err(error),
            None => Ok(()),
        };
        if let Err(error) = noted {
            self.replay_record_unapplied(
                "write_note",
                "collection key",
                record_lsn,
                &error.to_string(),
            );
        }
    }

    /// The stamp of a write a replay arm applied from the WAL record at
    /// `record_lsn`. `None` when `record_lsn == 0` (no durable record).
    fn replay_stamp(
        &self,
        db: DatabaseId,
        collection: &str,
        record_lsn: u64,
    ) -> Option<crate::Result<WriteStamp>> {
        if record_lsn == 0 {
            return None;
        }
        Some(match self.record_stamps.get(record_lsn) {
            Some(stamp) => Ok(stamp),
            None => collection_vshard(db, collection).map(|vshard| WriteStamp {
                vshard,
                lsn: Lsn::new(record_lsn),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::super::tests::{make_core, make_request, point_put, wal_task};
    use super::*;
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::handlers::kv::crud::KvWriteParams;

    fn kv_put(core: &mut CoreLoop, lsn: u64) -> crate::bridge::envelope::Response {
        core.execute_kv_put(
            &wal_task(lsn),
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
        )
    }

    #[test]
    fn a_write_advances_the_watermark_and_never_regresses_it() {
        let (mut core, _, _, _dir) = make_core();
        let resp = point_put(&mut core, &wal_task(10), "orders", "o1", 7, "1");
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(core.watermark, Lsn::new(10));

        point_put(&mut core, &wal_task(20), "orders", "o1", 7, "2");
        assert_eq!(core.watermark, Lsn::new(20));

        point_put(&mut core, &wal_task(15), "orders", "o1", 7, "3");
        assert_eq!(core.watermark, Lsn::new(20));
    }

    #[test]
    fn a_kv_truncate_advances_the_watermark() {
        let (mut core, _, _, _dir) = make_core();
        assert_eq!(kv_put(&mut core, 42).status, Status::Ok);
        let resp = core.execute_kv_truncate(&wal_task(50), DatabaseId::DEFAULT.as_u64(), 1, "kv");
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(core.watermark, Lsn::new(50));
    }

    #[test]
    fn an_autocommit_apply_is_named_by_the_next_replay_stamp() {
        let (mut core, _, _, _dir) = make_core();
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(40));
        assert_eq!(kv_put(&mut core, 42).status, Status::Ok);

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
    fn no_wal_lsn_notes_nothing() {
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
        assert_eq!(core.watermark, Lsn::ZERO);
    }
}
