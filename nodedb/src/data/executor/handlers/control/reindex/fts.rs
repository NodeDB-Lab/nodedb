// SPDX-License-Identifier: BUSL-1.1

//! Full-text rebuild of one collection.
//!
//! Start, on the owning core: pin a redb read snapshot of the collection and
//! open its write journal in one step, then hand the snapshot to a plain OS
//! thread. The thread reads the snapshot and derives the canonical rows.
//! The core keeps serving reads and writes from the live index; every write
//! to the collection notes its document in the journal.
//!
//! Cutover, on the owning core: one redb write transaction replaces the
//! collection's rows with the rebuilt ones and writes the live footprint of
//! every noted document over them. A reader sees the old rows or the new
//! ones, never a mix. The cutover emits `atomic_cutover` on the
//! `nodedb::reindex` target.

use std::sync::mpsc;

use tracing::info;

use super::hold::{FTS_BUILD_HOLD, hold_build};
use super::pending::{PendingBuild, PendingReindex, RebuildTarget};
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::sparse::inverted::{
    FTS_REBUILD_JOURNAL_MAX_DOCS, FtsInstallOutcome, FtsRebuildTicket, FtsRebuilt, FtsSnapshot,
};

impl CoreLoop {
    /// Start a full-text rebuild of `target` on its own thread. A collection
    /// with no full-text rows has nothing to rebuild.
    pub(super) fn start_fts_rebuild(&mut self, target: &RebuildTarget) -> crate::Result<()> {
        let Some(ticket) = self.begin_fts_rebuild(target)? else {
            return Ok(());
        };
        let token = ticket.token();
        let fail_scope = self.fail_scope;
        let (tx, rx) = mpsc::sync_channel::<crate::Result<FtsRebuilt>>(1);
        let spawned = std::thread::Builder::new()
            .name(format!("reindex-fts-{}", self.core_id))
            .spawn(move || {
                hold_build(fail_scope, FTS_BUILD_HOLD);
                // The receiver is gone only when the core shut down.
                let _ = tx.send(ticket.read().map(FtsSnapshot::compact));
            });
        if let Err(e) = spawned {
            self.inverted.abort_rebuild(token);
            return Err(crate::Error::Io(e));
        }
        self.maintenance.pending_reindex.push(PendingReindex {
            target: target.clone(),
            build: PendingBuild::Fts { token, rx },
        });
        Ok(())
    }

    /// Pin the snapshot and open the journal, or `None` when the
    /// collection has no full-text rows.
    fn begin_fts_rebuild(
        &mut self,
        target: &RebuildTarget,
    ) -> crate::Result<Option<FtsRebuildTicket>> {
        let db = target.database_id.as_u64();
        if !self
            .inverted
            .has_collection_rows(db, target.tenant_id, &target.collection)?
        {
            return Ok(None);
        }
        let ticket = self.inverted.begin_rebuild(
            db,
            target.tenant_id,
            &target.collection,
            FTS_REBUILD_JOURNAL_MAX_DOCS,
        )?;
        info!(
            target: "nodedb::reindex",
            core = self.core_id,
            index = "fts",
            collection = %target.collection,
            "rebuild_started"
        );
        Ok(Some(ticket))
    }

    /// Cut `rebuilt` over on this core. A refusal is an error: the live
    /// index stays as it is.
    pub(super) fn install_fts(
        &mut self,
        target: &RebuildTarget,
        rebuilt: FtsRebuilt,
    ) -> crate::Result<()> {
        match self.inverted.install_rebuild(rebuilt)? {
            FtsInstallOutcome::Installed {
                terms,
                docs,
                replayed,
            } => {
                info!(
                    target: "nodedb::reindex",
                    core = self.core_id,
                    index = "fts",
                    collection = %target.collection,
                    terms,
                    docs,
                    replayed,
                    "atomic_cutover"
                );
                Ok(())
            }
            FtsInstallOutcome::Refused(refusal) => {
                Err(crate::Error::ObjectNotInPrerequisiteState {
                    object: format!("full-text index of collection \"{}\"", target.collection),
                    detail: format!("rebuild discarded, live index unchanged: {refusal}"),
                })
            }
        }
    }
}
