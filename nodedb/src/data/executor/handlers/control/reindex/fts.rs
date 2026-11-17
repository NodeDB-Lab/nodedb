// SPDX-License-Identifier: BUSL-1.1

//! Full-text rebuild of one collection: REINDEX, `CREATE SEARCH INDEX`,
//! `DROP INDEX` of a full-text index, and the boot reconcile all run it.
//!
//! Start, on the owning core: pin a redb read snapshot of the sparse
//! database and open the collection's write journal in one step, spawn a
//! plain OS thread that runs the `TextBuildJob`, then install the declared
//! text fields the rebuild indexes. The job replays every stored document
//! version and every stored sync frame of the snapshot into a scratch
//! database. The core keeps serving reads and writes from the live index.
//! Every write to the collection notes its document in the journal.
//!
//! Cutover, on the owning core: one redb write transaction replaces the
//! collection's rows with the scratch rows, then derives every noted
//! document again from its live versions. A reader sees the old rows or
//! the new ones, never a mix. The cutover emits `atomic_cutover` on the
//! `nodedb::reindex` target.
//!
//! A pass whose journal passed its bound runs a catch-up round on a new
//! thread instead, and cuts over when a round ends under the bound.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::mpsc;

use redb::ReadableDatabase as _;
use tracing::info;

use super::hold::{FTS_BUILD_HOLD, hold_build};
use super::pending::{PendingBuild, PendingReindex, RebuildTarget};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::doc_config_key::DocConfigKey;
use crate::data::executor::handlers::spill::cleanup::SpillKind;
use crate::data::executor::handlers::text_rebuild::build::{
    BuildPass, TextBuildEnd, TextBuildJob, remove_scratch,
};
use crate::engine::sparse::btree_versioned::version_walk;
use crate::engine::sparse::inverted::{
    FtsCatchUp, FtsInstallOutcome, FtsRebuildBounds, FtsRebuilt, collection_has_frames,
};

/// A full-text rebuild that started.
pub(in crate::data::executor) struct StartedFtsRebuild {
    pub token: u64,
    /// Items the rebuild's walk visited so far.
    pub progress: Arc<AtomicU64>,
}

/// The thread end of one rebuild pass.
pub(in crate::data::executor) type FtsPassRx = mpsc::Receiver<crate::Result<TextBuildEnd>>;

/// The handles every pass of one rebuild shares.
pub(in crate::data::executor) struct FtsPassShared<'a> {
    pub text_fields: &'a [String],
    pub progress: &'a Arc<AtomicU64>,
    pub cancel: &'a Arc<AtomicBool>,
}

/// What a cutover attempt did.
pub(in crate::data::executor) enum FtsCutover {
    /// The rebuilt rows are live.
    Installed,
    /// The pass's journal passed its bound: run a catch-up round.
    CatchUp(Box<FtsCatchUp>),
}

impl CoreLoop {
    /// The collection's declared text fields, as REINDEX rebuilds them.
    pub(in crate::data::executor) fn current_text_fields(
        &self,
        target: &RebuildTarget,
    ) -> Vec<String> {
        self.declared_text_fields(
            target.database_id.as_u64(),
            target.tenant_id.as_u64(),
            &target.collection,
        )
        .to_vec()
    }

    /// Start a full-text rebuild of `target` over `text_fields` on its own
    /// thread, under the default bounds. See
    /// [`Self::start_fts_rebuild_bounded`].
    pub(in crate::data::executor) fn start_fts_rebuild(
        &mut self,
        target: &RebuildTarget,
        text_fields: Vec<String>,
    ) -> crate::Result<Option<StartedFtsRebuild>> {
        self.start_fts_rebuild_bounded(target, text_fields, FtsRebuildBounds::DEFAULT)
    }

    /// Start a full-text rebuild of `target` over `text_fields` under
    /// `bounds`. `text_fields` is sorted and deduped. It becomes the
    /// collection's indexed field list once the journal is open and the
    /// thread runs. A field the list adds stays out of the readable list:
    /// the register of the index's `Ready` record makes it readable.
    ///
    /// Returns `None` when there is nothing to rebuild: the core holds no
    /// full-text row, document version, or sync frame of the collection.
    /// The field list then changes alone. A collection this core has no
    /// config for refuses a non-empty field list: its writes index no
    /// text, so a rebuilt index drifts from them at the next write. Every
    /// error leaves the field list as it was.
    pub(in crate::data::executor) fn start_fts_rebuild_bounded(
        &mut self,
        target: &RebuildTarget,
        text_fields: Vec<String>,
        bounds: FtsRebuildBounds,
    ) -> crate::Result<Option<StartedFtsRebuild>> {
        let config_key = (
            target.database_id,
            target.tenant_id,
            target.collection.clone(),
        );
        if !self.doc_configs.contains_key(&config_key) && !text_fields.is_empty() {
            return Err(crate::Error::CollectionNotFound {
                tenant_id: target.tenant_id,
                collection: target.collection.clone(),
            });
        }
        if !self.has_text_to_rebuild(target)? {
            self.replace_text_fields(&config_key, text_fields);
            return Ok(None);
        }

        let ticket = self.inverted.begin_rebuild(
            target.database_id.as_u64(),
            target.tenant_id,
            &target.collection,
            bounds,
        )?;
        let token = ticket.token();
        let progress = Arc::new(AtomicU64::new(0));
        let cancel = Arc::new(AtomicBool::new(false));
        let shared = FtsPassShared {
            text_fields: &text_fields,
            progress: &progress,
            cancel: &cancel,
        };
        let rx = match self.spawn_fts_pass(&config_key, BuildPass::Full(ticket), &shared) {
            Ok(rx) => rx,
            Err(e) => {
                self.inverted.abort_rebuild(token);
                return Err(e);
            }
        };
        // The list changes only here, so every error above leaves it as it
        // was. The core runs one task at a time: no write lands between the
        // journal open and this line. Every write from here indexes the new
        // fields, and the journal notes it, so the cutover derives it again
        // under the same fields.
        self.replace_text_fields(&config_key, text_fields.clone());
        self.maintenance.pending_reindex.push(PendingReindex {
            target: target.clone(),
            build: PendingBuild::Fts {
                token,
                rx,
                text_fields,
                progress: Arc::clone(&progress),
                cancel,
            },
        });
        info!(
            target: "nodedb::reindex",
            core = self.core_id,
            index = "fts",
            collection = %target.collection,
            "rebuild_started"
        );
        Ok(Some(StartedFtsRebuild { token, progress }))
    }

    /// Make `text_fields` the indexed field list of the collection at
    /// `key`, when this core has a config for it.
    fn replace_text_fields(&mut self, key: &DocConfigKey, text_fields: Vec<String>) {
        if let Some(config) = self.doc_configs.get_mut(key) {
            config.text_fields.replace_indexed(text_fields);
        }
    }

    /// Run `pass` of a rebuild of the collection at `key` on its own OS
    /// thread. A thread that cannot spawn removes a catch-up round's
    /// previous scratch file.
    pub(in crate::data::executor) fn spawn_fts_pass(
        &self,
        key: &DocConfigKey,
        pass: BuildPass,
        shared: &FtsPassShared<'_>,
    ) -> crate::Result<FtsPassRx> {
        let ticket = pass.ticket();
        let (token, round) = (ticket.token(), ticket.round());
        let previous_scratch = match &pass {
            BuildPass::Full(_) => None,
            BuildPass::CatchUp(catch_up) => Some(catch_up.previous.scratch_path().to_path_buf()),
        };
        let job = TextBuildJob {
            pass,
            text_fields: shared.text_fields.to_vec(),
            config: self.doc_configs.get(key).cloned(),
            scratch_path: SpillKind::TextRebuild
                .core_dir(&self.data_dir, self.core_id)
                .join(format!("rebuild-{token}-{round}.redb")),
            governor: Arc::clone(&self.governor),
            progress: Arc::clone(shared.progress),
            cancel: Arc::clone(shared.cancel),
        };
        let fail_scope = self.fail_scope;
        let (tx, rx) = mpsc::sync_channel::<crate::Result<TextBuildEnd>>(1);
        let spawned = std::thread::Builder::new()
            .name(format!("reindex-fts-{}", self.core_id))
            .spawn(move || {
                if round == 0 {
                    hold_build(fail_scope, FTS_BUILD_HOLD);
                }
                // The receiver is gone only when the core shut down.
                let _ = tx.send(job.run());
            });
        match spawned {
            Ok(_) => Ok(rx),
            Err(e) => {
                if let Some(path) = previous_scratch {
                    remove_scratch(&path);
                }
                Err(crate::Error::Io(e))
            }
        }
    }

    /// Whether this core holds anything a rebuild of `target` reads or
    /// replaces: a full-text row, a stored document version, or a stored
    /// sync frame. A core that holds none has an empty index already.
    fn has_text_to_rebuild(&self, target: &RebuildTarget) -> crate::Result<bool> {
        let (db, tid) = (target.database_id.as_u64(), target.tenant_id.as_u64());
        if self
            .inverted
            .has_collection_rows(db, target.tenant_id, &target.collection)?
        {
            return Ok(true);
        }
        let txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!("rebuild source probe: {e}"),
            })?;
        let documents = version_walk::open_documents(&txn)?;
        if version_walk::collection_has_versions(&documents, db, tid, &target.collection)? {
            return Ok(true);
        }
        collection_has_frames(&txn, (db, tid), &target.collection)
    }

    /// Cut `rebuilt` over on this core. A refusal is an error: the live
    /// index stays as it is. A catch-up hands the scratch file on to the
    /// next pass. Every other end removes it.
    pub(super) fn install_fts(
        &mut self,
        target: &RebuildTarget,
        text_fields: &[String],
        rebuilt: FtsRebuilt,
    ) -> crate::Result<FtsCutover> {
        let scratch_path = rebuilt.scratch_path().to_path_buf();
        let outcome = self.cut_over_fts(target, text_fields, rebuilt);
        if !matches!(outcome, Ok(FtsInstallOutcome::CatchUp(_))) {
            remove_scratch(&scratch_path);
        }
        match outcome? {
            FtsInstallOutcome::CatchUp(catch_up) => Ok(FtsCutover::CatchUp(catch_up)),
            FtsInstallOutcome::Installed { replayed } => {
                info!(
                    target: "nodedb::reindex",
                    core = self.core_id,
                    index = "fts",
                    collection = %target.collection,
                    replayed,
                    "atomic_cutover"
                );
                Ok(FtsCutover::Installed)
            }
            FtsInstallOutcome::Refused(refusal) => {
                Err(crate::Error::ObjectNotInPrerequisiteState {
                    object: format!("full-text index of collection \"{}\"", target.collection),
                    detail: format!("rebuild discarded, live index unchanged: {refusal}"),
                })
            }
        }
    }

    /// The cutover transaction. The declared fields must still be the ones
    /// the rebuild indexed: a register that changed them during the rebuild
    /// discards it.
    fn cut_over_fts(
        &self,
        target: &RebuildTarget,
        text_fields: &[String],
        rebuilt: FtsRebuilt,
    ) -> crate::Result<FtsInstallOutcome> {
        let (db, tid) = (target.database_id.as_u64(), target.tenant_id.as_u64());
        if self.declared_text_fields(db, tid, &target.collection) != text_fields {
            self.inverted.abort_rebuild(rebuilt.token());
            return Err(crate::Error::ObjectNotInPrerequisiteState {
                object: format!("full-text index of collection \"{}\"", target.collection),
                detail: "its declared fields changed during the rebuild; \
                         rebuild discarded, live index unchanged"
                    .to_string(),
            });
        }
        self.inverted.install_rebuild(rebuilt, &mut |txn, touched| {
            self.replay_journaled_text(txn, db, tid, &target.collection, touched)
        })
    }
}
