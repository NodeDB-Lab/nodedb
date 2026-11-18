// SPDX-License-Identifier: BUSL-1.1

//! The install of a committed Calvin slice from its stamped redo entry.
//!
//! Every replica runs it from the vShard's data-group log, in log order. The
//! install is the committed-redo install every transaction takes. Around it
//! the slice adds:
//!
//! 1. the slice's ordinal on the core clock, so a later local write stamps
//!    above the record's graph versions;
//! 2. the end of the slice's staged state on a core that staged it: the
//!    staged entry and its synthetic overlay go;
//! 3. the reply the leader's stage decided, read from the entry's
//!    [`CalvinReplySpec`], so every replica answers the same bytes.
//!
//! The fold target rows stay in `Response::write_set`, so the funnel
//! journals them with the record.
//!
//! An install that parks between its batches carries 2 and 3 in its
//! [`CalvinTail`], and the tick that ends the install runs them.

use nodedb_physical::physical_plan::{CalvinInstall, CalvinReplySpec, RedoOrigin};
use tracing::info_span;

use crate::bridge::envelope::{Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_set_journal::RootJournalPermit;
use crate::data::executor::handlers::control::calvin_reply::CalvinReply;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::engine::KvWriteScopeGuard;

use super::batched::InstallTail;
use super::entry::{CalvinBoundary, CommittedRedo};
use super::state::TsInstalled;

/// What a committed Calvin slice owes once its install answered.
pub(super) struct CalvinTail {
    epoch: u64,
    position: u32,
    vshard_id: u32,
    /// The slice's position is a mirror source's.
    mirror: bool,
    reply: CalvinReplySpec,
    /// The slice's fixed-ordinal KV write scope, open until the tail.
    kv_scope: KvWriteScopeGuard,
}

impl CoreLoop {
    /// Install the committed Calvin slice `install` names from its redo
    /// record `committed`, and answer the slice's reply.
    ///
    /// A refused install rolled every write back and leaves any staged entry
    /// in place. A reply that fails to encode leaves the install `Ok`, since
    /// the record installed. Its error travels in `error_code`.
    pub(in crate::data::executor) fn install_calvin_redo(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
        origin: RedoOrigin,
        install: &CalvinInstall,
        permit: Option<RootJournalPermit>,
    ) -> Response {
        let CalvinInstall {
            epoch,
            position,
            epoch_system_ms,
            reply,
            // The Control Plane raises the write mark; the install writes rows.
            user_write: _,
            mirror,
        } = install;
        let (epoch, position, epoch_system_ms) = (*epoch, *position, *epoch_system_ms);
        let vshard_id = task.request.vshard_id.as_u32();
        let _apply_span = info_span!(
            "executor_apply",
            epoch,
            position,
            vshard = vshard_id,
            tenant_id = tid,
            trace_id = ?task.request.trace_id,
        )
        .entered();
        // The record's graph versions stamp at least the slice's ordinal, so
        // a later local write stamps above them.
        let txn_ordinal = nodedb_types::calvin_txn_ordinal(epoch_system_ms, position);
        self.hlc.update_from_remote(txn_ordinal);

        // What each resolved timeseries batch of the record stored: a count
        // reply answers the install's counts.
        let mut ts_installs = Vec::new();
        // Every KV write of the slice stamps the transaction's ordinal, which
        // every replica derives alike.
        let kv_scope = self.kv_engine.open_write_scope(Some(txn_ordinal));
        let response = self.install_committed_redo_into(
            task,
            tid,
            committed,
            origin,
            &mut ts_installs,
            CalvinBoundary {
                permit,
                epoch_system_ms,
            },
        );
        let tail = CalvinTail {
            epoch,
            position,
            vshard_id,
            mirror: *mirror,
            reply: reply.clone(),
            kv_scope,
        };
        // A parked install ends the slice in the tick that ends the
        // install. Its response is a placeholder until then.
        if let Some(park) = self.redo_apply.parking.as_mut() {
            park.tail = InstallTail::Calvin(Box::new(tail));
            return response;
        }
        self.calvin_install_tail(response, &ts_installs, tail)
    }

    /// End the Calvin slice `tail` names once its install answered
    /// `response`, with what each resolved timeseries batch stored in
    /// `ts_installs`. The slice's KV write scope closes. An installed slice
    /// ends its staged state and answers its reply.
    pub(super) fn calvin_install_tail(
        &mut self,
        mut response: Response,
        ts_installs: &[TsInstalled],
        tail: CalvinTail,
    ) -> Response {
        let CalvinTail {
            epoch,
            position,
            vshard_id,
            mirror,
            reply,
            kv_scope,
        } = tail;
        self.kv_engine.close_write_scope(kv_scope);
        if response.status != Status::Ok {
            return response;
        }
        // The record installed: a staged entry of the slice and its overlay
        // are spent. A mirror's position is its source's: a staged entry at
        // the same numbers belongs to another transaction of this cluster.
        if !mirror {
            self.calvin
                .commit_pending
                .remove(&(epoch, position, vshard_id));
            // The record is durable, so a spill directory the teardown
            // cannot remove changes no outcome. The teardown filed its
            // report, and the next core open sweeps the directory.
            if let Err(error) = self.drop_calvin_synthetic_overlay(epoch, position, vshard_id) {
                tracing::error!(
                    core = self.core_id,
                    epoch,
                    position,
                    vshard_id,
                    %error,
                    "calvin install: the transaction's spill directory was not removed"
                );
            }
        }

        match self.calvin_reply_payload(CalvinReply::from(reply), ts_installs) {
            Ok(payload) => response.payload = Payload::from_vec(payload),
            Err(error) => response.error_code = Some(Box::new(error)),
        }
        response
    }
}
