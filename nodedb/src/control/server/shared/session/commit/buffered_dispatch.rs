// SPDX-License-Identifier: BUSL-1.1

//! Dispatch a COMMIT's buffered writes once its buffered DDL is finalized.
//!
//! - A multi-shard batch goes through Calvin's Vote/Verdict barrier.
//! - A single-shard batch goes through Calvin too when its vShard's leader
//!   is not this node, or when it writes an edge or a label.
//! - Any other single-shard batch dispatches here as one interactive
//!   transaction record.
//!
//! Every abort undoes the finalized DDL first.

use std::collections::{BTreeSet, HashSet};

use nodedb_cluster::PendingDdlObject;
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::gateway::RouteDecision;
use crate::control::planner::calvin::{DispatchClass, classify_dispatch};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::VShardId;
use crate::wal::RedoPublish;

use super::super::commit_calvin;
use super::super::connection::SessionId;
use super::super::outcome::{AbortReason, CommitOutcome, TxnDataPlane};
use super::super::read_set::ReadSetEntry;
use super::super::reservation_release;
use super::super::store::SessionStore;
use super::read_split::SplitReads;
use super::read_validation::stale_read_abort;
use super::run::compensate_finalized_ddl;
use super::single_shard::dispatch_single_shard;
use super::ts_rejections::RejectedByCollection;

/// One COMMIT's buffered batch, and what its dispatch reads.
pub(super) struct BufferedCommit<'a, D> {
    pub sessions: &'a SessionStore,
    pub session_id: SessionId,
    pub identity: &'a AuthenticatedIdentity,
    pub state: &'a SharedState,
    pub dp: &'a D,
    pub buffered: &'a [PhysicalTask],
    pub read_set: &'a [ReadSetEntry],
    pub split_reads: &'a SplitReads<'a>,
    /// Collections the transaction wrote itself: a read of one is a
    /// read-your-own-write.
    pub written_collections: &'a HashSet<String>,
    pub ts_preview: &'a RejectedByCollection,
}

/// Why a dispatch stopped before every buffered write was durable.
enum Abort {
    /// A read changed. The outcome already rolled the session back.
    Stale(CommitOutcome),
    /// The dispatch returned an error. The session still needs a rollback.
    Failed(AbortReason),
}

impl<D: TxnDataPlane> BufferedCommit<'_, D> {
    /// Dispatch the batch, whose reads touch `read_vshards`. Returns the
    /// abort outcome, or `None` once every buffered write is durable. An
    /// abort undoes `ddl_compensation`.
    pub(super) async fn dispatch(
        &self,
        read_vshards: &BTreeSet<u32>,
        publishes: Vec<RedoPublish>,
        ddl_compensation: &Option<Vec<PendingDdlObject>>,
    ) -> Option<CommitOutcome> {
        let abort = match classify_dispatch(self.buffered, read_vshards) {
            DispatchClass::MultiShard { .. } => self.through_calvin(&publishes).await,
            DispatchClass::SingleShard { vshard } => {
                let leader = crate::control::gateway::live_leaders::resolve_live_decision(
                    self.state,
                    vshard.as_u32(),
                );
                // The interactive transaction WAL record belongs to this
                // coordinator. A remote leader never takes it as a bare
                // Data-Plane LSN, so a commit led elsewhere goes through
                // Calvin, with its leader routing, OCC, durability and apply
                // order. A commit that writes an edge or a label goes too:
                // every edge and label version takes a Calvin ordinal, which
                // orders it against a TRUNCATE's cut and a pinned read cut on
                // every replica.
                if !matches!(leader, RouteDecision::Local)
                    || crate::control::planner::calvin::writes_sequenced_graph(self.buffered)
                {
                    self.through_calvin(&publishes).await
                } else {
                    self.led_here(vshard, publishes).await
                }
            }
        }?;
        compensate_finalized_ddl(self.state, ddl_compensation).await;
        match abort {
            Abort::Stale(outcome) => Some(outcome),
            Abort::Failed(reason) => {
                reservation_release::release_and_rollback(
                    self.state,
                    self.sessions,
                    self.session_id,
                )
                .await;
                Some(CommitOutcome::Aborted { reason })
            }
        }
    }

    /// Flush the batch through Calvin's durable Vote/Verdict barrier,
    /// leader-routed. Calvin's participants check the enlisted reads and
    /// answer an ABORT verdict with a serialization abort (SQLSTATE 40001).
    /// This node checks the reads it homes before the flush.
    async fn through_calvin(&self, publishes: &[RedoPublish]) -> Option<Abort> {
        if let Some(outcome) = self
            .split_reads
            .stale_here(
                self.state,
                self.sessions,
                self.session_id,
                self.written_collections,
            )
            .await
        {
            return Some(Abort::Stale(outcome));
        }
        commit_calvin::run_commit_calvin(
            self.sessions,
            self.session_id,
            self.state,
            commit_calvin::CalvinCommit {
                buffered: self.buffered,
                tenant_id: self.identity.tenant_id,
                reads: self.split_reads.enlisted(),
                event_source: self.dp.event_source(),
                publishes,
                applied_key: self.dp.applied_key().zip(self.dp.applied_key_vshard()),
                ts_preview: self.ts_preview,
                user_roles: self.identity.role_names(),
            },
        )
        .await
        .map(Abort::Failed)
    }

    /// Dispatch the batch to `vshard`, whose leader is this node. Every read
    /// is checked on its vShard's leader first.
    async fn led_here(&self, vshard: VShardId, publishes: Vec<RedoPublish>) -> Option<Abort> {
        if let Some(outcome) = stale_read_abort(
            self.state,
            self.sessions,
            self.session_id,
            self.read_set,
            self.written_collections,
        )
        .await
        {
            return Some(Abort::Stale(outcome));
        }
        dispatch_single_shard(
            self.state,
            self.dp,
            self.buffered,
            self.identity,
            vshard,
            publishes,
            self.ts_preview,
        )
        .await
        .map(Abort::Failed)
    }
}
