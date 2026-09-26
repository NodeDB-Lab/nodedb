// SPDX-License-Identifier: BUSL-1.1

//! Calvin key ownership on a core.
//!
//! A staged Calvin transaction owns the rows it staged from its stage until
//! its flush or drop. Its flush installs images the stage computed, so a
//! write that lands on one of those rows in between would be overwritten.
//! A write to an owned row therefore waits here, in arrival order.
//!
//! - A document or KV point write waits only when a staged transaction staged
//!   its row, or truncated its collection.
//! - Every other write, and a committed session transaction's redo, waits
//!   when a staged transaction writes its collection.
//!
//! When the owner resolves, each waiting write that no other owner fences
//! runs, in arrival order. A write whose WAL record sits below the redo record
//! of a flush it waited on is refused with `RetryableRefusal`: restart replay
//! applies records in LSN order, so it would install that write before the
//! flush. The refusal cancels its record, and the client retries. A write
//! still waiting at its request deadline is refused the same way.

use std::collections::VecDeque;
use std::time::Instant;

use nodedb_physical::physical_plan::{DocumentOp, KvOp, MetaOp, PhysicalPlan};
use nodedb_types::{RowIdentity, TenantId};

use super::CoreLoop;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::wal_replication::transaction_redo::collections::written_collections;
use crate::data::executor::handlers::control::calvin_synthetic_txn_id;
use crate::data::executor::handlers::transaction::stage_write::kv_row_identity;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;

/// `(epoch, position, vshard)` of a staged Calvin transaction.
type Owner = (u64, u32, u32);

/// A write waiting for the Calvin transaction that owns its rows.
pub(in crate::data::executor) struct ParkedWrite {
    task: ExecutionTask,
    owner: Owner,
    /// The highest redo LSN among the flushes this write waited on.
    passed_flush_lsn: Option<Lsn>,
}

/// The writes waiting on this core, in arrival order, and the owners that
/// resolved since the core last released writes.
#[derive(Default)]
pub(in crate::data::executor) struct CalvinFence {
    parked: VecDeque<ParkedWrite>,
    /// Each owner that flushed, with its redo LSN, or dropped, with `None`.
    resolved: Vec<(Owner, Option<Lsn>)>,
}

#[cfg(test)]
impl CalvinFence {
    /// Number of waiting writes.
    pub(in crate::data::executor) fn len(&self) -> usize {
        self.parked.len()
    }
}

impl CalvinFence {
    /// Record that the staged transaction `owner` flushed at `flush_lsn`, or
    /// dropped when `flush_lsn` is `None`. The core releases its waiting
    /// writes once the resolving request answered.
    pub(in crate::data::executor) fn note_resolved(
        &mut self,
        owner: (u64, u32, u32),
        flush_lsn: Option<Lsn>,
    ) {
        if !self.parked.is_empty() {
            self.resolved.push((owner, flush_lsn));
        }
    }
}

/// What a write targets, as the fence compares it with staged state.
enum FenceTarget {
    /// Every row of a collection.
    Collection(String),
    /// One document row, by surrogate.
    Surrogate(String, u32),
    /// One KV row, by its overlay identity.
    Row(String, RowIdentity),
}

impl FenceTarget {
    fn collection(&self) -> &str {
        match self {
            Self::Collection(collection)
            | Self::Surrogate(collection, _)
            | Self::Row(collection, _) => collection,
        }
    }
}

/// The targets of `plan`, or `None` when it writes no base state.
fn fence_targets(plan: &PhysicalPlan) -> Option<Vec<FenceTarget>> {
    if let PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { collections, .. }) = plan {
        return Some(
            collections
                .iter()
                .cloned()
                .map(FenceTarget::Collection)
                .collect(),
        );
    }
    if matches!(
        plan,
        PhysicalPlan::Meta(
            MetaOp::CalvinExecuteStatic { .. }
                | MetaOp::CalvinExecuteActive { .. }
                | MetaOp::CalvinFlush { .. }
                | MetaOp::CalvinDrop { .. }
                | MetaOp::CalvinResolve { .. }
        )
    ) || !plan_is_write(plan)
    {
        return None;
    }
    let point = match plan {
        PhysicalPlan::Document(
            DocumentOp::PointPut {
                collection,
                surrogate,
                ..
            }
            | DocumentOp::PointInsert {
                collection,
                surrogate,
                ..
            }
            | DocumentOp::PointUpdate {
                collection,
                surrogate,
                ..
            }
            | DocumentOp::PointDelete {
                collection,
                surrogate,
                ..
            },
        ) => vec![FenceTarget::Surrogate(
            collection.as_str().to_string(),
            surrogate.as_u32(),
        )],
        PhysicalPlan::Kv(
            KvOp::Put {
                collection, key, ..
            }
            | KvOp::Insert {
                collection, key, ..
            }
            | KvOp::InsertIfAbsent {
                collection, key, ..
            }
            | KvOp::Expire {
                collection, key, ..
            }
            | KvOp::Persist {
                collection, key, ..
            },
        ) => vec![FenceTarget::Row(
            collection.as_str().to_string(),
            kv_row_identity(key),
        )],
        PhysicalPlan::Kv(KvOp::Delete {
            collection, keys, ..
        }) => keys
            .iter()
            .map(|key| FenceTarget::Row(collection.as_str().to_string(), kv_row_identity(key)))
            .collect(),
        _ => written_collections(std::slice::from_ref(plan))
            .into_iter()
            .map(FenceTarget::Collection)
            .collect(),
    };
    Some(point)
}

impl CoreLoop {
    /// Hold `task` when a staged Calvin transaction owns a row it writes.
    /// Returns the task when nothing owns its rows.
    pub(in crate::data::executor) fn park_if_calvin_owned(
        &mut self,
        task: ExecutionTask,
    ) -> Option<ExecutionTask> {
        match self.calvin_owner_of(&task) {
            Some(owner) => {
                // Raft fixed this write's order. Parking it holds its data
                // group's applied index behind the owner's flush, so the
                // capture names the write and its owner.
                if matches!(
                    task.request.admission,
                    crate::bridge::envelope::Admission::Exempt(
                        crate::bridge::envelope::ExemptReason::AlreadyOrdered
                    )
                ) {
                    crate::diag::replicated_write_parked(
                        self.core_id,
                        task.plan()
                            .named_collections()
                            .first()
                            .copied()
                            .unwrap_or(""),
                        owner,
                    );
                }
                self.calvin.fence.parked.push_back(ParkedWrite {
                    task,
                    owner,
                    passed_flush_lsn: None,
                });
                None
            }
            None => Some(task),
        }
    }

    /// The earliest staged Calvin transaction that owns a row `task` writes.
    fn calvin_owner_of(&self, task: &ExecutionTask) -> Option<Owner> {
        if self.calvin.commit_pending.is_empty() {
            return None;
        }
        let targets = fence_targets(task.plan())?;
        let database_id = task.request.database_id;
        let tenant_id = task.request.tenant_id;
        let mut owners: Vec<Owner> = self.calvin.commit_pending.keys().copied().collect();
        owners.sort_unstable();
        owners
            .into_iter()
            .find(|owner| self.owns_any(*owner, database_id, tenant_id, &targets))
    }

    /// Whether the staged transaction `owner` owns any of `targets`.
    fn owns_any(
        &self,
        owner: Owner,
        database_id: crate::types::DatabaseId,
        tenant_id: TenantId,
        targets: &[FenceTarget],
    ) -> bool {
        let Some(pending) = self.calvin.commit_pending.get(&owner) else {
            return false;
        };
        if pending.tenant_id != tenant_id {
            return false;
        }
        let written = written_collections(&pending.plans);
        let overlay = calvin_synthetic_txn_id(owner.0, owner.1, owner.2)
            .ok()
            .and_then(|txn_id| self.txn_overlays.get(&txn_id));
        targets.iter().any(|target| {
            if !written.iter().any(|c| c == target.collection()) {
                return false;
            }
            let coll_key = (database_id, tenant_id, target.collection().to_string());
            match target {
                FenceTarget::Collection(_) => true,
                FenceTarget::Surrogate(_, surrogate) => overlay.is_none_or(|overlay| {
                    overlay.is_truncated(&coll_key)
                        || overlay.get(&coll_key, *surrogate).is_some()
                        || overlay.get_ttl(&coll_key, *surrogate).is_some()
                }),
                FenceTarget::Row(_, identity) => overlay.is_none_or(|overlay| {
                    overlay.is_truncated(&coll_key)
                        || overlay.surrogate_for_doc_id(&coll_key, identity).is_some()
                }),
            }
        })
    }

    /// Run every waiting write no owner fences any more, once an owner
    /// resolved.
    pub(in crate::data::executor) fn release_resolved_calvin_owners(&mut self) {
        let resolved = std::mem::take(&mut self.calvin.fence.resolved);
        if resolved.is_empty() {
            return;
        }
        for (owner, flush_lsn) in resolved {
            let Some(lsn) = flush_lsn else {
                continue;
            };
            for parked in &mut self.calvin.fence.parked {
                if parked.owner == owner {
                    parked.passed_flush_lsn = parked.passed_flush_lsn.max(Some(lsn));
                }
            }
        }
        let parked = std::mem::take(&mut self.calvin.fence.parked);
        for mut write in parked {
            if let Some(next) = self.calvin_owner_of(&write.task) {
                write.owner = next;
                self.calvin.fence.parked.push_back(write);
                continue;
            }
            let below_flush = write
                .passed_flush_lsn
                .is_some_and(|flush| write.task.wal_lsn().is_some_and(|lsn| lsn < flush));
            if below_flush {
                let response = self.response_error(
                    &write.task,
                    ErrorCode::RetryableRefusal {
                        reason: "a Calvin transaction installed this row at a later log \
                                 position while the write waited"
                            .into(),
                    },
                );
                self.send_parked_response(response);
            } else {
                self.run_task(write.task);
            }
        }
    }

    /// Refuse every waiting write whose request deadline passed.
    pub(in crate::data::executor) fn expire_calvin_parked(&mut self) {
        if self.calvin.fence.parked.is_empty() {
            return;
        }
        // no-determinism: the request deadline bounds a wait; the refusal
        // cancels the write's record, so no replica applies it.
        let now = Instant::now();
        let parked = std::mem::take(&mut self.calvin.fence.parked);
        for write in parked {
            if now > write.task.request.deadline {
                let response = self.response_error(
                    &write.task,
                    ErrorCode::RetryableRefusal {
                        reason: format!(
                            "the write waited past its deadline for Calvin transaction \
                             {}/{} that owns its row",
                            write.owner.0, write.owner.1
                        ),
                    },
                );
                self.send_parked_response(response);
            } else {
                self.calvin.fence.parked.push_back(write);
            }
        }
    }

    fn send_parked_response(&mut self, response: Response) {
        if let Err(e) = self
            .response_tx
            .try_push(crate::bridge::dispatch::BridgeResponse { inner: response })
        {
            tracing::warn!(
                core = self.core_id,
                error = %e,
                "failed to send a parked write's refusal: response queue full"
            );
        }
    }
}
