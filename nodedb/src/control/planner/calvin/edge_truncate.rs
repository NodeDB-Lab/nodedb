// SPDX-License-Identifier: BUSL-1.1

//! TRUNCATE of an edge-bearing collection, as one Calvin transaction.
//!
//! The transaction holds the rows' truncate on the collection's vShard and
//! one `GraphOp::TruncateEdges` on every vShard. It spans every vShard, so
//! it travels as a multi-part transaction. It commits or aborts whole: no
//! reader sees the rows gone and an edge left, and no crash leaves either
//! half owed.
//!
//! - Each vShard's share records a cut of the collection at the
//!   transaction's ordinal. The cut writes no edge version and reads no
//!   stored edge: every read hides the collection's versions applied below
//!   it. Every edge write runs as a Calvin transaction
//!   ([`super::edge_sequencing`]), so every edge version is applied at a
//!   Calvin ordinal. An edge sequenced before the TRUNCATE is hidden, and
//!   one sequenced after it stays, on every replica, under any clock skew
//!   and in any order a core applies the transactions in.
//! - A RESTORE's edge version keeps its historical system time and is
//!   applied at the RESTORE's ordinal. A TRUNCATE sequenced before the
//!   RESTORE leaves it, and one sequenced after the RESTORE hides it.
//!
//! A collection that never held an edge truncates its rows alone, as an
//! autocommit write.

use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils::{
    AutocommitWrite, dispatch_authorized_durable_write, dispatch_durable_autocommit_write,
};
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

use super::submit::submit_calvin_routed;
use super::tx_class::build_static_tx_class;

/// Whether `plan` is a TRUNCATE.
pub fn is_truncate(plan: &PhysicalPlan) -> bool {
    matches!(plan, PhysicalPlan::Document(DocumentOp::Truncate { .. }))
}

/// The answer to a TRUNCATE.
pub(super) struct TruncateOutcome {
    /// The applied response of the rows' truncate, when a participant this
    /// node hosts deposited one.
    pub apply_result: Option<Response>,
}

/// Run the TRUNCATE among `tasks`: with the collection's edges in one
/// Calvin transaction when an edge was ever written into it. `None` when
/// `tasks` holds no TRUNCATE.
///
/// Every other task of the request, such as the settled balance deltas of a
/// cross-shard materialized sum, joins the transaction, and `reads` guard
/// them. The transaction then runs even when the collection has no edge.
///
/// With `identity`, the TRUNCATE passes the clone-write gate and
/// authorization. Without it, the caller already authorized it.
pub(super) async fn dispatch_truncate(
    state: &SharedState,
    tasks: &[PhysicalTask],
    identity: Option<&AuthenticatedIdentity>,
    tenant_id: TenantId,
    reads: &[ReadSetEntry],
) -> crate::Result<Option<TruncateOutcome>> {
    let Some(task) = tasks.iter().find(|task| is_truncate(&task.plan)) else {
        return Ok(None);
    };
    let PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) = &task.plan else {
        return Ok(None);
    };
    let collection = collection.clone();

    // The statement holds the collection's lease, so the flag read here
    // stays true or false until the TRUNCATE's outcome.
    let edge_bearing =
        collection_is_edge_bearing(state, task.tenant_id, task.database_id, collection.as_str())?;
    let others: Vec<PhysicalTask> = tasks
        .iter()
        .filter(|other| !is_truncate(&other.plan))
        .cloned()
        .collect();
    // A request that ships balance deltas needs the transaction to carry them.
    let in_transaction = edge_bearing || !others.is_empty();

    // The clone-write gate and authorization of the rows' truncate. The
    // lease holds until the transaction's outcome.
    let (rows_task, _lease) = match identity {
        Some(identity) => {
            let emitter = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
                &state.audit,
            ));
            match intercept_and_authorize(InterceptAndAuthorizeParams {
                state,
                task: task.clone(),
                identity,
                tenant_id,
                permissions: &state.permissions,
                roles: &state.roles,
                emitter: &emitter,
            })
            .await?
            {
                CloneCheckedOutcome::Handled(response) if !in_transaction => {
                    return Ok(Some(TruncateOutcome {
                        apply_result: Some(response),
                    }));
                }
                CloneCheckedOutcome::Handled(_) => {
                    return Err(crate::Error::BadRequest {
                        detail: format!(
                            "TRUNCATE of '{collection}' writes through a clone and cuts \
                             graph edges, which one transaction does not carry together"
                        ),
                    });
                }
                CloneCheckedOutcome::Proceed(checked) if !in_transaction => {
                    let response =
                        dispatch_authorized_durable_write(state, checked, TraceId::ZERO).await?;
                    crate::control::local_dispatch::reject_data_plane_error(&response)?;
                    return Ok(Some(TruncateOutcome {
                        apply_result: Some(response),
                    }));
                }
                CloneCheckedOutcome::Proceed(checked) => {
                    let (authorized, lease) = checked.into_parts();
                    (authorized.into_physical_task(), Some(lease))
                }
            }
        }
        None if !in_transaction => {
            let response = dispatch_rows_plan(state, task).await?;
            crate::control::local_dispatch::reject_data_plane_error(&response)?;
            return Ok(Some(TruncateOutcome {
                apply_result: Some(response),
            }));
        }
        None => (task.clone(), None),
    };

    let edge_shares = if edge_bearing {
        edge_share_tasks(&rows_task, &collection)
    } else {
        Vec::new()
    };
    let edge_shares = match identity {
        Some(identity) => authorize(state, identity, edge_shares)?,
        None => edge_shares,
    };
    let mut submission = Vec::with_capacity(edge_shares.len() + others.len() + 1);
    submission.push(rows_task);
    submission.extend(edge_shares);
    submission.extend(others);
    let tx_class = build_static_tx_class(&submission, tenant_id, reads)?;
    #[cfg(feature = "failpoints")]
    crate::control::fail_gate::before_recon_submit(state.node_id).await;
    let apply_result = submit_calvin_routed(state, tx_class).await?;
    Ok(Some(TruncateOutcome { apply_result }))
}

/// One `TruncateEdges` of `collection` per vShard, beside `rows_task`.
fn edge_share_tasks(
    rows_task: &PhysicalTask,
    collection: &nodedb_types::QualifiedCollection,
) -> Vec<PhysicalTask> {
    (0..nodedb_cluster::routing::VSHARD_COUNT)
        .map(|vshard| PhysicalTask {
            vshard_id: VShardId::new(vshard),
            plan: PhysicalPlan::Graph(GraphOp::TruncateEdges {
                collection: collection.clone(),
                vshard,
            }),
            ..rows_task.clone()
        })
        .collect()
}

/// Authorize the edge shares as `identity`.
fn authorize(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tasks: Vec<PhysicalTask>,
) -> crate::Result<Vec<PhysicalTask>> {
    let emitter =
        crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    Ok(
        crate::control::server::shared::authorization::authorize_task_set(
            identity,
            &tasks,
            &state.permissions,
            &state.roles,
            &emitter,
        )?
        .into_tasks()
        .into_iter()
        .map(|task| task.into_physical_task())
        .collect(),
    )
}

/// Dispatch the rows' truncate `task` as a durable autocommit write.
async fn dispatch_rows_plan(state: &SharedState, task: &PhysicalTask) -> crate::Result<Response> {
    dispatch_durable_autocommit_write(
        state,
        AutocommitWrite {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            vshard_id: task.vshard_id,
            plan: task.plan.clone(),
            trace_id: TraceId::ZERO,
            event_source: crate::event::EventSource::User,
            txn_id: None,
            // A truncate rewrites no row for a transition to judge.
            user_roles: Vec::new(),
        },
    )
    .await
}

/// Whether an edge was ever written into `collection`, the
/// database-qualified name.
pub(super) fn collection_is_edge_bearing(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
) -> crate::Result<bool> {
    let bare =
        crate::control::target_identity::naming::bare_collection_name(database_id, collection);
    Ok(state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), &bare)?
        .is_some_and(|coll| coll.has_implicit_edges))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn truncate_task() -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(3),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::Truncate {
                collection: nodedb_types::QualifiedCollection::from_stored("c".to_string()),
                restart_identity: false,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn only_a_truncate_is_a_truncate() {
        let task = truncate_task();
        assert!(is_truncate(&task.plan));
        let share = PhysicalPlan::Graph(GraphOp::TruncateEdges {
            collection: nodedb_types::QualifiedCollection::from_stored("c".to_string()),
            vshard: 1,
        });
        assert!(!is_truncate(&share));
    }

    /// A TRUNCATE carries one edge share per vShard, each on the vShard it
    /// names, so the transaction's participants are every vShard.
    #[test]
    fn a_truncate_carries_one_edge_share_per_vshard() {
        let task = truncate_task();
        let collection = nodedb_types::QualifiedCollection::from_stored("c".to_string());
        let shares = edge_share_tasks(&task, &collection);
        assert_eq!(shares.len(), nodedb_cluster::routing::VSHARD_COUNT as usize);
        for (vshard, share) in (0u32..).zip(&shares) {
            assert_eq!(share.vshard_id, VShardId::new(vshard));
            assert!(matches!(
                &share.plan,
                PhysicalPlan::Graph(GraphOp::TruncateEdges { vshard: named, .. }) if *named == vshard
            ));
        }
        let mut submission = vec![task];
        submission.extend(shares);
        let tx_class = build_static_tx_class(&submission, TenantId::new(1), &[])
            .expect("a TRUNCATE transaction class");
        assert_eq!(
            tx_class.participating_vshards().len(),
            nodedb_cluster::routing::VSHARD_COUNT as usize
        );
    }
}
