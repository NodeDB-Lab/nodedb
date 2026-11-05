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
//!   ([`super::graph_sequencing`]), so every edge version is applied at a
//!   Calvin ordinal. An edge sequenced before the TRUNCATE is hidden, and
//!   one sequenced after it stays, on every replica, under any clock skew
//!   and in any order a core applies the transactions in.
//! - A RESTORE's edge version keeps its historical system time and is
//!   applied at the RESTORE's ordinal. A TRUNCATE sequenced before the
//!   RESTORE leaves it, and one sequenced after the RESTORE hides it.
//!
//! Every other task of the request, such as the settled balance deltas of a
//! cross-shard materialized sum, joins the transaction in request order. The
//! settlement reads guard those deltas.
//!
//! A lone TRUNCATE of a collection that never held an edge, with no settlement
//! read, truncates its rows as an autocommit write.

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
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

use super::dependent_recon::SettledReads;
use super::submit::submit_calvin_routed;
use super::tx_class::build_single_vshard_tx_class;

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

/// Run the TRUNCATEs among `tasks`. `None` when `tasks` holds no TRUNCATE.
///
/// A lone TRUNCATE of an edge-free collection with no settlement read runs as
/// an autocommit write. Any other set runs as one Calvin transaction. Each
/// TRUNCATE of an edge-bearing collection adds its edge shares. Every other
/// task keeps its place in the set and commits under `user_roles`. `settled`
/// guards the balance deltas among them.
///
/// With `identity`, each TRUNCATE passes the clone-write gate and
/// authorization. Without it, the caller already authorized it.
pub(super) async fn dispatch_truncate(
    state: &SharedState,
    tasks: &[PhysicalTask],
    identity: Option<&AuthenticatedIdentity>,
    tenant_id: TenantId,
    user_roles: &[String],
    settled: SettledReads<'_>,
) -> crate::Result<Option<TruncateOutcome>> {
    let Some(first) = tasks.iter().find(|task| is_truncate(&task.plan)) else {
        return Ok(None);
    };
    if tasks.len() == 1 && settled.reads.is_empty() && !truncate_is_edge_bearing(state, first)? {
        return dispatch_lone_truncate(state, first, identity, tenant_id)
            .await
            .map(Some);
    }

    // Each lease holds until the transaction's outcome.
    let mut leases = Vec::new();
    let mut submission = Vec::with_capacity(tasks.len());
    for task in tasks {
        let PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) = &task.plan else {
            submission.push(task.clone());
            continue;
        };
        // The statement holds the collection's lease, so this flag stays true
        // or false until the TRUNCATE's outcome.
        let edge_bearing = truncate_is_edge_bearing(state, task)?;
        let rows_task = match identity {
            Some(identity) => match clone_checked(state, task, identity, tenant_id).await? {
                CloneCheckedOutcome::Handled(_) => {
                    return Err(crate::Error::BadRequest {
                        detail: format!(
                            "TRUNCATE of '{collection}' writes through a clone, so it cannot \
                             join a transaction. Run it alone, on a collection with no edge \
                             and no materialized sum."
                        ),
                    });
                }
                CloneCheckedOutcome::Proceed(checked) => {
                    let (authorized, lease) = checked.into_parts();
                    leases.push(lease);
                    authorized.into_physical_task()
                }
            },
            None => task.clone(),
        };
        let edge_shares = if edge_bearing {
            edge_share_tasks(&rows_task, collection)
        } else {
            Vec::new()
        };
        let edge_shares = match identity {
            Some(identity) => authorize(state, identity, edge_shares)?,
            None => edge_shares,
        };
        submission.push(rows_task);
        submission.extend(edge_shares);
    }

    // A zero-net settlement ships no task, so the set can hold one vShard.
    let mut tx_class = build_single_vshard_tx_class(&submission, tenant_id, settled.reads)?;
    tx_class.set_user_roles(user_roles.to_vec());
    tx_class.set_snapshot_ordinal(settled.snapshot_ordinal);
    #[cfg(feature = "failpoints")]
    crate::control::fail_gate::before_recon_submit(state.node_id).await;
    let apply_result = submit_calvin_routed(state, tx_class).await?;
    drop(leases);
    Ok(Some(TruncateOutcome { apply_result }))
}

/// Run one TRUNCATE of an edge-free collection as an autocommit write.
async fn dispatch_lone_truncate(
    state: &SharedState,
    task: &PhysicalTask,
    identity: Option<&AuthenticatedIdentity>,
    tenant_id: TenantId,
) -> crate::Result<TruncateOutcome> {
    let response = match identity {
        Some(identity) => match clone_checked(state, task, identity, tenant_id).await? {
            CloneCheckedOutcome::Handled(response) => response,
            CloneCheckedOutcome::Proceed(checked) => {
                let response =
                    dispatch_authorized_durable_write(state, checked, TraceId::ZERO).await?;
                crate::control::local_dispatch::reject_data_plane_error(&response)?;
                response
            }
        },
        None => {
            let response = dispatch_rows_plan(state, task).await?;
            crate::control::local_dispatch::reject_data_plane_error(&response)?;
            response
        }
    };
    Ok(TruncateOutcome {
        apply_result: Some(response),
    })
}

/// The clone-write gate and authorization of one rows' truncate.
async fn clone_checked(
    state: &SharedState,
    task: &PhysicalTask,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
) -> crate::Result<CloneCheckedOutcome> {
    let emitter =
        crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    intercept_and_authorize(InterceptAndAuthorizeParams {
        state,
        task: task.clone(),
        identity,
        tenant_id,
        permissions: &state.permissions,
        roles: &state.roles,
        emitter: &emitter,
    })
    .await
}

/// Whether the collection `task` truncates ever held an edge. `false` for a
/// task that is no TRUNCATE.
fn truncate_is_edge_bearing(state: &SharedState, task: &PhysicalTask) -> crate::Result<bool> {
    let PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) = &task.plan else {
        return Ok(false);
    };
    collection_is_edge_bearing(state, task.tenant_id, task.database_id, collection.as_str())
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
    let bare = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?.name();
    Ok(state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), bare)?
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
                collection: nodedb_types::QualifiedCollection::from_stored(
                    DatabaseId::DEFAULT,
                    "c".to_string(),
                )
                .expect("bare name"),
                restart_identity: false,
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    /// A zero-net settlement ships no balance task. Its TRUNCATE on one vShard
    /// still builds a transaction that carries the settlement read.
    #[test]
    fn a_truncate_on_one_vshard_carries_its_settlement_read() {
        use crate::control::server::shared::session::read_set::{EngineTag, ReadKey, ReadSetEntry};
        let read = ReadSetEntry {
            engine: EngineTag::Document,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: "c".to_string(),
            key: ReadKey::Predicate,
            observed_at: Some(42),
            home: None,
        };
        let tx_class = build_single_vshard_tx_class(&[truncate_task()], TenantId::new(1), &[read])
            .expect("a one-vShard TRUNCATE transaction class");
        assert_eq!(tx_class.versioned_reads.len(), 1);
    }

    #[test]
    fn only_a_truncate_is_a_truncate() {
        let task = truncate_task();
        assert!(is_truncate(&task.plan));
        let share = PhysicalPlan::Graph(GraphOp::TruncateEdges {
            collection: nodedb_types::QualifiedCollection::from_stored(
                DatabaseId::DEFAULT,
                "c".to_string(),
            )
            .expect("bare name"),
            vshard: 1,
        });
        assert!(!is_truncate(&share));
    }

    /// A TRUNCATE carries one edge share per vShard, each on the vShard it
    /// names, so the transaction's participants are every vShard.
    #[test]
    fn a_truncate_carries_one_edge_share_per_vshard() {
        let task = truncate_task();
        let collection =
            nodedb_types::QualifiedCollection::from_stored(DatabaseId::DEFAULT, "c".to_string())
                .expect("bare name");
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
        let tx_class = build_single_vshard_tx_class(&submission, TenantId::new(1), &[])
            .expect("a TRUNCATE transaction class");
        assert_eq!(
            tx_class.participating_vshards().len(),
            nodedb_cluster::routing::VSHARD_COUNT as usize
        );
    }
}
