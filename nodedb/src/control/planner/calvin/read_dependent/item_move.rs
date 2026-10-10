// SPDX-License-Identifier: BUSL-1.1

//! `TRANSFER_ITEM` across two vShards, as one read-dependent Calvin
//! transaction.
//!
//! The item leaves the source collection's vShard and arrives at the
//! destination collection's vShard with the bytes it held at the source.
//! The destination write depends on a row of another vShard, so:
//!
//! 1. The coordinator reads the item at its source owner.
//! 2. It plans a delete of the item at the source and a put of the same
//!    bytes at the destination, and submits them as one transaction whose
//!    passive read is the item, with the bytes it read as the expected value.
//! 3. Under the transaction's locks the source vShard reads the item again
//!    and broadcasts it to both active vShards through their data-group logs.
//!    Each votes `PredictionDrift` when the item moved, and the coordinator
//!    reads again and resubmits. An item gone at the source ends the move
//!    with `NotFound`.
//!
//! Both halves commit, or neither does.

use std::collections::BTreeMap;

use nodedb_cluster::calvin::types::{EngineKeySet, PassiveReadKeyId, SortedVec};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::{CollectionKey, QualifiedCollection, RlsWriteCheck, Surrogate};

use super::recon::read_kv_row;
use crate::Error;
use crate::bridge::envelope::ErrorCode;
use crate::control::cluster::calvin::executor::ollp::error::OllpError;
use crate::control::planner::calvin::dependent_recon_finish::finish_committed;
use crate::control::planner::calvin::{
    DependentOutcome, DependentRetryArgs, PassiveReads, build_read_dependent_tx_class,
    predicate_class, run_dependent_with_retry, submit_calvin_routed_assign,
};
use crate::control::planner::rls_injection::inject_rls_for_single_plan;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::authorization::authorize_task_set;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// One item move between collections on two vShards.
pub struct CrossShardItemMove {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub source_collection: QualifiedCollection,
    pub dest_collection: QualifiedCollection,
    /// The item's key at the source.
    pub item_key: Vec<u8>,
    /// The item's key at the destination.
    pub dest_key: Vec<u8>,
    /// The moved row's identity at the destination.
    pub surrogate: Surrogate,
    /// The source collection's compiled write policy, decided against the
    /// row the move removes.
    pub source_rls_write_check: RlsWriteCheck,
}

/// Move the item of `mv` across its two vShards as one read-dependent
/// Calvin transaction, for `identity`.
///
/// Returns once both halves committed. An item absent at the source ends
/// the move with `ErrorCode::NotFound`, as a same-vShard move does. A write
/// policy of the destination that refuses the item's bytes refuses the move
/// before anything is submitted.
pub async fn move_item_across_shards(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    mv: CrossShardItemMove,
) -> crate::Result<()> {
    let registry = state
        .calvin_completion_registry
        .get()
        .ok_or(Error::SequencerUnavailable)?;
    let orchestrator = state
        .ollp_orchestrator
        .get()
        .ok_or(Error::SequencerUnavailable)?;
    let source_vshard =
        CollectionKey::from_qualified_str(mv.database_id, mv.source_collection.as_str())?.vshard();
    let dest_vshard =
        CollectionKey::from_qualified_str(mv.database_id, mv.dest_collection.as_str())?.vshard();

    let delete_task = PhysicalTask {
        tenant_id: mv.tenant_id,
        vshard_id: source_vshard,
        database_id: mv.database_id,
        plan: PhysicalPlan::Kv(KvOp::Delete {
            collection: mv.source_collection.clone(),
            keys: vec![mv.item_key.clone()],
            rls_write_check: mv.source_rls_write_check.clone(),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let put_task = |value: Vec<u8>| PhysicalTask {
        tenant_id: mv.tenant_id,
        vshard_id: dest_vshard,
        database_id: mv.database_id,
        plan: PhysicalPlan::Kv(KvOp::Put {
            collection: mv.dest_collection.clone(),
            key: mv.dest_key.clone(),
            value,
            ttl_ms: 0,
            surrogate: mv.surrogate,
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let passive_keys = BTreeMap::from([(
        source_vshard.as_u32(),
        vec![EngineKeySet::Kv {
            collection: mv.source_collection.as_str().to_owned(),
            keys: SortedVec::new(vec![mv.item_key.clone()]),
        }],
    )]);
    let item_row = PassiveReadKeyId::kv(mv.source_collection.clone(), mv.item_key.clone());
    let class_hash = predicate_class(
        &format!("TRANSFER_ITEM TO {}", mv.dest_collection.as_str()),
        mv.source_collection.as_str(),
    );
    let tenant_id = mv.tenant_id;
    let source = &mv;
    // Copy: it captures only references.
    let read_item = move || async move {
        read_kv_row(
            state,
            source.tenant_id,
            source.database_id,
            &source.source_collection,
            &source.item_key,
        )
        .await?
        .ok_or(Error::DataPlane(ErrorCode::NotFound))
    };

    let submit = |item: &Vec<u8>| {
        let item = item.clone();
        let delete_task = delete_task.clone();
        let passive_keys = passive_keys.clone();
        let item_row = item_row.clone();
        let put = put_task(item.clone());
        async move {
            let put = admit_destination_image(state, identity, put)
                .map_err(|e| OllpError::Terminal(Box::new(e)))?;
            // Each attempt's tasks pass the authorization boundary as the
            // capability they are submitted under.
            let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
            let tasks: Vec<PhysicalTask> = authorize_task_set(
                identity,
                &[delete_task, put],
                &state.permissions,
                &state.roles,
                &emitter,
            )
            .map_err(|e| OllpError::Terminal(Box::new(e.into())))?
            .into_tasks()
            .into_iter()
            .map(|task| task.into_physical_task())
            .collect();
            let reads = PassiveReads {
                keys: passive_keys,
                expected: BTreeMap::from([(item_row, Some(item))]),
            };
            orchestrator
                .submit_with_retry_via(
                    class_hash,
                    tenant_id,
                    || {
                        build_read_dependent_tx_class(&tasks, tenant_id, reads.clone())
                            .map(Some)
                            .map_err(|e| OllpError::Terminal(Box::new(e)))
                    },
                    |tx_class| async move {
                        submit_calvin_routed_assign(state, tx_class)
                            .await
                            .map_err(|e| OllpError::Retryable(Box::new(e)))
                    },
                )
                .await
        }
    };

    let initial_item = read_item().await?;
    let outcome = run_dependent_with_retry(DependentRetryArgs {
        registry,
        orchestrator,
        predicate_class_hash: class_hash,
        timeout: std::time::Duration::from_secs(state.tuning.network.default_deadline_secs),
        ollp_max_retries: u32::from(orchestrator.ollp_max_retries()),
        initial_predicted: initial_item,
        submit,
        rescan: read_item,
    })
    .await?;
    let (txn_id, ack_results) = match outcome {
        DependentOutcome::Committed {
            txn_id,
            ack_results,
        } => (txn_id, ack_results),
        DependentOutcome::NoOp => {
            return Err(Error::Internal {
                detail: "a cross-shard TRANSFER_ITEM submitted no transaction".to_owned(),
            });
        }
    };
    // The applied responses of the two writes are counts the move does not
    // report. Draining them keeps the sidecar from holding them. The drain
    // reads only the plans' collections and kinds.
    finish_committed(
        state,
        &[delete_task, put_task(Vec::new())],
        txn_id,
        &ack_results,
    )
    .await?;
    Ok(())
}

/// Run the destination's write policy over the item's bytes in `put`: the
/// injection pass admits the image, or refuses the move.
fn admit_destination_image(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    mut put: PhysicalTask,
) -> crate::Result<PhysicalTask> {
    let scope = RequestAuthScope::for_database(identity, state.auth_stores(), put.database_id);
    inject_rls_for_single_plan(
        put.tenant_id.as_u64(),
        put.database_id,
        &mut put.plan,
        &state.rls,
        state.credentials.catalog(),
        scope.auth(),
    )?;
    Ok(put)
}
