// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral strict atomic Calvin task-set dispatch (static path).
//!
//! The universal strict entry point validates the complete task set, rejects
//! mid-block statements, builds one static `TxClass`, and routes exactly one
//! submit-and-await to the sequencer group leader.
//!
//! It takes the session-derived inputs the core needs — the dispatch's position
//! in the transaction lifecycle and the session read-set — as plain parameters,
//! so both the pgwire and native protocol paths can supply them and share one
//! implementation. The optional Data-Plane response is present only when the
//! scheduler retained a materialized applied primary response; it is absent
//! when no response was retained.
//!
//! The OLLP (dependent-predicate) variant is intentionally NOT handled here: it
//! is still tied to the local `OllpOrchestrator` and completion registry and is
//! not yet leader-routed. Callers that can carry a dependent predicate must
//! route that case through their own OLLP path.

use crate::bridge::envelope::Response;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, build_single_vshard_tx_class,
    classify_dispatch, read_vshards_of, submit_calvin_routed,
};
use crate::control::server::shared::authorization::AuthorizedTaskSet;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

/// The sources a Calvin transaction's writes commit under.
#[derive(Debug, Clone)]
pub(crate) struct TxnProvenance {
    /// The transaction's own source.
    pub event_source: EventSource,
    /// Indexes into the task set of the tasks a trigger body buffered. Their
    /// rows commit under `Trigger`.
    pub body_tasks: Vec<u32>,
    /// The messages the transaction's trigger bodies published, as
    /// [`crate::wal::RedoPublish::encode_all`] wrote them. Empty for none.
    pub publishes: Vec<u8>,
    /// The encoded dedup key of the cross-shard request the transaction
    /// applies, with the vShard the request addresses. `None` for any other
    /// transaction.
    pub applied_key: Option<(Vec<u8>, u32)>,
}

impl TxnProvenance {
    /// A client transaction no trigger body joined.
    pub(crate) fn client() -> Self {
        Self {
            event_source: EventSource::User,
            body_tasks: Vec::new(),
            publishes: Vec::new(),
            applied_key: None,
        }
    }
}

/// Submit one trusted internal strict atomic static Calvin task set.
///
/// The task set must be non-empty. Both single- and multi-participant task sets
/// are admitted only at autocommit or commit flush; a mid-block statement is
/// rejected because it cannot be atomically buffered. The permissive
/// single-vShard builder is used for every task set: it accepts one or more
/// write participants while deriving all write homes internally, including both
/// endpoints of graph edges. The full read set remains part of the transaction
/// class, retaining read participants and their OCC observations.
///
/// On success, `Some(Response)` means the scheduler retained a materialized
/// applied primary response; `None` means no response was retained. This is not
/// an affected-row envelope and callers must apply their own operation semantics.
///
/// Every participant stamps `provenance` on the transaction's writes.
pub(crate) async fn dispatch_strict_atomic_tasks_to_calvin(
    state: &SharedState,
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    position: TxnDispatchPosition,
    reads: &[ReadSetEntry],
    lock_owner: Option<nodedb_cluster::calvin::types::TxnIdWire>,
    provenance: TxnProvenance,
) -> crate::Result<Option<Response>> {
    let resolved = crate::control::write_resolve::resolve_tasks_for_log(state, tasks).await?;
    let tasks = resolved.as_deref().unwrap_or(tasks);
    let mut tx_class = admit_strict_atomic_tasks(tasks, tenant_id, position, reads)?;
    tx_class.set_lock_owner(lock_owner);
    tx_class.set_event_source(provenance.event_source.wal_code());
    tx_class.set_body_plans(provenance.body_tasks);
    tx_class.set_publishes(provenance.publishes);
    if let Some((applied_key, vshard)) = provenance.applied_key {
        tx_class.set_applied_key(applied_key, vshard);
    }
    submit_calvin_routed(state, tx_class).await
}

/// Validate strict atomic admission and build its one static transaction class.
/// Kept pure so the empty/mid-block gates and complete-home construction are
/// testable.
fn admit_strict_atomic_tasks(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    position: TxnDispatchPosition,
    reads: &[ReadSetEntry],
) -> crate::Result<nodedb_cluster::calvin::types::TxClass> {
    if tasks.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "strict atomic Calvin task set cannot be empty".to_owned(),
        });
    }
    if position == TxnDispatchPosition::MidBlockStatement {
        return Err(crate::Error::CrossShardInExplicitTransaction);
    }
    // This constructor permits one or more write participants and derives the
    // complete write-home set itself. In particular, graph edges contribute both
    // endpoint homes, which the task-level classifier cannot reconstruct from a
    // task's routed source home alone. It still unions `reads` into participants
    // and preserves their versioned OCC observations.
    build_single_vshard_tx_class(tasks, tenant_id, reads)
}

/// Preserve the legacy multi-shard helper's admission ordering independent of
/// the async sequencer state: cross-shard mid-block statements are rejected
/// before the mode branch, including best-effort mode.
fn admit_legacy_multi_shard_dispatch(
    cross_shard_mode: CrossShardTxnMode,
    position: TxnDispatchPosition,
) -> crate::Result<()> {
    if position == TxnDispatchPosition::MidBlockStatement {
        return Err(crate::Error::CrossShardInExplicitTransaction);
    }
    match cross_shard_mode {
        CrossShardTxnMode::Strict => Ok(()),
        CrossShardTxnMode::BestEffortNonAtomic => Err(crate::Error::Internal {
            detail: "unexpected non-Calvin dispatch outcome for strict multi-shard query"
                .to_owned(),
        }),
    }
}

/// Drive the authorized strict Calvin multi-shard path.
pub async fn dispatch_authorized_tasks_to_calvin(
    state: &SharedState,
    authorized: AuthorizedTaskSet,
    tenant_id: TenantId,
    cross_shard_mode: CrossShardTxnMode,
    position: TxnDispatchPosition,
    reads: &[ReadSetEntry],
    lock_owner: Option<nodedb_cluster::calvin::types::TxnIdWire>,
) -> crate::Result<Option<Response>> {
    let tasks: Vec<PhysicalTask> = authorized
        .into_tasks()
        .into_iter()
        .map(|task| task.into_physical_task())
        .collect();
    dispatch_tasks_to_calvin(
        state,
        &tasks,
        tenant_id,
        cross_shard_mode,
        position,
        reads,
        lock_owner,
    )
    .await
}

/// Drive the legacy trusted-internal strict Calvin multi-shard path for `tasks`.
///
/// This API serves multi-shard dispatch only.
/// Its strict branch delegates to [`dispatch_strict_atomic_tasks_to_calvin`],
/// while best-effort remains rejected because this helper has no non-atomic
/// dispatch implementation.
pub(crate) async fn dispatch_tasks_to_calvin(
    state: &SharedState,
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    cross_shard_mode: CrossShardTxnMode,
    position: TxnDispatchPosition,
    reads: &[ReadSetEntry],
    lock_owner: Option<nodedb_cluster::calvin::types::TxnIdWire>,
) -> crate::Result<Option<Response>> {
    let read_vshards = read_vshards_of(reads)?;
    match classify_dispatch(tasks, &read_vshards) {
        DispatchClass::MultiShard { .. } => {
            admit_legacy_multi_shard_dispatch(cross_shard_mode, position)?;
            dispatch_strict_atomic_tasks_to_calvin(
                state,
                tasks,
                tenant_id,
                position,
                reads,
                lock_owner,
                TxnProvenance::client(),
            )
            .await
        }
        DispatchClass::SingleShard { .. } => Err(crate::Error::Internal {
            detail: "unexpected single-shard classification on the strict multi-shard Calvin path"
                .to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::read_set::{EngineTag, ReadKey, ReadOrigin};
    use crate::types::{DatabaseId, KeyRepr, VShardId};
    use nodedb_cluster::calvin::types::TxnIdWire;
    use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan};
    use nodedb_physical::physical_task::PostSetOp;
    use nodedb_types::{CollectionKey, Surrogate};

    fn task(collection: &str, surrogate: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: CollectionKey::from_bare(DatabaseId::DEFAULT, collection).vshard(),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
                document_id: format!("d{surrogate}"),
                surrogate: Surrogate::new(surrogate),
                value: Vec::new(),
                if_absent: false,
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    fn read(collection: &str) -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Document,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: collection.to_owned(),
            key: ReadKey::Point {
                repr: KeyRepr::Surrogate(1),
            },
            read_version: nodedb_types::WriteVersion::logged(0, 1),
            origin: ReadOrigin::Session,
            home: None,
        }
    }

    fn distinct_collection(from: &str) -> String {
        let home = CollectionKey::from_bare(DatabaseId::DEFAULT, from).vshard();
        (0..1024)
            .map(|index| format!("atomic_{index}"))
            .find(|candidate| {
                CollectionKey::from_bare(DatabaseId::DEFAULT, candidate).vshard() != home
            })
            .expect("test routing domain must contain more than one vShard")
    }

    fn distinct_key_pair() -> (String, String) {
        let first = "graph_src".to_owned();
        let first_home = VShardId::from_key(first.as_bytes());
        let second = (0..1024)
            .map(|index| format!("graph_dst_{index}"))
            .find(|candidate| VShardId::from_key(candidate.as_bytes()) != first_home)
            .expect("test routing domain must contain more than one vShard");
        (first, second)
    }

    fn edge_task(src_id: String, dst_id: String) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::from_key(src_id.as_bytes()),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "edges"),
                src_id,
                label: "links".to_owned(),
                dst_id,
                properties: Vec::new(),
                src_surrogate: Surrogate::new(1),
                dst_surrogate: Surrogate::new(2),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn admission_rejects_empty_task_set() {
        let error =
            admit_strict_atomic_tasks(&[], TenantId::new(1), TxnDispatchPosition::Autocommit, &[])
                .expect_err("empty strict atomic task set must fail");
        assert!(matches!(error, crate::Error::BadRequest { .. }));
    }

    #[test]
    fn admission_rejects_mid_block_single_and_multi_participant_sets() {
        let single = [task("single", 1)];
        let second_collection = distinct_collection("single");
        let multi = [task("single", 1), task(&second_collection, 2)];
        for tasks in [&single[..], &multi[..]] {
            let error = admit_strict_atomic_tasks(
                tasks,
                TenantId::new(1),
                TxnDispatchPosition::MidBlockStatement,
                &[],
            )
            .expect_err("mid-block strict atomic task set must fail");
            assert!(matches!(
                error,
                crate::Error::CrossShardInExplicitTransaction
            ));
        }
    }

    #[test]
    fn admission_builds_single_and_multi_document_sets_and_preserves_read_widening() {
        let first = task("single", 1);
        let second_collection = distinct_collection("single");
        let second = task(&second_collection, 2);

        let single = admit_strict_atomic_tasks(
            std::slice::from_ref(&first),
            TenantId::new(1),
            TxnDispatchPosition::Autocommit,
            &[],
        )
        .expect("single participant must build through the universal constructor");
        assert_eq!(single.participating_vshards().len(), 1);

        let multi = admit_strict_atomic_tasks(
            &[first.clone(), second],
            TenantId::new(1),
            TxnDispatchPosition::Autocommit,
            &[],
        )
        .expect("multi participant must build through the universal constructor");
        assert!(multi.participating_vshards().len() >= 2);

        let widened = admit_strict_atomic_tasks(
            &[first],
            TenantId::new(1),
            TxnDispatchPosition::Autocommit,
            &[read(&second_collection)],
        )
        .expect("a single-write-vShard batch must preserve read widening");
        assert!(
            widened.participating_vshards().len() >= 2,
            "the full read set must still widen the atomic transaction participants"
        );
        assert_eq!(
            widened.versioned_reads.len(),
            1,
            "the read-only participant must retain its OCC observation"
        );
    }

    #[test]
    fn admission_derives_both_graph_edge_write_homes() {
        let (src_id, dst_id) = distinct_key_pair();
        let src_home = VShardId::from_key(src_id.as_bytes());
        let dst_home = VShardId::from_key(dst_id.as_bytes());
        assert_ne!(src_home, dst_home);

        let transaction = admit_strict_atomic_tasks(
            &[edge_task(src_id, dst_id)],
            TenantId::new(1),
            TxnDispatchPosition::Autocommit,
            &[],
        )
        .expect("a dual-home graph edge must build as one atomic task set");

        assert_eq!(transaction.participating_vshards().len(), 2);
        assert!(transaction.participating_vshards().contains(&src_home));
        assert!(transaction.participating_vshards().contains(&dst_home));
    }

    #[test]
    fn admission_preserves_database_checks() {
        let mut other_database_task = task("single", 2);
        other_database_task.database_id = DatabaseId::new(7);
        let error = admit_strict_atomic_tasks(
            &[task("single", 1), other_database_task],
            TenantId::new(1),
            TxnDispatchPosition::Autocommit,
            &[],
        )
        .expect_err("cross-database tasks must fail");
        assert!(matches!(error, crate::Error::BadRequest { .. }));

        let mut foreign_read = read("single");
        foreign_read.database_id = DatabaseId::new(7);
        let error = admit_strict_atomic_tasks(
            &[task("single", 1)],
            TenantId::new(1),
            TxnDispatchPosition::Autocommit,
            &[foreign_read],
        )
        .expect_err("cross-database reads must fail");
        assert!(matches!(error, crate::Error::BadRequest { .. }));
    }

    #[test]
    fn legacy_multi_shard_admission_rejects_mid_block_before_mode_branch() {
        for mode in [
            CrossShardTxnMode::Strict,
            CrossShardTxnMode::BestEffortNonAtomic,
        ] {
            let error =
                admit_legacy_multi_shard_dispatch(mode, TxnDispatchPosition::MidBlockStatement)
                    .expect_err(
                        "legacy mid-block multi-shard dispatch must reject before mode handling",
                    );
            assert!(matches!(
                error,
                crate::Error::CrossShardInExplicitTransaction
            ));
        }

        let error = admit_legacy_multi_shard_dispatch(
            CrossShardTxnMode::BestEffortNonAtomic,
            TxnDispatchPosition::Autocommit,
        )
        .expect_err("best-effort legacy dispatch retains its prior internal rejection");
        assert!(matches!(error, crate::Error::Internal { .. }));
    }

    #[test]
    fn selected_transaction_class_preserves_lock_owner() {
        let owner = TxnIdWire {
            epoch: 4,
            position: 2,
        };
        let mut tx = admit_strict_atomic_tasks(
            &[task("single", 1)],
            TenantId::new(1),
            TxnDispatchPosition::Autocommit,
            &[],
        )
        .expect("single participant must build");
        tx.set_lock_owner(Some(owner));
        assert_eq!(tx.lock_owner, Some(owner));
    }
}
