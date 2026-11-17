// SPDX-License-Identifier: BUSL-1.1

//! Shared builders for the `RETURNING` evaluation tests.

use std::sync::Arc;

use nodedb_cluster::calvin::types::{
    PlanReturning, ReturningContract, ReturningContractLimits, SequencedTxn, TxClass,
};
use nodedb_query::expr::{BinaryOp, SqlExpr};
use nodedb_types::{DatabaseId, ReturningColumns, ReturningItem, ReturningSpec, TenantId, Value};

use crate::control::security::catalog::sequence_types::StoredSequence;
use crate::control::server::response_shape::types::ShapedRow;
use crate::control::server::shared::returning::precommit::{
    PreparedReturningRow, PreparedRowIdentity,
};
use crate::control::state::SharedState;

/// The collection every fixture txn writes, in its stored form.
const COLLECTION: &str = "test_coll";
/// The tenant of every fixture txn.
const TENANT: TenantId = TenantId::new(1);

fn computed(name: &str, expression: SqlExpr) -> ReturningItem {
    ReturningItem::Computed {
        name: name.into(),
        expression: zerompk::to_msgpack_vec(&expression).expect("encode expression"),
    }
}

/// An item `n` that draws `nextval(sequence)`.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn nextval(
    sequence: &str,
) -> ReturningItem {
    computed(
        "n",
        SqlExpr::Function {
            name: "nextval".into(),
            args: vec![SqlExpr::Literal(Value::String(sequence.into()))],
        },
    )
}

/// An item `bad` that evaluates `10 / 0`.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn division_by_zero()
-> ReturningItem {
    computed(
        "bad",
        SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Literal(Value::Integer(10))),
            op: BinaryOp::Div,
            right: Box::new(SqlExpr::Literal(Value::Integer(0))),
        },
    )
}

/// A contract whose one entry, plan 0, answers `items` with at most `rows`
/// rows.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn contract(
    items: Vec<ReturningItem>,
    rows: u64,
) -> ReturningContract {
    ReturningContract {
        plans: vec![PlanReturning {
            plan_index: 0,
            collection: COLLECTION.into(),
            spec: ReturningSpec {
                columns: ReturningColumns::Named(items),
                precommit: None,
            },
            source_columns: Vec::new(),
            limits: ReturningContractLimits {
                rows,
                columns: 100,
                cells: 10_000,
                payload_bytes: 1 << 20,
            },
            announced: None,
            vshards: Vec::new(),
            sequence_after: Vec::new(),
        }],
        roles: Vec::new(),
        sequence_values: Vec::new(),
    }
}

/// A class of the fixture txn's shape that carries `contract`.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn txn_class(
    contract: ReturningContract,
) -> Arc<TxClass> {
    with_contract(
        super::super::test_support::make_sequenced_txn(1, 0),
        contract,
    )
    .tx_class
}

/// `txn` with `contract` on its class.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn with_contract(
    mut txn: SequencedTxn,
    contract: ReturningContract,
) -> SequencedTxn {
    Arc::make_mut(&mut txn.tx_class).set_returning(Some(contract));
    txn
}

/// `count` prepared rows of the fixture collection, each with its own key.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn image_rows(
    count: usize,
) -> Vec<PreparedReturningRow> {
    (0..count)
        .map(|index| PreparedReturningRow {
            identity: PreparedRowIdentity {
                collection: COLLECTION.into(),
                stable_row_key: format!("k{index}").into_bytes(),
                operation_index: index as u64,
            },
            fields: ShapedRow::from([("v".into(), Value::Integer(index as i64))]),
        })
        .collect()
}

/// Create `name` in the node's sequence registry, in the fixture tenant.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn create_sequence(
    shared: &SharedState,
    name: &str,
) {
    shared
        .sequence_registry
        .create(StoredSequence::new(
            DatabaseId::DEFAULT.as_u64(),
            TENANT.as_u64(),
            name.into(),
            "alice".into(),
        ))
        .expect("create sequence");
}

/// Draw the next value of `name`, as a later statement does.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn sequence_next(
    shared: &SharedState,
    name: &str,
) -> i64 {
    shared
        .sequence_registry
        .nextval(DatabaseId::DEFAULT.as_u64(), TENANT.as_u64(), name)
        .expect("nextval")
}

/// The committed `CalvinFrozenReply` entry `scheduler` proposed for
/// `txn_id`, read from its data-group log. Ticks the single-voter group
/// until the entry commits.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn proposed_frozen_reply(
    scheduler: &super::super::scheduler::Scheduler,
    txn_id: crate::control::cluster::calvin::scheduler::lock_manager::TxnId,
) -> crate::control::wal_replication::types::CalvinFrozenReply {
    use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
    let (group_id, index) = scheduler
        .pending
        .get(&txn_id)
        .and_then(|pending| pending.returning.frozen_entry.as_ref())
        .and_then(|owed| owed.proposed)
        .expect("the frozen-reply entry is in the log");
    let mut mr = scheduler
        .multi_raft
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    for _ in 0..20 {
        let entries = mr
            .read_committed_entries(group_id, index, index)
            .expect("read the group's log");
        if let Some(entry) = entries.into_iter().find(|entry| entry.index == index) {
            let entry = ReplicatedEntry::from_bytes(&entry.data).expect("a replicated entry");
            return match entry.write {
                ReplicatedWrite::CalvinFrozenReply { reply } => reply,
                other => panic!("expected a frozen reply entry, got {other:?}"),
            };
        }
        mr.wait_all_durable_blocking().expect("durable");
        mr.tick().expect("tick");
    }
    panic!("the frozen-reply entry at {index} never committed");
}

/// Apply `txn_id`'s proposed frozen-reply entry on the fixture node, as the
/// data-group apply loop does: store it and wake the scheduler.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn apply_frozen_reply(
    scheduler: &super::super::scheduler::Scheduler,
    txn_id: crate::control::cluster::calvin::scheduler::lock_manager::TxnId,
) -> crate::control::wal_replication::types::CalvinFrozenReply {
    let reply = proposed_frozen_reply(scheduler, txn_id);
    crate::control::cluster::calvin::scheduler::frozen_replies::store(
        scheduler.shared.credentials.catalog(),
        &reply,
    )
    .expect("store the frozen reply");
    scheduler
        .shared
        .calvin
        .frozen_replies
        .vshard(scheduler.vshard_id)
        .wake();
    reply
}

/// The txn the frozen-reply tests stage.
pub(in crate::control::cluster::calvin::scheduler::driver::core) const FROZEN_TXN:
    crate::control::cluster::calvin::scheduler::lock_manager::TxnId =
    crate::control::cluster::calvin::scheduler::lock_manager::TxnId {
        epoch: 3,
        position: 0,
    };

/// A leader of the `test_coll` vShard holding `FROZEN_TXN` staged, with a
/// contract that draws `nextval('s')` per row.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn frozen_leader(
    registry: &Arc<nodedb_cluster::calvin::CalvinCompletionRegistry>,
) -> (
    super::super::scheduler::Scheduler,
    tempfile::TempDir,
    crate::bridge::dispatch::CoreChannelDataSide,
) {
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_local_write_txn, test_coll_vshard,
    };
    let (mut scheduler, dir, data_side) =
        build_test_scheduler_with_data_side(test_coll_vshard(), Arc::clone(registry));
    crate::control::cluster::calvin::scheduler::driver::core::test_proposer::lead_data_group(
        &mut scheduler,
    );
    create_sequence(&scheduler.shared, "s");
    let txn = with_contract(make_local_write_txn(3, 0), contract(vec![nextval("s")], 10));
    scheduler.process_scheduler_input(nodedb_cluster::calvin::types::SchedulerInput::Txn(
        Box::new(txn),
    ));
    (scheduler, dir, data_side)
}

/// Whether a reply of `txn` on `scheduler`'s vShard is stored.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn stores_frozen_reply(
    scheduler: &super::super::scheduler::Scheduler,
    txn: crate::control::cluster::calvin::scheduler::lock_manager::TxnId,
) -> bool {
    crate::control::cluster::calvin::scheduler::frozen_replies::holds(
        scheduler.shared.credentials.catalog(),
        scheduler.vshard_id,
        txn,
    )
    .expect("holds")
}

/// A stored reply of `txn` on `vshard` that holds plan 0 with no rows, as
/// an earlier leader's evaluation of the fixture contract stores it.
pub(in crate::control::cluster::calvin::scheduler::driver::core) fn stored_plan_zero(
    vshard: u32,
    txn: crate::control::cluster::calvin::scheduler::lock_manager::TxnId,
) -> crate::control::wal_replication::types::CalvinFrozenReply {
    crate::control::wal_replication::types::CalvinFrozenReply {
        epoch: txn.epoch,
        position: txn.position,
        vshard,
        plans: vec![crate::control::wal_replication::types::FrozenPlanReply {
            plan_index: 0,
            payload: zerompk::to_msgpack_vec(&crate::data::executor::response_codec::RowsPayload {
                columns: vec!["n".into()],
                rows: Vec::new(),
            })
            .expect("encode rows"),
            column_types: vec!["int8".into()],
            sequence_effects: Vec::new(),
            rejected: None,
        }],
    }
}
