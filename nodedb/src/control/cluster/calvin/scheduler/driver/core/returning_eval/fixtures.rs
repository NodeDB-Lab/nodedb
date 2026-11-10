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
