// SPDX-License-Identifier: BUSL-1.1

//! A single-core commit driver: stage, resolve, and install a transaction.

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Request, Response, Status};
use nodedb::control::wal_replication::transaction_redo::collections::written_collections;
use nodedb::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::Lsn;
use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::tx_batch_helpers::make_request;

fn send_request(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    request: Request,
) -> Response {
    tx.try_push(BridgeRequest::unfloored(request)).unwrap();
    core.tick();
    rx.try_pop().unwrap().inner
}

/// Commit `plans` as one transaction the way a session COMMIT does on its
/// core: stage each plan under the transaction `lsn` names, resolve the
/// transaction into its redo record, and install the record at `lsn`.
///
/// Returns the install's response, or the first staging or resolve refusal.
/// The staging overlay is released on every path.
pub fn commit_plans(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    plans: Vec<PhysicalPlan>,
    lsn: u64,
) -> Response {
    let txn_id = nodedb_types::id::TxnId::new(lsn);
    let in_txn = |plan: PhysicalPlan| {
        let mut request = make_request(plan);
        request.txn_id = Some(txn_id);
        request
    };
    let mut refusal = None;
    for plan in &plans {
        let staged = send_request(
            core,
            tx,
            rx,
            in_txn(PhysicalPlan::Meta(MetaOp::StageWrite {
                plan: Box::new(plan.clone()),
            })),
        );
        if staged.status != Status::Ok {
            refusal = Some(staged);
            break;
        }
    }
    let resolved = match refusal {
        Some(refusal) => Err(refusal),
        None => {
            let resolved = send_request(
                core,
                tx,
                rx,
                in_txn(PhysicalPlan::Meta(MetaOp::ResolveTxn {
                    txn_id,
                    plans: plans.clone(),
                })),
            );
            if resolved.status == Status::Ok {
                Ok(resolved.payload.to_vec())
            } else {
                Err(resolved)
            }
        }
    };
    let response = match resolved {
        Ok(redo) => {
            let mut install = make_request(PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
                redo,
                collections: written_collections(&plans),
                sum_targets: redo_sum_targets(&plans),
                origin: nodedb_physical::physical_plan::RedoOrigin::Commit,
            }));
            install.wal_lsn = Some(Lsn::new(lsn));
            send_request(core, tx, rx, install)
        }
        Err(refusal) => refusal,
    };
    // A session COMMIT releases the overlay once the install answered.
    let dropped = send_request(
        core,
        tx,
        rx,
        in_txn(PhysicalPlan::Meta(MetaOp::DropTxnOverlay { txn_id })),
    );
    assert_eq!(
        dropped.status,
        Status::Ok,
        "drop overlay: {:?}",
        dropped.error_code
    );
    response
}
