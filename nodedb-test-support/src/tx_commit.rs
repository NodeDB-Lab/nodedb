// SPDX-License-Identifier: BUSL-1.1

//! A single-core commit driver: stage, resolve, and install a transaction.

use nodedb::bridge::dispatch::{BridgeRequest, BridgeResponse};
use nodedb::bridge::envelope::{Request, Response, Status};
use nodedb::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::types::Lsn;
use nodedb::wal::RedoRecord;
use nodedb::wal::redo::frame::{RedoFrameTag, RedoFrames, decode_frame};
use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, ResolvedTxnPart, written_collections};

use crate::tx_batch_helpers::make_request;

/// The part bound the driver resolves with: small, so a test redo of a few
/// rows already reads in several parts.
const RESOLVE_PART_BYTES: u64 = 512;

/// The encoded redo record the framed parts of a resolve hold.
fn redo_of_parts(framed: &[u8]) -> Vec<u8> {
    let mut record = RedoRecord {
        version: 1,
        ops: Vec::new(),
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: Vec::new(),
    };
    for frame in RedoFrames::new(framed) {
        let (tag, body) = frame.expect("resolve frame");
        match tag {
            RedoFrameTag::Op => record.ops.push(decode_frame(tag, body).expect("op")),
            RedoFrameTag::RowChange => record
                .row_changes
                .push(decode_frame(tag, body).expect("row change")),
            RedoFrameTag::RowSource => record
                .row_sources
                .push(decode_frame(tag, body).expect("row source")),
            other => panic!("a resolve frames no {other:?} record"),
        }
    }
    record.to_bytes().expect("encode redo")
}

fn send_request(
    core: &mut CoreLoop,
    tx: &mut Producer<BridgeRequest>,
    rx: &mut Consumer<BridgeResponse>,
    request: Request,
) -> Response {
    tx.try_push(BridgeRequest::unfloored(request))
        .expect("push the request onto the bridge");
    core.tick();
    rx.try_pop()
        .expect("pop the response from the bridge")
        .inner
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
            let mut framed = Vec::new();
            let mut part = 0u32;
            loop {
                let resolved = send_request(
                    core,
                    tx,
                    rx,
                    in_txn(PhysicalPlan::Meta(MetaOp::ResolveTxnPart {
                        txn_id,
                        plans: if part == 0 { plans.clone() } else { Vec::new() },
                        part,
                        max_part_bytes: RESOLVE_PART_BYTES,
                    })),
                );
                if resolved.status != Status::Ok {
                    break Err(resolved);
                }
                let answer: ResolvedTxnPart =
                    zerompk::from_msgpack(resolved.payload.as_bytes()).expect("decode part");
                framed.extend_from_slice(&answer.bytes);
                if answer.done {
                    break Ok(redo_of_parts(&framed));
                }
                part += 1;
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
                calvin: None,
                reads: Vec::new(),
                snapshot_ordinal: None,
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
