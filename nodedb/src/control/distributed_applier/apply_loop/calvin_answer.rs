// SPDX-License-Identifier: BUSL-1.1

//! The answer an installed Calvin slice that holds a `RETURNING` contract
//! gives its coordinator.
//!
//! A slice of the contract voted commit only once its frozen reply was
//! stored, and that entry precedes the slice's redo in the log. So every
//! replica holds the reply when the redo installs, and a slice with no
//! stored reply holds no plan of the contract.
//!
//! - A redo whose reply is `CalvinReplySpec::Frozen` answers the stored
//!   reply's encoded `CalvinFrozenReply`. No replica evaluates again.
//! - Any other redo answers what its install answered.

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::control::cluster::calvin::scheduler::frozen_replies;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::state::SharedState;
use crate::wal::CalvinPositionId;

/// The installed slice an answer is for.
pub(super) struct AnsweredSlice {
    pub at: CalvinPositionId,
    /// The slice answers rows, not a count.
    pub returning: bool,
    /// The redo answers the slice's stored frozen reply.
    pub frozen: bool,
}

/// `reply`, the install's answer for `slice`, as the coordinator reads it.
/// A frozen reply this replica cannot read leaves the install `Ok` with the
/// error in `error_code`: the record installed.
pub(super) fn contract_answer(
    state: &SharedState,
    slice: AnsweredSlice,
    mut reply: Response,
) -> Response {
    // A mirror's position names no txn of this cluster, and a reply that
    // failed keeps its error.
    if slice.at.mirror_database.is_some()
        || !slice.returning
        || !slice.frozen
        || reply.status != Status::Ok
        || reply.error_code.is_some()
    {
        return reply;
    }
    let txn = TxnId::new(slice.at.epoch, slice.at.position);
    let vshard_id = slice.at.vshard;
    let stored = match frozen_replies::load(state.credentials.catalog(), vshard_id, txn) {
        Ok(stored) => stored,
        Err(e) => {
            crate::diag::calvin_frozen_reply_store_failed(
                vshard_id,
                Some((txn.epoch, txn.position)),
                "load",
                &e,
            );
            reply.error_code = Some(Box::new(ErrorCode::Internal {
                detail: format!("calvin RETURNING: the frozen reply does not read: {e}"),
            }));
            return reply;
        }
    };
    let Some(frozen) = stored else {
        reply.error_code = Some(Box::new(ErrorCode::Internal {
            detail: format!(
                "calvin RETURNING: the frozen reply of txn ({}, {}) on vShard {vshard_id} \
                 is not stored on this replica",
                txn.epoch, txn.position
            ),
        }));
        return reply;
    };
    match frozen.to_bytes() {
        Ok(bytes) => reply.payload = Payload::from_vec(bytes),
        Err(e) => reply.error_code = Some(Box::new(ErrorCode::from(e))),
    }
    reply
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::wal_replication::types::{CalvinFrozenReply, FrozenPlanReply};
    use crate::types::{Lsn, RequestId};
    use crate::wal::WalManager;

    const VSHARD: u32 = 4;

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        (
            SharedState::new(dispatcher, wal).expect("shared state"),
            dir,
        )
    }

    fn installed(payload: Vec<u8>) -> Response {
        Response {
            request_id: RequestId::new(0),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(payload),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            stage_vote: None,
            read_vshards: crate::types::ReadShards::new(),
            write_set: Vec::new(),
        }
    }

    fn slice(epoch: u64, frozen: bool) -> AnsweredSlice {
        AnsweredSlice {
            at: CalvinPositionId {
                mirror_database: None,
                vshard: VSHARD,
                epoch,
                position: 0,
            },
            returning: true,
            frozen,
        }
    }

    fn frozen(epoch: u64) -> CalvinFrozenReply {
        CalvinFrozenReply {
            epoch,
            position: 0,
            vshard: VSHARD,
            plans: vec![FrozenPlanReply {
                plan_index: 0,
                payload: vec![0x93],
                column_types: vec!["text".into()],
                sequence_effects: Vec::new(),
                rejected: None,
                deferred_currval: Vec::new(),
            }],
        }
    }

    /// The install answers the stored frozen bytes, whatever it rendered.
    #[test]
    fn a_frozen_install_answers_the_stored_reply() {
        let (state, _dir) = state();
        frozen_replies::store(state.credentials.catalog(), &frozen(7)).expect("store");
        let reply = contract_answer(&state, slice(7, true), installed(Vec::new()));
        assert!(reply.error_code.is_none());
        assert_eq!(
            CalvinFrozenReply::from_bytes(reply.payload.as_bytes()).expect("decode"),
            frozen(7)
        );
    }

    /// A redo that answers no frozen reply keeps what its install answered.
    #[test]
    fn a_slice_whose_redo_is_not_frozen_keeps_its_answer() {
        let (state, _dir) = state();
        frozen_replies::store(state.credentials.catalog(), &frozen(8)).expect("store");
        let reply = contract_answer(&state, slice(8, false), installed(vec![0x90]));
        assert!(reply.error_code.is_none());
        assert_eq!(reply.payload.as_bytes(), &[0x90]);
    }

    #[test]
    fn a_slice_with_no_contract_keeps_its_rows_and_a_missing_reply_fails() {
        let (state, _dir) = state();
        let plain = contract_answer(&state, slice(9, false), installed(vec![0x90]));
        assert_eq!(plain.payload.as_bytes(), &[0x90]);
        let missing = contract_answer(&state, slice(9, true), installed(Vec::new()));
        assert!(matches!(
            missing.error_code.as_deref(),
            Some(ErrorCode::Internal { .. })
        ));
    }
}
