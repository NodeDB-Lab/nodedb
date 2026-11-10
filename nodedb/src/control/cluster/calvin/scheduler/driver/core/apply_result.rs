// SPDX-License-Identifier: BUSL-1.1

//! What a committed slice's install answers to its coordinator: the local
//! applied-result sidecar entry, and the result its `CompletionAck` carries.

use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::state::{AckSlice, CalvinAckResult, CalvinApplyResult};

use super::scheduler::Scheduler;

impl Scheduler {
    /// Deposit a primary slice's applied `reply` into this node's sidecar,
    /// before its `CompletionAck` fires the coordinator's waiter.
    ///
    /// A multi-collection cross-shard COMMIT has many primary-write
    /// participants, each a plain affected-count write. This node's sidecar
    /// holds only the participants it hosts, so it keeps the first plain
    /// answer as the response the coordinator reshapes. The coordinator takes
    /// the statement's count from every participant's `CompletionAck`
    /// (`with_reported_results`). A slice that carries `RETURNING` keeps its
    /// own answer under its vShard, so the coordinator answers the rows of
    /// every such participant.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn deposit_reply(
        &self,
        txn_id: TxnId,
        reply: &Response,
        returning: bool,
    ) {
        let response = statement_reply(reply.clone());
        let key = nodedb_cluster::calvin::TxnId::new(txn_id.epoch, txn_id.position);
        let vshard_id = self.vshard_id;
        self.shared.calvin.apply_results.deposit_with(key, |entry| {
            deposit_into(entry.or_default(), vshard_id, response, returning);
        });
    }

    /// The result a slice's `CompletionAck` carries for its applied `reply`:
    /// the install's timeseries counts, and the rows of a RETURNING slice.
    /// The rows are bounded by the query result limit and by the ack's
    /// frame budget, whichever is lower. Every replica renders the same
    /// reply, so every replica owes the same bytes.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn ack_result_of(
        &self,
        reply: &Response,
        slice: AckSlice,
    ) -> Vec<u8> {
        let limit = self
            .shared
            .tuning
            .network
            .max_query_result_bytes
            .min(crate::control::state::ACK_ROWS_FRAME_BUDGET);
        CalvinAckResult::of(reply, slice, limit).to_bytes_or_failed()
    }
}

/// Add `vshard_id`'s applied `response` to `held`. A `RETURNING` answer
/// keeps its own place, and a second deposit of the same vShard changes
/// nothing. A plain answer folds into the plain answer held.
fn deposit_into(held: &mut CalvinApplyResult, vshard_id: u32, response: Response, returning: bool) {
    if returning {
        held.returning.entry(vshard_id).or_insert(response);
        return;
    }
    match &mut held.plain {
        Some(plain) => merge_install_counts(plain, &response),
        None => held.plain = Some(response),
    }
}

/// Fold the timeseries install counts `incoming` answers into the plain
/// answer `response`.
fn merge_install_counts(response: &mut Response, incoming: &Response) {
    if let Some(merged) = crate::engine::timeseries::install_counts::merge_count_payloads(
        response.payload.as_bytes(),
        incoming.payload.as_bytes(),
    ) {
        response.payload = crate::bridge::envelope::Payload::from_vec(merged);
    }
}

/// The response the statement drains. An install whose reply failed to
/// render answers `Ok` with the render error in `error_code`. The statement
/// reports that error.
fn statement_reply(mut response: Response) -> Response {
    if response.status == Status::Ok && response.error_code.is_some() {
        response.status = Status::Error;
        response.payload = crate::bridge::envelope::Payload::empty();
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        error_response, staged_response,
    };

    fn installed_with_counts(collection: &str, accepted: u64, rejected: u64) -> Response {
        use crate::engine::timeseries::install_counts::{TsInstallCount, TsInstallCounts};
        let mut response = staged_response(Status::Ok, None);
        response.payload = crate::bridge::envelope::Payload::from_vec(
            TsInstallCounts::new(vec![TsInstallCount {
                collection: collection.into(),
                accepted,
                rejected,
            }])
            .to_bytes()
            .expect("encode install counts"),
        );
        response
    }

    /// Two plain participants of one Calvin transaction each installed a
    /// timeseries batch. The coordinator's result carries both installs'
    /// apply counts, so COMMIT reports every participant's rejected rows. A
    /// RETURNING answer keeps its rows beside them.
    #[test]
    fn plain_participants_merge_their_install_counts() {
        use crate::engine::timeseries::install_counts::TsInstallCounts;
        let mut held = CalvinApplyResult::default();
        deposit_into(&mut held, 1, installed_with_counts("cpu", 2, 1), false);
        deposit_into(&mut held, 2, installed_with_counts("mem", 3, 2), false);
        let rows = Response {
            payload: crate::bridge::envelope::Payload::from_vec(vec![0x90]),
            ..installed_with_counts("cpu", 1, 0)
        };
        deposit_into(&mut held, 3, rows, true);
        let plain = held.plain.as_ref().expect("a plain answer");
        let totals = TsInstallCounts::from_payload(plain.payload.as_bytes())
            .expect("install counts")
            .by_collection();
        assert_eq!(totals.get("cpu"), Some(&(2, 1)));
        assert_eq!(totals.get("mem"), Some(&(3, 2)));
        assert_eq!(
            held.returning
                .get(&3)
                .map(|r| r.payload.as_bytes().to_vec()),
            Some(vec![0x90])
        );
    }

    /// Each participant that carries `RETURNING` keeps its answer under its
    /// vShard. A second deposit of the same vShard changes nothing.
    #[test]
    fn every_returning_participant_keeps_its_answer() {
        let answer = |byte: u8| Response {
            payload: crate::bridge::envelope::Payload::from_vec(vec![byte]),
            ..staged_response(Status::Ok, None)
        };
        let mut held = CalvinApplyResult::default();
        deposit_into(&mut held, 4, answer(0x91), true);
        deposit_into(&mut held, 2, answer(0x92), true);
        deposit_into(&mut held, 4, answer(0x93), true);
        let kept: Vec<(u32, Vec<u8>)> = held
            .returning
            .iter()
            .map(|(vshard, r)| (*vshard, r.payload.as_bytes().to_vec()))
            .collect();
        assert_eq!(kept, vec![(2, vec![0x92]), (4, vec![0x91])]);
        assert!(held.plain.is_none());
    }

    /// A render error on an installed slice reaches the statement as a
    /// typed error.
    #[test]
    fn a_render_error_on_an_installed_slice_becomes_the_statement_error() {
        let mut response = error_response(ErrorCode::Internal {
            detail: "render".to_string(),
        });
        response.status = Status::Ok;

        let reply = statement_reply(response);

        assert_eq!(reply.status, Status::Error);
        assert!(matches!(
            reply.error_code.as_deref(),
            Some(ErrorCode::Internal { .. })
        ));
    }
}
