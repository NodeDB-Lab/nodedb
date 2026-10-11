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
    /// participants, each a plain affected-count write, and they coalesce:
    /// the first answer stands, with every participant's timeseries install
    /// counts merged in. Only two participants that each carry RETURNING
    /// rows conflict: a cross-shard RETURNING union is unsupported, so the
    /// statement fails loudly rather than return one shard's rows.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn deposit_reply(
        &self,
        txn_id: TxnId,
        reply: &Response,
        returning: bool,
    ) {
        use std::collections::hash_map::Entry;

        let response = statement_reply(reply.clone());
        let key = nodedb_cluster::calvin::TxnId::new(txn_id.epoch, txn_id.position);
        let vshard_id = self.vshard_id;
        self.shared
            .calvin
            .apply_results
            .deposit_with(key, |entry| match entry {
                Entry::Vacant(slot) => {
                    slot.insert(CalvinApplyResult::Single {
                        response,
                        has_returning: returning,
                    });
                }
                Entry::Occupied(mut slot) => {
                    let existing_returning = matches!(
                        slot.get(),
                        CalvinApplyResult::Single {
                            has_returning: true,
                            ..
                        }
                    );
                    let already_conflict = matches!(slot.get(), CalvinApplyResult::Conflict);
                    if already_conflict {
                        // A RETURNING union was already recorded; it stays one.
                    } else if returning && existing_returning {
                        tracing::error!(
                            epoch = txn_id.epoch,
                            position = txn_id.position,
                            vshard = vshard_id,
                            "two RETURNING-bearing participants for one Calvin txn — cross-shard \
                         RETURNING union unsupported"
                        );
                        slot.insert(CalvinApplyResult::Conflict);
                    } else if returning {
                        // The incoming participant carries the rows; the held
                        // entry was a plain affected-count sibling. Rows win.
                        slot.insert(CalvinApplyResult::Single {
                            response,
                            has_returning: true,
                        });
                    } else {
                        merge_install_counts(slot.get_mut(), &response);
                    }
                }
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
        CalvinAckResult::of(reply, slice, limit)
            .to_bytes()
            .unwrap_or_else(|error| {
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    %error,
                    "calvin: the completion ack carries no report"
                );
                Vec::new()
            })
    }
}

/// Fold the timeseries install counts `incoming` answers into the plain
/// result `held`. A RETURNING result keeps its rows untouched.
fn merge_install_counts(held: &mut CalvinApplyResult, incoming: &Response) {
    let CalvinApplyResult::Single {
        response,
        has_returning: false,
    } = held
    else {
        return;
    };
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
    /// RETURNING result keeps its rows.
    #[test]
    fn plain_participants_merge_their_install_counts() {
        use crate::engine::timeseries::install_counts::TsInstallCounts;
        let mut held = CalvinApplyResult::Single {
            response: installed_with_counts("cpu", 2, 1),
            has_returning: false,
        };
        merge_install_counts(&mut held, &installed_with_counts("mem", 3, 2));
        let CalvinApplyResult::Single { response, .. } = &held else {
            panic!("a merged result stays single");
        };
        let totals = TsInstallCounts::from_payload(response.payload.as_bytes())
            .expect("install counts")
            .by_collection();
        assert_eq!(totals.get("cpu"), Some(&(2, 1)));
        assert_eq!(totals.get("mem"), Some(&(3, 2)));

        let rows = crate::bridge::envelope::Payload::from_vec(vec![0x90]);
        let mut returning = CalvinApplyResult::Single {
            response: Response {
                payload: rows,
                ..installed_with_counts("cpu", 1, 0)
            },
            has_returning: true,
        };
        merge_install_counts(&mut returning, &installed_with_counts("mem", 1, 1));
        let CalvinApplyResult::Single { response, .. } = &returning else {
            panic!("a RETURNING result stays single");
        };
        assert_eq!(response.payload.as_bytes(), &[0x90]);
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
