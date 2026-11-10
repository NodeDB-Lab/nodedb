// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` answer of a completed Calvin statement, from every
//! participant that carries `RETURNING`.
//!
//! Each such participant answers in its `CompletionAck`, and this node's
//! sidecar holds the answer of each one it hosts a replica of. One
//! participant answers with its own rows. Several answer with their frozen
//! replies, and the statement answers the union of them in statement order,
//! as the per-task path appends each task's rows.

use std::collections::{BTreeMap, BTreeSet};

use super::local::synthetic_returning_response;
use crate::Error;
use crate::bridge::envelope::Response;
use crate::control::state::AckReturning;
use crate::control::wal_replication::types::CalvinFrozenReply;

/// The statement's `RETURNING` answer: `local`, the answers this node's
/// replicas deposited by vShard, and `reported`, every participant's acked
/// answer. `None` when no participant carries `RETURNING`.
///
/// A local answer stands for its own vShard's report, and the local
/// response path enforces the result limit on it. A reported answer past
/// the limit fails the statement with the error a local `RETURNING` over the
/// limit gives, unless a local answer of its vShard stands for it.
pub(super) fn returning_answer(
    mut local: BTreeMap<u32, Response>,
    reported: &[&AckReturning],
) -> crate::Result<Option<Response>> {
    if reported.len() <= 1 && local.len() <= 1 {
        return one_answer(local, reported.first().copied());
    }
    let mut replies = Vec::with_capacity(reported.len().max(local.len()));
    let mut base: Option<Response> = None;
    for report in reported {
        match report {
            AckReturning::Rows { rows } => replies.push(frozen_reply(rows)?),
            AckReturning::OverLimit {
                vshard,
                bytes,
                limit,
            } => {
                let response = local
                    .remove(vshard)
                    .ok_or_else(|| over_limit_error(*bytes, *limit))?;
                replies.push(frozen_reply(response.payload.as_bytes())?);
                base.get_or_insert(response);
            }
            AckReturning::Failed { detail } => return Err(failed(detail)),
        }
    }
    // A local answer of a vShard that reported rows holds the same bytes. One
    // whose vShard reported nothing still answers its rows.
    let reported_vshards: BTreeSet<u32> = replies.iter().map(|reply| reply.vshard).collect();
    for (vshard, response) in local {
        if !reported_vshards.contains(&vshard) {
            replies.push(frozen_reply(response.payload.as_bytes())?);
        }
        base.get_or_insert(response);
    }
    let Some(union) = CalvinFrozenReply::union(replies)? else {
        return Ok(None);
    };
    let payload = crate::bridge::envelope::Payload::from_vec(union.to_bytes()?);
    Ok(Some(match base {
        Some(response) => Response {
            payload,
            ..response
        },
        None => Response {
            payload,
            ..synthetic_returning_response(Vec::new())
        },
    }))
}

/// The answer of a statement with at most one `RETURNING` participant: its
/// local answer when this node hosts it, else its report. A report past the
/// limit is answered only by the local answer of its own vShard.
fn one_answer(
    local: BTreeMap<u32, Response>,
    reported: Option<&AckReturning>,
) -> crate::Result<Option<Response>> {
    if let Some((local_vshard, response)) = local.into_iter().next() {
        return match reported {
            Some(AckReturning::OverLimit {
                vshard,
                bytes,
                limit,
            }) if *vshard != local_vshard => Err(over_limit_error(*bytes, *limit)),
            _ => Ok(Some(response)),
        };
    }
    match reported {
        None => Ok(None),
        Some(AckReturning::Rows { rows }) => Ok(Some(synthetic_returning_response(rows.clone()))),
        Some(AckReturning::OverLimit { bytes, limit, .. }) => Err(over_limit_error(*bytes, *limit)),
        Some(AckReturning::Failed { detail }) => Err(failed(detail)),
    }
}

/// The frozen reply `bytes` encode. Only a participant that holds a
/// `RETURNING` contract answers one, and only those join a union.
fn frozen_reply(bytes: &[u8]) -> crate::Result<CalvinFrozenReply> {
    CalvinFrozenReply::from_bytes(bytes).map_err(|error| Error::Internal {
        detail: format!(
            "calvin RETURNING: several participants answer rows, and one answer is no frozen \
             reply: {error}"
        ),
    })
}

fn over_limit_error(bytes: u64, limit: u64) -> Error {
    Error::ExecutionLimitExceeded {
        detail: format!("query result exceeded max_query_result_bytes ({bytes} > {limit} bytes)"),
    }
}

fn failed(detail: &str) -> Error {
    Error::Internal {
        detail: format!("calvin RETURNING: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{NativeCell, Value};

    use super::*;
    use crate::control::wal_replication::types::FrozenPlanReply;
    use crate::data::executor::response_codec::RowsPayload;

    /// The frozen reply of `vshard`, one plan per `(plan index, id)`.
    fn frozen(vshard: u32, plans: &[(u32, &str)]) -> CalvinFrozenReply {
        CalvinFrozenReply {
            epoch: 1,
            position: 0,
            vshard,
            plans: plans
                .iter()
                .map(|(plan_index, id)| FrozenPlanReply {
                    plan_index: *plan_index,
                    payload: zerompk::to_msgpack_vec(&RowsPayload {
                        columns: vec!["id".into()],
                        rows: vec![vec![NativeCell(Value::String((*id).into()))]],
                    })
                    .expect("encode rows"),
                    column_types: vec!["text".into()],
                    sequence_effects: Vec::new(),
                    rejected: None,
                    deferred_currval: Vec::new(),
                })
                .collect(),
        }
    }

    fn rows_of(reply: &CalvinFrozenReply) -> AckReturning {
        AckReturning::Rows {
            rows: reply.to_bytes().expect("encode reply"),
        }
    }

    fn plan_order(answer: &Response) -> Vec<(u32, Vec<u8>)> {
        CalvinFrozenReply::from_bytes(answer.payload.as_bytes())
            .expect("a frozen union")
            .plans
            .into_iter()
            .map(|plan| (plan.plan_index, plan.payload))
            .collect()
    }

    /// Two participants on other nodes answer the union of their plans,
    /// ascending by plan index.
    #[test]
    fn remote_participants_answer_their_union() {
        let (a, b) = (frozen(5, &[(1, "b")]), frozen(2, &[(0, "a"), (2, "c")]));
        let answer = returning_answer(BTreeMap::new(), &[&rows_of(&a), &rows_of(&b)])
            .expect("an answer")
            .expect("rows");
        let order: Vec<u32> = plan_order(&answer).into_iter().map(|(i, _)| i).collect();
        assert_eq!(order, vec![0, 1, 2]);
    }

    /// A local answer stands for its participant's report, once, and covers
    /// a report past the ack's limit.
    #[test]
    fn a_local_answer_stands_for_its_report() {
        let (a, b) = (frozen(1, &[(0, "a")]), frozen(4, &[(1, "b")]));
        let local = BTreeMap::from([
            (
                1,
                synthetic_returning_response(a.to_bytes().expect("encode")),
            ),
            (
                4,
                synthetic_returning_response(b.to_bytes().expect("encode")),
            ),
        ]);
        let over = AckReturning::OverLimit {
            vshard: 4,
            bytes: 64,
            limit: 16,
        };
        let answer = returning_answer(local, &[&rows_of(&a), &over])
            .expect("an answer")
            .expect("rows");
        assert_eq!(plan_order(&answer).len(), 2, "each plan answers once");
    }

    /// A report past the limit that no local answer stands for fails the
    /// statement with the result limit error. A failed report fails it.
    #[test]
    fn an_unanswered_report_fails_the_statement() {
        let a = frozen(1, &[(0, "a")]);
        let over = AckReturning::OverLimit {
            vshard: 9,
            bytes: 64,
            limit: 16,
        };
        match returning_answer(BTreeMap::new(), &[&rows_of(&a), &over]) {
            Err(Error::ExecutionLimitExceeded { detail }) => assert_eq!(
                detail,
                "query result exceeded max_query_result_bytes (64 > 16 bytes)"
            ),
            other => panic!("expected the result limit error, got {other:?}"),
        }
        let failed = AckReturning::Failed {
            detail: "render".into(),
        };
        assert!(returning_answer(BTreeMap::new(), &[&rows_of(&a), &failed]).is_err());
    }

    /// A report past the limit is answered only by the local answer of its
    /// own vShard. An unrelated local answer never stands for it, so the
    /// statement fails rather than skip the over-limit participant's rows.
    #[test]
    fn an_over_limit_report_needs_its_own_vshards_answer() {
        let (a, c) = (frozen(1, &[(0, "a")]), frozen(7, &[(2, "c")]));
        let unrelated = BTreeMap::from([(
            7,
            synthetic_returning_response(c.to_bytes().expect("encode")),
        )]);
        let over = AckReturning::OverLimit {
            vshard: 4,
            bytes: 64,
            limit: 16,
        };
        assert!(matches!(
            returning_answer(unrelated.clone(), &[&rows_of(&a), &over]),
            Err(Error::ExecutionLimitExceeded { .. })
        ));
        assert!(matches!(
            returning_answer(unrelated, &[&over]),
            Err(Error::ExecutionLimitExceeded { .. })
        ));
    }

    /// One participant answers its own bytes, whatever they hold.
    #[test]
    fn one_participant_answers_its_own_bytes() {
        let rows = AckReturning::Rows { rows: vec![0x90] };
        let answer = returning_answer(BTreeMap::new(), &[&rows])
            .expect("an answer")
            .expect("rows");
        assert_eq!(answer.payload.as_bytes(), &[0x90]);
        assert!(
            returning_answer(BTreeMap::new(), &[])
                .expect("no answer")
                .is_none()
        );
    }

    /// Several answers that are no frozen replies cannot form a union.
    #[test]
    fn several_unfrozen_answers_fail() {
        let rows = AckReturning::Rows { rows: vec![0x90] };
        assert!(returning_answer(BTreeMap::new(), &[&rows, &rows]).is_err());
    }
}
