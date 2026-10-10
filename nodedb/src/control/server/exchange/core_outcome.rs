// SPDX-License-Identifier: BUSL-1.1

//! The one merge rule every all-core gather applies to its per-core answers.
//!
//! Each core holds only the state its own vShards home to. A gather that
//! merges the cores that answered and skips a core that failed returns a
//! partial answer as a complete one. Every all-core gather therefore
//! classifies each core's response here and merges through
//! [`require_every_core`].

use crate::bridge::envelope::{Response, Status};

/// One core's answer: `Some` when the core answered, `None` when it refused
/// with `NotFound`, `Err` when it failed.
pub(crate) type CoreOutcome<T> = crate::Result<Option<T>>;

/// Classify one core's collected response.
///
/// - `NotFound` is `Ok(None)`: the core holds no slice of the target.
/// - An empty payload is `Ok(Some)`: the core answered with no rows.
/// - Every other error status is `Err` with the core's typed error.
pub(crate) fn classify_core_response(collected: crate::Result<Response>) -> CoreOutcome<Response> {
    let resp = collected?;
    if resp.status == Status::Error {
        crate::control::local_dispatch::reject_data_plane_error(&resp)?;
        return Ok(None);
    }
    Ok(Some(resp))
}

/// The versions every core reported, a `NotFound` refusal's included. A
/// refusal observed its vShards with no matching row, and a read validates
/// that observation at the versions the refusal reported.
pub(crate) fn reported_versions<'a>(
    collected: impl IntoIterator<Item = &'a crate::Result<Response>>,
) -> crate::types::ReadVersions {
    let mut versions = crate::types::ReadVersions::new();
    for resp in collected.into_iter().flatten() {
        versions.merge(&resp.read_versions);
    }
    versions
}

/// Require every core to answer, and return the answers in core order.
///
/// The first error in core order fails the whole gather. It crosses as the
/// core's own typed error, so its SQLSTATE class survives.
pub(crate) fn require_every_core<T>(
    outcomes: impl IntoIterator<Item = CoreOutcome<T>>,
) -> crate::Result<Vec<T>> {
    let mut answered = Vec::new();
    for outcome in outcomes {
        if let Some(answer) = outcome? {
            answered.push(answer);
        }
    }
    Ok(answered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{ErrorCode, Payload};
    use crate::types::{Lsn, RequestId};

    fn response(status: Status, payload: &[u8], code: Option<ErrorCode>) -> Response {
        Response {
            request_id: RequestId::new(7),
            status,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(payload.to_vec()),
            watermark_lsn: Lsn::ZERO,
            error_code: code.map(Box::new),
            stage_vote: None,
            read_versions: crate::types::ReadVersions::new(),
            write_set: Vec::new(),
        }
    }

    fn answered(payload: &[u8]) -> crate::Result<Response> {
        Ok(response(Status::Ok, payload, None))
    }

    fn refused(code: ErrorCode) -> crate::Result<Response> {
        Ok(response(Status::Error, &[], Some(code)))
    }

    fn payloads(merged: &[Response]) -> Vec<Vec<u8>> {
        merged.iter().map(|r| r.payload.as_ref().to_vec()).collect()
    }

    #[test]
    fn every_core_answering_merges_in_core_order() {
        let merged = require_every_core(
            [answered(b"a"), answered(b"b"), answered(b"c")]
                .into_iter()
                .map(classify_core_response),
        )
        .unwrap();
        assert_eq!(
            payloads(&merged),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn one_failed_core_fails_the_gather_with_its_own_code() {
        let result = require_every_core(
            [
                answered(b"a"),
                refused(ErrorCode::DivisionByZero),
                answered(b"c"),
            ]
            .into_iter()
            .map(classify_core_response),
        );
        match result {
            Err(crate::Error::DataPlane(ErrorCode::DivisionByZero)) => {}
            other => panic!("expected the failed core's own code, got {other:?}"),
        }
    }

    #[test]
    fn a_core_that_never_answered_fails_the_gather() {
        let result = require_every_core(
            [
                answered(b"a"),
                Err(crate::Error::DeadlineExceeded {
                    request_id: RequestId::new(3),
                }),
            ]
            .into_iter()
            .map(classify_core_response),
        );
        match result {
            Err(crate::Error::DeadlineExceeded { request_id }) => {
                assert_eq!(request_id, RequestId::new(3));
            }
            other => panic!("expected the deadline, got {other:?}"),
        }
    }

    #[test]
    fn the_first_error_in_core_order_wins() {
        let result = require_every_core(
            [
                refused(ErrorCode::DivisionByZero),
                refused(ErrorCode::DeadlineExceeded),
            ]
            .into_iter()
            .map(classify_core_response),
        );
        assert!(matches!(
            result,
            Err(crate::Error::DataPlane(ErrorCode::DivisionByZero))
        ));
    }

    #[test]
    fn an_empty_answer_stays_distinct_from_a_missing_one() {
        let merged = require_every_core(
            [answered(b""), refused(ErrorCode::NotFound), answered(b"c")]
                .into_iter()
                .map(classify_core_response),
        )
        .unwrap();
        // The empty answer is kept. The `NotFound` core contributes nothing.
        assert_eq!(payloads(&merged), vec![Vec::new(), b"c".to_vec()]);
    }

    #[test]
    fn an_error_status_without_a_code_fails_closed() {
        let result = require_every_core(
            [answered(b"a"), Ok(response(Status::Error, &[], None))]
                .into_iter()
                .map(classify_core_response),
        );
        assert!(matches!(
            result,
            Err(crate::Error::DataPlane(ErrorCode::Internal { .. }))
        ));
    }
}
