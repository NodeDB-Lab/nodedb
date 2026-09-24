// SPDX-License-Identifier: BUSL-1.1

//! The one place a Data-Plane error `Response` becomes a typed `Err`.
//!
//! Every Control-Plane seam that collapses a shard `Response` into a
//! `crate::Result` goes through here, so an error status can never flatten
//! into an empty success payload.

use crate::bridge::envelope::{ErrorCode, Response, Status};

/// Reject a Data-Plane error response, keeping its typed code.
///
/// `NotFound` passes: it means the shard holds no slice of the collection,
/// which reads back as an empty — still validatable — observation. Every
/// other code crosses as `Error::DataPlane` so its SQLSTATE survives. An
/// error status carrying no code fails closed rather than reading as success.
///
/// `DeadlineExceeded` and `ExpiredBeforeExecution` are the exception, and
/// they cross as [`crate::Error::DeadlineExceeded`]. A shard refusing an
/// expired task is the
/// statement running out of time — the same condition the Control-Plane timer
/// reports — so both produce one variant and one SQLSTATE. Leaving it wrapped
/// would make the SQLSTATE a client sees depend on which half of that race
/// won.
pub(crate) fn reject_data_plane_error(resp: &Response) -> crate::Result<()> {
    if resp.status != Status::Error {
        return Ok(());
    }
    match resp.error_code.as_deref() {
        Some(ErrorCode::NotFound) => Ok(()),
        Some(ErrorCode::DeadlineExceeded | ErrorCode::ExpiredBeforeExecution) => {
            Err(crate::Error::DeadlineExceeded {
                request_id: resp.request_id,
            })
        }
        Some(code) => Err(crate::Error::DataPlane(code.clone())),
        None => Err(crate::Error::DataPlane(ErrorCode::Internal {
            detail: "data plane returned an error status with no error code".into(),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Payload;
    use crate::types::{Lsn, RequestId};

    fn refusal(code: ErrorCode) -> Response {
        Response {
            request_id: RequestId::new(9),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: Some(Box::new(code)),
            read_set_valid: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    #[test]
    fn a_shard_refusing_an_expired_task_reports_the_deadline() {
        // The other half of the race — the Control-Plane timer — raises this
        // same variant, so a client sees one SQLSTATE either way.
        match reject_data_plane_error(&refusal(ErrorCode::DeadlineExceeded)) {
            Err(crate::Error::DeadlineExceeded { request_id }) => {
                assert_eq!(request_id, RequestId::new(9));
            }
            other => panic!("expected the deadline variant, got {other:?}"),
        }
    }

    /// A task that expired before it started reports the same deadline.
    #[test]
    fn a_task_that_never_started_reports_the_deadline() {
        match reject_data_plane_error(&refusal(ErrorCode::ExpiredBeforeExecution)) {
            Err(crate::Error::DeadlineExceeded { request_id }) => {
                assert_eq!(request_id, RequestId::new(9));
            }
            other => panic!("expected the deadline variant, got {other:?}"),
        }
    }

    #[test]
    fn every_other_verdict_keeps_its_data_plane_code() {
        match reject_data_plane_error(&refusal(ErrorCode::DivisionByZero)) {
            Err(crate::Error::DataPlane(ErrorCode::DivisionByZero)) => {}
            other => panic!("expected the shard's own code, got {other:?}"),
        }
    }

    #[test]
    fn a_shard_holding_no_slice_is_not_an_error() {
        assert!(reject_data_plane_error(&refusal(ErrorCode::NotFound)).is_ok());
    }
}
