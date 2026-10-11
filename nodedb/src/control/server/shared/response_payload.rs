// SPDX-License-Identifier: BUSL-1.1

//! Flattening a Data-Plane [`Response`] into the payload bytes a
//! payload-returning door hands back.

use crate::bridge::envelope::{Response, Status};

/// Return `response`'s payload, or its rejection as a typed error.
///
/// The typed [`crate::bridge::envelope::ErrorCode`] is preserved rather than
/// formatted into a message, so a caller classifies the rejection
/// (`is_retriable()`, `is_not_found()`, …) instead of substring-matching it. A
/// rejection carrying no code has only its payload to report, so that becomes
/// the detail.
///
/// Shared by every door that returns bytes rather than a `Response`: the
/// replicated sync write, the user DDL/DSL dispatch, the system dispatch, and
/// the version-history read. One copy so a response a clone hook served and a response the Data
/// Plane returned cannot be reported differently.
pub(crate) fn payload_or_typed_error(response: Response) -> crate::Result<Vec<u8>> {
    if response.status != Status::Ok {
        return Err(match response.error_code {
            Some(code) => crate::Error::DataPlane(*code),
            None => crate::Error::Internal {
                detail: String::from_utf8_lossy(&response.payload).into_owned(),
            },
        });
    }
    Ok(response.payload.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{ErrorCode, Payload};
    use crate::types::{Lsn, RequestId};

    fn refusal(code: Option<ErrorCode>, payload: &[u8]) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Error,
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

    #[test]
    fn a_coded_refusal_keeps_its_code() {
        let code = ErrorCode::Unsupported {
            detail: "not on this engine".into(),
        };
        match payload_or_typed_error(refusal(Some(code.clone()), b"")) {
            Err(crate::Error::DataPlane(kept)) => assert_eq!(kept, code),
            other => panic!("expected the typed refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_refusal_with_no_code_reports_its_payload() {
        match payload_or_typed_error(refusal(None, b"handler detail")) {
            Err(crate::Error::Internal { detail }) => assert_eq!(detail, "handler detail"),
            other => panic!("expected an internal error, got {other:?}"),
        }
    }
}
