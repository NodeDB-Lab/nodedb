// SPDX-License-Identifier: BUSL-1.1

//! Shared classification helpers for committed-write apply results.

use crate::control::distributed_applier::propose_tracker::AppliedWrite;

pub(super) fn committed_response_result(
    response: &crate::bridge::envelope::Response,
) -> crate::Result<AppliedWrite> {
    if response.status == crate::bridge::envelope::Status::Ok {
        return Ok(AppliedWrite::from_response(response));
    }
    // The typed code carries the client's classification (constraint, authz,
    // conflict); stringifying it here would leave the caller only XX000.
    match response.error_code.as_deref() {
        Some(code) => {
            tracing::warn!(reason = ?code, "applying committed write failed");
            Err(crate::Error::DataPlane(code.clone()))
        }
        None => {
            tracing::warn!(
                reason = "execution error",
                "applying committed write failed"
            );
            Err(crate::Error::Internal {
                detail: "execution error".to_owned(),
            })
        }
    }
}

pub(super) fn deterministic_crdt_fence_noop(result: &crate::Result<AppliedWrite>) -> bool {
    matches!(
        result,
        Err(crate::Error::DataPlane(
            crate::bridge::envelope::ErrorCode::CrdtFrontierMismatch { .. }
        ))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::distributed_applier::applied_index::AppliedPrefix;

    #[test]
    fn fenced_frontier_mismatch_completes_retry_and_advances_durable_prefix() {
        let result: crate::Result<AppliedWrite> = Err(crate::Error::DataPlane(
            crate::bridge::envelope::ErrorCode::CrdtFrontierMismatch {
                expected: [1; 32],
                actual: [2; 32],
            },
        ));
        assert!(deterministic_crdt_fence_noop(&result));

        let mut prefix = AppliedPrefix::new();
        prefix.record(17, deterministic_crdt_fence_noop(&result));
        assert_eq!(prefix.floor(), Some(17));
    }
}
