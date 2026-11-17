// SPDX-License-Identifier: BUSL-1.1

//! Build outbound [`ArrayRejectMsg`] frames, and map a typed engine error to
//! its reject reason.

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};

use crate::bridge::envelope::ErrorCode;
use crate::control::server::sync::refusal::{
    is_retryable_dispatch_error, is_unknown_commit_outcome,
};

/// Build an [`ArrayRejectMsg`] for a rejected inbound op.
///
/// `op_hlc` is the HLC of the offending op. `reason` is a structured code.
/// `detail` is a human-readable explanation.
pub fn build_reject(
    array: &str,
    op_hlc: Hlc,
    reason: ArrayRejectReason,
    detail: impl Into<String>,
) -> ArrayRejectMsg {
    ArrayRejectMsg {
        array: array.to_owned(),
        op_hlc_bytes: op_hlc.to_bytes(),
        reason,
        detail: detail.into(),
    }
}

/// Build the reject for an op the engine path failed with `error`.
///
/// The reason comes from the typed error through
/// [`reason_for_engine_error`]. `context` names the failed step in `detail`.
pub fn engine_reject(
    array: &str,
    op_hlc: Hlc,
    context: &str,
    error: &crate::Error,
) -> ArrayRejectMsg {
    build_reject(
        array,
        op_hlc,
        reason_for_engine_error(error),
        format!("{context}: {error}"),
    )
}

/// The reject reason for an engine-path error, read from its type alone.
///
/// - `PermissionDenied`: an authorization denial.
/// - `EngineUnavailable`: the shared sync classifier finds the error
///   retryable, or the commit outcome is unknown. Array apply dedups by
///   HLC, so a re-send of an op that did commit is a no-op.
/// - `EngineRefused`: every other error, which the same bytes reproduce.
pub fn reason_for_engine_error(error: &crate::Error) -> ArrayRejectReason {
    if is_authorization_denial(error) {
        return ArrayRejectReason::PermissionDenied;
    }
    if is_retryable_dispatch_error(error) || is_unknown_commit_outcome(error) {
        return ArrayRejectReason::EngineUnavailable;
    }
    ArrayRejectReason::EngineRefused
}

fn is_authorization_denial(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::RejectedAuthz { .. }
            | crate::Error::DataPlane(ErrorCode::RejectedAuthz { .. })
    )
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::array::ArrayRejectClass;

    use super::*;
    use crate::types::{RequestId, TenantId, VShardId};

    fn class_of(error: &crate::Error) -> ArrayRejectClass {
        reason_for_engine_error(error).class()
    }

    #[test]
    fn a_dispatch_capacity_refusal_is_transient() {
        let error = crate::Error::DispatchCapacity {
            scope: crate::DispatchCapacityScope::TenantInflight {
                tenant_id: TenantId::new(1),
                inflight: 64,
                cap: 64,
            },
        };
        assert_eq!(
            reason_for_engine_error(&error),
            ArrayRejectReason::EngineUnavailable
        );
        assert_eq!(class_of(&error), ArrayRejectClass::Transient);
    }

    #[test]
    fn a_data_plane_capacity_refusal_is_transient() {
        let error = crate::Error::DataPlane(ErrorCode::DispatchCapacity {
            reason: "core queue full".into(),
        });
        assert_eq!(class_of(&error), ArrayRejectClass::Transient);
    }

    #[test]
    fn leader_change_deadline_and_lost_quorum_are_transient() {
        let errors = [
            crate::Error::NotLeader {
                vshard_id: VShardId::new(0),
                leader_node: 2,
                leader_addr: "10.0.0.2:9000".into(),
                leader_term: 3,
            },
            crate::Error::DeadlineExceeded {
                request_id: RequestId::new(1),
            },
            crate::Error::GroupQuorumUnavailable {
                group_id: 1,
                voters: vec![1, 2, 3],
                unreachable: vec![2, 3],
            },
            crate::Error::MetadataLeaderUnavailable,
            crate::Error::Backpressure {
                engine: nodedb_mem::EngineId::Timeseries,
            },
        ];
        for error in errors {
            assert_eq!(
                reason_for_engine_error(&error),
                ArrayRejectReason::EngineUnavailable,
                "{error:?}"
            );
        }
    }

    #[test]
    fn an_unknown_commit_outcome_is_transient_because_hlc_dedups_a_resend() {
        let error = crate::Error::ProposalOutcomeUnknown {
            group_id: 1,
            log_index: 9,
        };
        assert_eq!(class_of(&error), ArrayRejectClass::Transient);
    }

    #[test]
    fn an_authorization_denial_is_permission_denied() {
        let control = crate::Error::RejectedAuthz {
            tenant_id: TenantId::new(1),
            resource: "array 'a'".into(),
        };
        let data = crate::Error::DataPlane(ErrorCode::RejectedAuthz {
            resource: "array 'a'".into(),
        });
        for error in [control, data] {
            assert_eq!(
                reason_for_engine_error(&error),
                ArrayRejectReason::PermissionDenied
            );
            assert_eq!(class_of(&error), ArrayRejectClass::Permanent);
        }
    }

    #[test]
    fn a_verdict_on_the_op_is_permanent() {
        let errors = [
            crate::Error::DataPlane(ErrorCode::RejectedPrevalidation {
                reason: "array put fenced".into(),
            }),
            crate::Error::DataPlane(ErrorCode::TypeMismatch {
                collection: "a".into(),
                detail: "attr v".into(),
            }),
            crate::Error::RejectedConstraint {
                collection: "a".into(),
                constraint: "c".into(),
                detail: "violated".into(),
            },
            crate::Error::Internal {
                detail: "replicated entry encode".into(),
            },
        ];
        for error in errors {
            assert_eq!(
                reason_for_engine_error(&error),
                ArrayRejectReason::EngineRefused,
                "{error:?}"
            );
        }
    }

    #[test]
    fn an_engine_reject_carries_the_context_and_the_derived_reason() {
        let error = crate::Error::DispatchCapacity {
            scope: crate::DispatchCapacityScope::QueueFull {
                core_id: 0,
                capacity: 8,
            },
        };
        let reject = engine_reject("a", Hlc::ZERO, "raft propose", &error);
        assert_eq!(reject.reason, ArrayRejectReason::EngineUnavailable);
        assert!(reject.detail.starts_with("raft propose: "));
    }
}
