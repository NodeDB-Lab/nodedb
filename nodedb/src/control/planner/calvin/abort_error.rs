// SPDX-License-Identifier: BUSL-1.1

//! Mapping of a Calvin cross-shard ABORT verdict to the error the client sees.

use nodedb_cluster::calvin::{AbortCause, AbortReason};

use crate::Error;
use crate::bridge::envelope::ErrorCode;
use crate::control::cluster::calvin::refusal::decode_refusal;

/// Pick the error for an ABORT verdict from the cause the verdict recorded.
///
/// A participant error never validated a read-set, so it must not be reported
/// as a serialization conflict. A participant error that carries a refusal is
/// the Data-Plane error the stage refused with. The client sees the SQLSTATE
/// a direct write of the same slice renders.
pub fn calvin_abort_error(cause: AbortCause) -> Error {
    match cause.reason {
        AbortReason::ParticipantError => match cause.refusal {
            Some(refusal) => match decode_refusal(&refusal) {
                // A limit refusal is the Control Plane's own error, so the
                // client sees what a direct statement past it reports.
                Ok(ErrorCode::ExecutionLimitExceeded { detail }) => {
                    Error::ExecutionLimitExceeded { detail }
                }
                Ok(code) => Error::DataPlane(code),
                Err(error) => error,
            },
            None => Error::CalvinParticipantError,
        },
        // A participant rejected the plans: they do not decode or route, or
        // hold no work for it. A resubmit of the same plans fails the same
        // way, so this is an internal error (`XX000`), never a retry class.
        AbortReason::PlanRejected => Error::Internal {
            detail: "cross-shard transaction aborted: a participant vShard rejected the \
                     transaction's plans"
                .to_owned(),
        },
        // Planned against a collection a purge and a same-name create replaced.
        AbortReason::CollectionSuperseded => Error::RetryableSchemaChanged {
            descriptor: "a collection the transaction writes was dropped and recreated".into(),
        },
        // The state a participant checked moved since the coordinator read
        // it. A retrying coordinator reads again; any other caller reports a
        // conflict the client retries. A multi-part transaction whose parts a
        // leader change lost wrote nothing, and a retry succeeds the same way.
        AbortReason::SerializationConflict
        | AbortReason::PredictionDrift
        | AbortReason::PartsLost => Error::CalvinSerializationConflict,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::sqlstate;

    use super::*;
    use crate::control::cluster::calvin::refusal::encode_refusal;

    fn refused(code: &ErrorCode) -> AbortCause {
        AbortCause::refused(encode_refusal(code).expect("encode"))
    }

    #[test]
    fn participant_error_is_not_reported_as_a_serialization_conflict() {
        assert!(matches!(
            calvin_abort_error(AbortReason::ParticipantError.into()),
            Error::CalvinParticipantError
        ));
    }

    /// A refused stage answers its Data-Plane code, which renders the
    /// SQLSTATE of the constraint it names.
    #[test]
    fn a_refused_participant_answers_its_refusal() {
        let cases = [
            (
                "primary_key_immutable",
                sqlstate::INTEGRITY_CONSTRAINT_VIOLATION,
            ),
            ("not_null", sqlstate::NOT_NULL_VIOLATION),
        ];
        for (constraint, expected) in cases {
            let code = ErrorCode::RejectedConstraint {
                constraint: constraint.into(),
                detail: format!("{constraint} refused the row"),
            };
            let error = calvin_abort_error(refused(&code));
            assert!(
                matches!(&error, Error::DataPlane(found) if *found == code),
                "{constraint}: {error:?}"
            );
            let (_, state, _) = crate::control::server::pgwire::types::error_to_sqlstate(&error);
            assert_eq!(state, expected, "{constraint}");
        }
    }

    #[test]
    fn an_undecodable_refusal_is_an_internal_error() {
        assert!(matches!(
            calvin_abort_error(AbortCause::refused(vec![0xde, 0xad])),
            Error::Internal { .. }
        ));
    }

    #[test]
    fn a_rejected_plan_is_an_internal_error() {
        assert!(matches!(
            calvin_abort_error(AbortReason::PlanRejected.into()),
            Error::Internal { .. }
        ));
    }

    #[test]
    fn a_superseded_collection_asks_for_a_retry() {
        assert!(matches!(
            calvin_abort_error(AbortReason::CollectionSuperseded.into()),
            Error::RetryableSchemaChanged { .. }
        ));
    }

    #[test]
    fn a_stale_read_set_is_a_serialization_conflict() {
        assert!(matches!(
            calvin_abort_error(AbortReason::SerializationConflict.into()),
            Error::CalvinSerializationConflict
        ));
    }

    #[test]
    fn a_limit_refusal_is_the_control_plane_limit_error() {
        let detail = "RETURNING rows 1 exceeds limit 0".to_owned();
        let error = calvin_abort_error(refused(&ErrorCode::ExecutionLimitExceeded {
            detail: detail.clone(),
        }));
        assert!(
            matches!(&error, Error::ExecutionLimitExceeded { detail: got } if *got == detail),
            "{error:?}"
        );
    }
}
