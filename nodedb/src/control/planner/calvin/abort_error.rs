// SPDX-License-Identifier: BUSL-1.1

//! Mapping of a Calvin cross-shard ABORT verdict to the error the client sees.

use nodedb_cluster::calvin::{AbortCause, AbortReason};

use crate::Error;
use crate::bridge::envelope::ErrorCode;
use crate::control::cluster::calvin::refusal::{StageRefusal, decode_refusal};
use crate::control::planner::calvin::refused_sequences;

/// Pick the error for an ABORT verdict from the cause the verdict recorded.
///
/// A participant error never validated a read-set, so it must not be reported
/// as a serialization conflict. A participant error that carries a refusal is
/// the Data-Plane error the stage refused with. The client sees the SQLSTATE
/// a direct write of the same slice renders.
///
/// A refused `RETURNING` evaluation's sequence results stand, so this gives
/// the writer those of every refusing participant before it returns the
/// error: the reported refusal's first, then the others' in vShard order
/// (see [`refused_sequences::record`]).
pub fn calvin_abort_error(cause: AbortCause) -> Error {
    record_refused_sequences(&cause);
    verdict_error(&cause)
}

/// Give the writer the sequence results of every refusal `cause` carries:
/// the reported refusal's first, then the others' in vShard order. A
/// coordinator that retries an aborted attempt records them too: the
/// participants drew them.
pub fn record_refused_sequences(cause: &AbortCause) {
    for refusal in cause.refusal.iter().chain(&cause.other_refusals) {
        match decode_refusal(refusal) {
            Ok(StageRefusal {
                sequences: Some(sequences),
                ..
            }) => refused_sequences::record(sequences),
            Ok(_) => {}
            Err(error) => tracing::warn!(
                %error,
                "calvin: a participant's refusal does not decode; its sequence results are lost"
            ),
        }
    }
}

/// The error of `cause`.
fn verdict_error(cause: &AbortCause) -> Error {
    match cause.reason {
        AbortReason::ParticipantError => match &cause.refusal {
            Some(refusal) => match decode_refusal(refusal) {
                Ok(StageRefusal { code, .. }) => refusal_error(code),
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

/// The error of a stage refusal's `code`.
fn refusal_error(code: ErrorCode) -> Error {
    match code {
        // A limit refusal is the Control Plane's own error, so the client
        // sees what a direct statement past it reports.
        ErrorCode::ExecutionLimitExceeded { detail } => Error::ExecutionLimitExceeded { detail },
        code => Error::DataPlane(code),
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

    /// A refused evaluation's sequence results reach the writer with the
    /// refusal's own error.
    #[tokio::test]
    async fn a_refusal_gives_its_sequence_results_to_the_writer() {
        use crate::control::cluster::calvin::refusal::{
            RefusedSequences, encode_refusal_with_sequences,
        };
        use crate::control::planner::calvin::refused_sequences::collecting;
        use crate::control::wal_replication::types::FrozenSequenceEffect;
        let sequences = RefusedSequences {
            database_id: nodedb_types::DatabaseId::new(1),
            tenant_id: nodedb_types::TenantId::new(7),
            effects: vec![FrozenSequenceEffect::Nextval {
                name: "s".into(),
                value: 1,
            }],
        };
        let refusal = encode_refusal_with_sequences(&ErrorCode::DivisionByZero, Some(&sequences))
            .expect("encode");
        let (error, collected) =
            collecting(async { calvin_abort_error(AbortCause::refused(refusal)) }).await;
        assert!(
            matches!(&error, Error::DataPlane(ErrorCode::DivisionByZero)),
            "{error:?}"
        );
        assert_eq!(collected, Some(sequences));
    }

    /// Two participants draw and refuse. The verdict reports one refusal,
    /// and the writer takes the results of both.
    #[tokio::test]
    async fn every_refusing_participant_gives_its_results_to_the_writer() {
        use crate::control::cluster::calvin::refusal::{
            RefusedSequences, encode_refusal_with_sequences,
        };
        use crate::control::planner::calvin::refused_sequences::collecting;
        use crate::control::wal_replication::types::FrozenSequenceEffect;
        let drew = |value: i64| RefusedSequences {
            database_id: nodedb_types::DatabaseId::new(1),
            tenant_id: nodedb_types::TenantId::new(7),
            effects: vec![FrozenSequenceEffect::Nextval {
                name: "s".into(),
                value,
            }],
        };
        let refusal = |value: i64| {
            encode_refusal_with_sequences(&ErrorCode::DivisionByZero, Some(&drew(value)))
                .expect("encode")
        };
        let cause = AbortCause {
            other_refusals: vec![refusal(2)],
            ..AbortCause::refused(refusal(1))
        };
        let (error, collected) = collecting(async { calvin_abort_error(cause) }).await;
        assert!(
            matches!(&error, Error::DataPlane(ErrorCode::DivisionByZero)),
            "{error:?}"
        );
        let collected = collected.expect("both refusals drew");
        assert_eq!(
            collected.effects,
            [drew(1).effects, drew(2).effects].concat(),
            "the reported refusal first, then the other"
        );
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
