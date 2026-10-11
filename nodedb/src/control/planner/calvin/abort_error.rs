// SPDX-License-Identifier: BUSL-1.1

//! Mapping of a Calvin cross-shard ABORT verdict to the error the client sees.

use nodedb_cluster::calvin::AbortReason;

use crate::Error;

/// Pick the error for an ABORT verdict from the reason the verdict recorded.
///
/// A participant error never validated a read-set, so it must not be reported
/// as a serialization conflict.
pub fn calvin_abort_error(reason: AbortReason) -> Error {
    match reason {
        AbortReason::ParticipantError => Error::CalvinParticipantError,
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
    use super::*;

    #[test]
    fn participant_error_is_not_reported_as_a_serialization_conflict() {
        assert!(matches!(
            calvin_abort_error(AbortReason::ParticipantError),
            Error::CalvinParticipantError
        ));
    }

    #[test]
    fn a_rejected_plan_is_an_internal_error() {
        assert!(matches!(
            calvin_abort_error(AbortReason::PlanRejected),
            Error::Internal { .. }
        ));
    }

    #[test]
    fn a_superseded_collection_asks_for_a_retry() {
        assert!(matches!(
            calvin_abort_error(AbortReason::CollectionSuperseded),
            Error::RetryableSchemaChanged { .. }
        ));
    }

    #[test]
    fn a_stale_read_set_is_a_serialization_conflict() {
        assert!(matches!(
            calvin_abort_error(AbortReason::SerializationConflict),
            Error::CalvinSerializationConflict
        ));
    }
}
