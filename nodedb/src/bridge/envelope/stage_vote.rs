// SPDX-License-Identifier: BUSL-1.1

//! The local vote a Calvin stage response carries to its scheduler.

use super::error_code::ErrorCode;

/// A participant's local vote on its slice of a staged Calvin transaction.
///
/// Every Calvin stage path sets one on its response. No other response
/// carries one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageVote {
    /// The slice staged, and its slice of the read-set is still current.
    Commit,
    /// The slice staged, but a read it validates was superseded.
    SerializationConflict,
    /// A state the coordinator predicted moved. The slice holds no staged
    /// state, and the coordinator reads again and retries.
    PredictionDrift,
    /// The stage failed. The slice holds no staged state.
    ParticipantError,
}

impl StageVote {
    /// The vote of a stage that refused with `error`.
    ///
    /// `OllpRetryRequired` reports a moved prediction. Every other error is a
    /// participant error.
    pub fn of_stage_error(error: &ErrorCode) -> Self {
        if matches!(error, ErrorCode::OllpRetryRequired) {
            Self::PredictionDrift
        } else {
            Self::ParticipantError
        }
    }

    /// The vote of a staged slice whose read-set check found it `current`.
    pub fn of_read_set(current: bool) -> Self {
        if current {
            Self::Commit
        } else {
            Self::SerializationConflict
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ollp_retry_refusal_votes_prediction_drift() {
        assert_eq!(
            StageVote::of_stage_error(&ErrorCode::OllpRetryRequired),
            StageVote::PredictionDrift
        );
    }

    #[test]
    fn any_other_refusal_votes_participant_error() {
        assert_eq!(
            StageVote::of_stage_error(&ErrorCode::DeadlineExceeded),
            StageVote::ParticipantError
        );
        assert_eq!(
            StageVote::of_stage_error(&ErrorCode::Internal {
                detail: "stage".into()
            }),
            StageVote::ParticipantError
        );
    }

    #[test]
    fn read_set_check_maps_to_commit_or_serialization_conflict() {
        assert_eq!(StageVote::of_read_set(true), StageVote::Commit);
        assert_eq!(
            StageVote::of_read_set(false),
            StageVote::SerializationConflict
        );
    }
}
