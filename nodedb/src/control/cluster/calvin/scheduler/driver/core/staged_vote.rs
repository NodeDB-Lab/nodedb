// SPDX-License-Identifier: BUSL-1.1

//! This participant's local commit vote on a staged Calvin transaction,
//! read from the stage vote its executor response carries.

use nodedb_cluster::calvin::{AbortCause, AbortReason};

use crate::bridge::envelope::{ErrorCode, Response, StageVote, Status};
use crate::control::cluster::calvin::refusal::encode_refusal;

/// A participant's local verdict on its staged slice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum StagedVote {
    /// The read-set was still current, or the path carries none.
    Commit,
    /// Validated and stale: a genuine serialization conflict.
    SerializationConflict,
    /// The slice holds no staged state, so no read-set was ever validated.
    ///
    /// `refusal` is the encoded error a stage path refused the slice with.
    /// Every replica stages the same slice and raises the same refusal. It
    /// is `None` for a slice that never reached a stage path.
    ParticipantError { refusal: Option<Vec<u8>> },
    /// A collection the transaction names no longer holds its planned
    /// incarnation in this replica's catalog.
    CollectionSuperseded,
    /// The stage refused because the state the coordinator predicted moved.
    /// The coordinator reads again and retries.
    PredictionDrift,
}

impl StagedVote {
    /// `None` for a commit, the abort cause otherwise.
    pub(super) fn abort_cause(&self) -> Option<AbortCause> {
        match self {
            Self::Commit => None,
            Self::SerializationConflict => Some(AbortReason::SerializationConflict.into()),
            Self::ParticipantError { refusal } => Some(AbortCause {
                reason: AbortReason::ParticipantError,
                refusal: refusal.clone(),
            }),
            Self::CollectionSuperseded => Some(AbortReason::CollectionSuperseded.into()),
            Self::PredictionDrift => Some(AbortReason::PredictionDrift.into()),
        }
    }
}

/// A stage response the scheduler cannot read a vote from.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(super) enum StageVoteError {
    /// A stage response that did not fail carries no stage vote.
    #[error("stage returned {status:?} without a stage vote")]
    Missing { status: Status },
    /// A stage response that did not succeed carries a commit vote.
    #[error("stage returned {status:?} with a commit stage vote")]
    CommitWithoutSuccess { status: Status },
    /// The vShard's core fail-stopped and refused the stage. It refuses
    /// every later request too, the committed install included, so no vote
    /// this replica casts can be kept.
    #[error("stage refused by fail-stopped core {core_id}")]
    CoreFailStopped { core_id: usize },
    /// The stage's refusal has no wire form, so no vote can carry it.
    #[error("stage refusal cannot be encoded for the abort vote: {detail}")]
    RefusalUnencodable { detail: String },
}

/// Read the local vote of a staged slice from its stage response.
///
/// The response's stage vote maps 1:1. A `ParticipantError` vote carries the
/// response's error code as its refusal. An error response with no vote never
/// reached a stage path: the request expired, or its core is gone or
/// drained. That slice never staged, so it votes `ParticipantError` with no
/// refusal. A fail-stopped core's refusal is the exception: it is refused.
///
/// A response that did not fail and carries no vote is refused, and so is a
/// commit vote on a response that did not succeed. Neither counts as a commit.
pub(super) fn staged_commit_vote(response: &Response) -> Result<StagedVote, StageVoteError> {
    let status = response.status;
    if let Some(ErrorCode::CoreFailStopped { core_id, .. }) = response.error_code.as_deref() {
        return Err(StageVoteError::CoreFailStopped { core_id: *core_id });
    }
    match (response.stage_vote, status) {
        (Some(StageVote::Commit), Status::Ok) => Ok(StagedVote::Commit),
        (Some(StageVote::Commit), Status::Partial | Status::Error) => {
            Err(StageVoteError::CommitWithoutSuccess { status })
        }
        (Some(StageVote::SerializationConflict), _) => Ok(StagedVote::SerializationConflict),
        (Some(StageVote::PredictionDrift), _) => Ok(StagedVote::PredictionDrift),
        (Some(StageVote::ParticipantError), _) => {
            let refusal = response
                .error_code
                .as_deref()
                .map(encode_refusal)
                .transpose()
                .map_err(|e| StageVoteError::RefusalUnencodable {
                    detail: e.to_string(),
                })?;
            Ok(StagedVote::ParticipantError { refusal })
        }
        (None, Status::Error) => Ok(StagedVote::ParticipantError { refusal: None }),
        (None, Status::Ok | Status::Partial) => Err(StageVoteError::Missing { status }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Payload;
    use crate::types::{Lsn, RequestId};

    fn staged_response(status: Status, stage_vote: Option<StageVote>) -> Response {
        Response {
            request_id: RequestId::new(1),
            status,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            stage_vote,
            read_vshards: crate::types::ReadShards::new(),
            write_set: Vec::new(),
        }
    }

    #[test]
    fn every_stage_vote_maps_one_to_one() {
        let cases = [
            (Status::Ok, StageVote::Commit, StagedVote::Commit),
            (
                Status::Ok,
                StageVote::SerializationConflict,
                StagedVote::SerializationConflict,
            ),
            (
                Status::Error,
                StageVote::PredictionDrift,
                StagedVote::PredictionDrift,
            ),
            (
                Status::Error,
                StageVote::ParticipantError,
                StagedVote::ParticipantError { refusal: None },
            ),
        ];
        for (status, vote, expected) in cases {
            assert_eq!(
                staged_commit_vote(&staged_response(status, Some(vote))),
                Ok(expected),
                "{vote:?} on {status:?}"
            );
        }
    }

    #[test]
    fn a_prediction_drift_vote_aborts_with_prediction_drift() {
        let mut response = staged_response(Status::Error, Some(StageVote::PredictionDrift));
        response.error_code = Some(Box::new(ErrorCode::OllpRetryRequired));
        let vote = staged_commit_vote(&response);
        assert_eq!(vote, Ok(StagedVote::PredictionDrift));
        assert_eq!(
            vote.map(|vote| vote.abort_cause()),
            Ok(Some(AbortReason::PredictionDrift.into()))
        );
    }

    /// A stage that refused with a constraint keeps the refusal on its vote,
    /// so the coordinator answers that refusal instead of a bare abort.
    #[test]
    fn a_refused_stage_vote_keeps_its_error_code() {
        let code = ErrorCode::RejectedConstraint {
            constraint: "primary_key_immutable".into(),
            detail: "UPDATE cannot change primary key 'id'".into(),
        };
        let mut response = staged_response(Status::Error, Some(StageVote::ParticipantError));
        response.error_code = Some(Box::new(code.clone()));

        let vote = staged_commit_vote(&response).expect("a readable vote");
        let cause = vote.abort_cause().expect("an abort vote");
        assert_eq!(cause.reason, AbortReason::ParticipantError);
        let refusal = cause.refusal.expect("the vote carries the refusal");
        assert_eq!(
            crate::control::cluster::calvin::refusal::decode_refusal(&refusal)
                .expect("the refusal decodes"),
            code
        );
    }

    #[test]
    fn a_successful_stage_response_without_a_vote_is_refused() {
        assert_eq!(
            staged_commit_vote(&staged_response(Status::Ok, None)),
            Err(StageVoteError::Missing { status: Status::Ok })
        );
        assert_eq!(
            staged_commit_vote(&staged_response(Status::Partial, None)),
            Err(StageVoteError::Missing {
                status: Status::Partial
            })
        );
    }

    #[test]
    fn a_commit_vote_on_an_unsuccessful_response_is_refused() {
        assert_eq!(
            staged_commit_vote(&staged_response(Status::Error, Some(StageVote::Commit))),
            Err(StageVoteError::CommitWithoutSuccess {
                status: Status::Error
            })
        );
    }

    #[test]
    fn an_error_that_never_reached_a_stage_path_votes_participant_error() {
        // An expired request answers before any stage handler runs.
        let mut response = staged_response(Status::Error, None);
        response.error_code = Some(Box::new(ErrorCode::ExpiredBeforeExecution));
        assert_eq!(
            staged_commit_vote(&response),
            Ok(StagedVote::ParticipantError { refusal: None })
        );
    }

    /// A fail-stopped core's refusal is no vote: the core refuses the
    /// committed install too.
    #[test]
    fn a_fail_stopped_core_refusal_is_refused() {
        let mut response = staged_response(Status::Error, None);
        response.error_code = Some(Box::new(ErrorCode::CoreFailStopped {
            core_id: 2,
            detail: "rollback failed".into(),
        }));
        assert_eq!(
            staged_commit_vote(&response),
            Err(StageVoteError::CoreFailStopped { core_id: 2 })
        );
    }

    #[test]
    fn only_an_abort_carries_a_cause() {
        assert_eq!(StagedVote::Commit.abort_cause(), None);
        assert_eq!(
            StagedVote::SerializationConflict.abort_cause(),
            Some(AbortReason::SerializationConflict.into())
        );
        assert_eq!(
            StagedVote::ParticipantError { refusal: None }.abort_cause(),
            Some(AbortReason::ParticipantError.into())
        );
        assert_eq!(
            StagedVote::ParticipantError {
                refusal: Some(vec![3])
            }
            .abort_cause(),
            Some(AbortCause::refused(vec![3]))
        );
        assert_eq!(
            StagedVote::CollectionSuperseded.abort_cause(),
            Some(AbortReason::CollectionSuperseded.into())
        );
        assert_eq!(
            StagedVote::PredictionDrift.abort_cause(),
            Some(AbortReason::PredictionDrift.into())
        );
    }
}
