// SPDX-License-Identifier: Apache-2.0

//! The typed cause an error frame carries alongside its own classification.

use serde::{Deserialize, Serialize};

use crate::error::{ErrorCode, ErrorDetails, NodeDbError};

/// The typed error that caused the one an error frame reports.
///
/// A phase error such as `MOVE_TENANT_SNAPSHOT_FAILED` keeps its own code,
/// and the Data-Plane refusal that failed the phase rides here with its own
/// code and details. A client rebuilds it as the typed error's
/// [`NodeDbError::cause`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct ErrorCausePayload {
    /// Stable numeric NodeDB code of the cause.
    pub ndb_code: u16,
    /// Human-readable message of the cause.
    pub message: String,
    /// Structured details of the cause, when it had any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<ErrorDetails>,
}

impl From<&NodeDbError> for ErrorCausePayload {
    fn from(error: &NodeDbError) -> Self {
        Self {
            ndb_code: error.code().0,
            message: error.message().to_owned(),
            details: Some(error.details().clone()),
        }
    }
}

impl ErrorCausePayload {
    /// Rebuild the typed cause.
    pub fn to_error(&self) -> NodeDbError {
        NodeDbError::from_wire_with_details(
            ErrorCode(self.ndb_code),
            self.message.clone(),
            self.details.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cause_round_trips_its_class() {
        let cause = NodeDbError::division_by_zero();
        let payload = ErrorCausePayload::from(&cause);
        let bytes = zerompk::to_msgpack_vec(&payload).expect("encode");
        let decoded: ErrorCausePayload = zerompk::from_msgpack(&bytes).expect("decode");
        let rebuilt = decoded.to_error();
        assert_eq!(rebuilt.code(), ErrorCode::DIVISION_BY_ZERO);
        assert_eq!(rebuilt.details(), cause.details());
    }
}
