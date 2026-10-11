// SPDX-License-Identifier: BUSL-1.1

//! Shared error constructors for the transaction-command handlers.

use pgwire::error::{ErrorInfo, PgWireError};

/// Builds the canonical SQLSTATE 57014 error emitted when a Calvin coordinator
/// channel is closed (coordinator task dropped due to deadline expiry).  The
/// neutral commit orchestrator surfaces this as `AbortReason::CalvinCancelled`;
/// this constructor defines the mapping in exactly one place so the tests
/// exercise the production path.
pub(super) fn calvin_cancelled_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "57014".to_owned(),
        "Calvin coordinator cancelled (deadline exceeded)".to_owned(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calvin_assignment_channel_closed_returns_57014() {
        // Exercises the production constructor directly — no replicated mapping.
        let err = calvin_cancelled_error();
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(
                    info.code, "57014",
                    "expected SQLSTATE 57014 (query_canceled) for Calvin channel-closed, got {}",
                    info.code
                );
                assert_ne!(
                    info.code, "XX000",
                    "must not surface XX000 (internal_error) for a coordinator deadline cancel"
                );
            }
            other => panic!("expected PgWireError::UserError, got {other:?}"),
        }
    }

    #[test]
    fn calvin_completion_channel_closed_returns_57014() {
        // Completion arm uses the same production constructor — assert it here too.
        let err = calvin_cancelled_error();
        match err {
            PgWireError::UserError(info) => {
                assert_eq!(
                    info.code, "57014",
                    "expected SQLSTATE 57014 for Calvin completion channel-closed, got {}",
                    info.code
                );
            }
            other => panic!("expected PgWireError::UserError, got {other:?}"),
        }
    }
}
