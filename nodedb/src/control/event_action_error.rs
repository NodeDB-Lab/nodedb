// SPDX-License-Identifier: BUSL-1.1

//! Failure taxonomy for DEFINE EVENT THEN actions.
//!
//! An action renders a template into SQL, plans it, takes descriptor leases,
//! and runs its tasks as one transaction. Each of those can fail differently,
//! and only some failures can be tried again — the distinction decides
//! whether a failed action is queued for retry or reported and dropped.

use crate::control::system_txn::SystemTxnError;

/// Why a DEFINE EVENT THEN action template could not become executable SQL.
///
/// Both cases mean the template is malformed in a way that could change what
/// the rendered statement does, so rendering refuses rather than guessing.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum TriggerRenderError {
    #[error("unterminated block comment in event trigger action")]
    UnterminatedBlockComment,

    #[error("event trigger placeholders must not be manually quoted")]
    QuotedPlaceholder,

    #[error("unterminated quoted region in event trigger action")]
    UnterminatedQuote,

    #[error("unterminated dollar quote in event trigger action")]
    UnterminatedDollarQuote,

    #[error("invalid UTF-8 boundary in event trigger action")]
    InvalidUtf8Boundary,
}

/// Why one DEFINE EVENT THEN action did not run to completion.
#[derive(Debug, thiserror::Error)]
pub enum TriggerActionError {
    /// The action template could not be rendered into executable SQL.
    #[error("trigger action rejected: {source}")]
    Rejected {
        #[source]
        source: TriggerRenderError,
    },

    /// Planning the rendered SQL failed.
    #[error("trigger action planning failed: {source}")]
    Plan {
        #[source]
        source: crate::Error,
    },

    /// Descriptor lease admission refused the plan.
    #[error("trigger action refused by descriptor lease admission: {source}")]
    LeaseAdmission {
        #[source]
        source: crate::Error,
    },

    /// The action's transaction did not commit, so it applied nothing.
    #[error("trigger action transaction failed: {source}")]
    Transaction {
        #[source]
        source: SystemTxnError,
    },
}

impl From<TriggerActionError> for crate::Error {
    /// Preserve the underlying error where the action carried one, so a
    /// retryable failure does not flatten into an opaque internal error on
    /// its way out of the Event Plane.
    fn from(e: TriggerActionError) -> Self {
        match e {
            TriggerActionError::Rejected { source } => crate::Error::BadRequest {
                detail: source.to_string(),
            },
            TriggerActionError::Plan { source } | TriggerActionError::LeaseAdmission { source } => {
                source
            }
            TriggerActionError::Transaction { source } => crate::Error::Internal {
                detail: source.to_string(),
            },
        }
    }
}

impl TriggerActionError {
    /// Whether re-running the whole action is safe.
    ///
    /// Safe unless the template itself is malformed: the same template renders
    /// the same way every time, so a rendering failure never becomes a
    /// success. Every other failure leaves nothing applied — the action's
    /// tasks commit as one transaction — so re-running repeats no side
    /// effect.
    pub fn is_retryable(&self) -> bool {
        match self {
            // A failed transaction applied nothing at all, so there is never
            // a partial application to duplicate by running it again.
            Self::Plan { .. } | Self::LeaseAdmission { .. } | Self::Transaction { .. } => true,
            Self::Rejected { .. } => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction_failure() -> TriggerActionError {
        TriggerActionError::Transaction {
            source: SystemTxnError::Commit {
                detail: "serialization failure against a concurrent write".to_owned(),
                code: None,
            },
        }
    }

    #[test]
    fn a_malformed_template_is_not_retried() {
        let error = TriggerActionError::Rejected {
            source: TriggerRenderError::QuotedPlaceholder,
        };
        assert!(
            !error.is_retryable(),
            "the same template renders the same way every time"
        );
    }

    #[test]
    fn a_planning_failure_is_retried() {
        let error = TriggerActionError::Plan {
            source: crate::Error::Internal {
                detail: "planner unavailable".into(),
            },
        };
        assert!(error.is_retryable());
    }

    #[test]
    fn lease_admission_is_retried() {
        let error = TriggerActionError::LeaseAdmission {
            source: crate::Error::RetryableSchemaChanged {
                descriptor: "orders".into(),
            },
        };
        assert!(error.is_retryable());
    }

    #[test]
    fn a_failed_transaction_is_retried_because_it_applied_nothing() {
        assert!(transaction_failure().is_retryable());
    }

    #[test]
    fn a_planning_failure_keeps_its_underlying_error() {
        let error = TriggerActionError::Plan {
            source: crate::Error::RetryableSchemaChanged {
                descriptor: "orders".into(),
            },
        };
        match crate::Error::from(error) {
            crate::Error::RetryableSchemaChanged { descriptor } => {
                assert_eq!(descriptor, "orders");
            }
            other => panic!("expected the planner error to survive, got {other:?}"),
        }
    }

    #[test]
    fn a_render_failure_reports_as_a_bad_request() {
        let error = TriggerActionError::Rejected {
            source: TriggerRenderError::UnterminatedQuote,
        };
        assert!(matches!(
            crate::Error::from(error),
            crate::Error::BadRequest { .. }
        ));
    }
}
