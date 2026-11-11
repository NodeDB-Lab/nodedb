// SPDX-License-Identifier: BUSL-1.1

//! Why a planned statement stopped, before any protocol renders it.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::server::response_shape::types::DmlFoldError;
use crate::control::server::shared::retry::RetryableSchemaChange;
use crate::control::server::shared::sql_split::statement_position;
use crate::types::TenantId;

use super::StatementMap;

/// Why a statement stopped. Each protocol renders it in its own error shape.
pub(crate) enum StatementError {
    /// A Control-Plane error.
    Error(crate::Error),
    /// A spent hard quota refused a task before it ran.
    Quota(crate::Error),
    /// A task names a tenant other than the statement's.
    TenantIsolation,
    /// The Data Plane refused a staged write. `None` when it gave no code.
    Rejected(Option<ErrorCode>),
    /// The Data Plane answered a dispatched task with an error status.
    Response(Box<Response>),
    /// Two tasks of the statement report verbs that cannot share one tag.
    DmlFold(DmlFoldError),
    /// A task's answer cannot be shaped.
    Shape(nodedb_types::NodeDbError),
    /// `error` stopped statement `statement` of a request of `statements`
    /// statements. Only a request of several statements wraps its errors.
    InStatement {
        /// The statement's 0-based index in its request.
        statement: usize,
        /// How many statements the request holds.
        statements: usize,
        error: Box<StatementError>,
    },
}

impl StatementError {
    /// The prefix a protocol puts before the message of an error that
    /// stopped statement `statement` of `statements`. The prefix counts
    /// statements from 1: `statement 2 of 2: `.
    pub(crate) fn statement_prefix(statement: usize, statements: usize) -> String {
        format!(
            "statement {} of {statements}: ",
            statement.saturating_add(1)
        )
    }

    /// The Control-Plane error a protocol with no finer error shape renders.
    /// `tenant_id` is the statement's tenant.
    pub(crate) fn into_error(self, tenant_id: TenantId) -> crate::Error {
        match self {
            Self::Error(error) | Self::Quota(error) => error,
            Self::TenantIsolation => crate::Error::RejectedAuthz {
                tenant_id,
                resource: "tenant isolation violation".into(),
            },
            Self::Rejected(Some(code)) => crate::Error::DataPlane(code),
            Self::Rejected(None) => crate::Error::Internal {
                detail: "unknown data plane error".into(),
            },
            Self::Response(response) => match response.error_code {
                Some(code) => crate::Error::DataPlane(*code),
                None => crate::Error::Internal {
                    detail: "data plane returned an error status with no error code".into(),
                },
            },
            Self::DmlFold(error) => crate::Error::Internal {
                detail: error.to_string(),
            },
            Self::Shape(error) => crate::Error::from(error),
            // `crate::Error` has no slot for the statement.
            // [`Self::into_request_error`] carries it.
            Self::InStatement { error, .. } => error.into_error(tenant_id),
        }
    }

    /// [`Self::into_error`] with the statement an `InStatement` names.
    pub(crate) fn into_request_error(self, tenant_id: TenantId) -> RequestError {
        match self {
            Self::InStatement {
                statement,
                statements,
                error,
            } => RequestError::in_statement(error.into_error(tenant_id), statement, statements),
            error => RequestError::from(error.into_error(tenant_id)),
        }
    }
}

/// The statement an error stopped, in a request of several statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementAt {
    /// The statement's 0-based index in its request.
    pub statement: usize,
    /// How many statements the request holds.
    pub statements: usize,
}

impl StatementAt {
    /// Statement `statement` of `statements`. `None` for a request of one
    /// statement: its error carries no index.
    pub fn of(statement: usize, statements: usize) -> Option<Self> {
        (statements > 1).then_some(Self {
            statement,
            statements,
        })
    }

    /// The statement of task `task` by `map`, in a request of `statements`
    /// statements. `None` for a request of one statement or a task with no
    /// entry.
    pub fn of_task(map: &StatementMap, task: usize, statements: usize) -> Option<Self> {
        let statement = usize::try_from(map.statement_of(task)?).ok()?;
        Self::of(statement, statements)
    }

    /// The 1-based statement number a client reads.
    pub fn number(self) -> usize {
        self.statement.saturating_add(1)
    }

    /// The 1-based character offset in `sql` where the statement starts.
    /// `sql` is the request text. `None` when it holds fewer statements.
    pub fn position(self, sql: &str) -> Option<usize> {
        statement_position(sql, self.statement)
    }
}

/// A Control-Plane error and the statement it stopped.
#[derive(Debug)]
pub struct RequestError {
    /// Boxed so the error stays pointer-sized. Every `Result` that returns
    /// this type then reserves little on the success path.
    pub error: Box<crate::Error>,
    /// `None` for a request of one statement, and for an error of the whole
    /// request.
    pub at: Option<StatementAt>,
}

impl RequestError {
    /// `error`, raised by statement `statement` (0-based) of `statements`.
    pub fn in_statement(error: crate::Error, statement: usize, statements: usize) -> Self {
        Self {
            error: Box::new(error),
            at: StatementAt::of(statement, statements),
        }
    }

    /// `error`, raised by task `task`. `map` names each task's statement.
    pub fn in_task(
        error: crate::Error,
        map: &StatementMap,
        task: usize,
        statements: usize,
    ) -> Self {
        Self {
            error: Box::new(error),
            at: StatementAt::of_task(map, task, statements),
        }
    }

    /// The error with no statement.
    pub fn into_inner(self) -> crate::Error {
        *self.error
    }
}

impl From<crate::Error> for RequestError {
    fn from(error: crate::Error) -> Self {
        Self {
            error: Box::new(error),
            at: None,
        }
    }
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl RetryableSchemaChange for RequestError {
    fn retryable_descriptor(&self) -> Option<&str> {
        self.error.retryable_descriptor()
    }
}

/// A protocol with a statement slot in its error shape renders the
/// statement as an `InStatement`.
impl From<RequestError> for StatementError {
    fn from(error: RequestError) -> Self {
        match error.at {
            Some(at) => Self::InStatement {
                statement: at.statement,
                statements: at.statements,
                error: Box::new(Self::Error(*error.error)),
            },
            None => Self::Error(*error.error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn internal() -> crate::Error {
        crate::Error::Internal { detail: "x".into() }
    }

    #[test]
    fn an_error_of_a_request_of_one_statement_names_no_statement() {
        assert_eq!(RequestError::in_statement(internal(), 0, 1).at, None);
        let bare = StatementError::from(RequestError::in_statement(internal(), 0, 1));
        assert!(matches!(bare, StatementError::Error(_)));
    }

    #[test]
    fn an_in_statement_error_keeps_its_statement() {
        let wrapped = StatementError::InStatement {
            statement: 1,
            statements: 2,
            error: Box::new(StatementError::TenantIsolation),
        };
        let error = wrapped.into_request_error(TenantId::new(1));
        assert_eq!(error.at, StatementAt::of(1, 2));
        assert!(matches!(
            StatementError::from(error),
            StatementError::InStatement {
                statement: 1,
                statements: 2,
                ..
            }
        ));
    }

    #[test]
    fn a_statement_number_and_position_count_from_one() {
        let at = StatementAt::of(1, 2).expect("two statements carry an index");
        assert_eq!(at.number(), 2);
        assert_eq!(at.position("SELECT 1; SELECT 2"), Some(11));
    }
}
