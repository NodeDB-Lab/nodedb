// SPDX-License-Identifier: BUSL-1.1

//! Why a planned statement stopped, before any protocol renders it.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::control::server::response_shape::types::DmlFoldError;
use crate::types::TenantId;

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
}

impl StatementError {
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
        }
    }
}
