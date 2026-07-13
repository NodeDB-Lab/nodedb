//! Transport-neutral authorization failures.

use crate::types::TenantId;

/// A safe, transport-neutral authorization denial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationError {
    tenant_id: TenantId,
    resource: String,
}

impl AuthorizationError {
    pub fn new(tenant_id: TenantId, resource: impl Into<String>) -> Self {
        Self {
            tenant_id,
            resource: resource.into(),
        }
    }

    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    pub fn resource(&self) -> &str {
        &self.resource
    }
}

impl From<AuthorizationError> for crate::Error {
    fn from(error: AuthorizationError) -> Self {
        Self::RejectedAuthz {
            tenant_id: error.tenant_id,
            resource: error.resource,
        }
    }
}
