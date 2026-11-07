// SPDX-License-Identifier: BUSL-1.1

//! The durable record of an audited action outside the catalog, written before the action runs.
//!
//! Catalog changes record their intent in `metadata_proposer`.
//! Store and engine actions have no such chokepoint, so each handler calls this first.
//! A failed record refuses the action, and the action does not run.
//! An action that removes access does not call this, so an audit outage cannot block it.

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::result::DdlError;

/// Record the intent named by `action`, durable before the action runs.
pub(crate) fn record_action_intent(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: Option<TenantId>,
    action: &str,
) -> Result<(), DdlError> {
    state
        .audit_record_strict(
            AuditEvent::ActionIntent,
            tenant_id,
            &identity.username,
            action,
        )
        .map_err(|e| DdlError::from_error_in_context("audit record", &e))
}
