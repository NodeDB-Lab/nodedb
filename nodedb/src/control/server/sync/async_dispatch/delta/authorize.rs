// SPDX-License-Identifier: BUSL-1.1

//! Write authorization for an inbound CRDT delta.

use std::sync::Arc;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::super::super::wire::{
    CompensationHint, DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType,
};

/// Fail closed unless the handshake-bound identity has write access to the
/// target collection. This is used at both the session boundary (before its
/// bookkeeping and validation side effects) and again immediately before plan
/// construction, so a permission revocation between those points cannot reach
/// the Data Plane.
pub(in crate::control::server::sync) fn authorize_delta_write(
    shared: &SharedState,
    identity: Option<&AuthenticatedIdentity>,
    collection: &str,
) -> Result<TenantId, DeltaAuthorizationFailure> {
    let audit = ArcAuditEmitter(Arc::clone(&shared.audit));
    authorize_delta_write_with(
        identity,
        collection,
        &shared.permissions,
        &shared.roles,
        &audit,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::control::server::sync) enum DeltaAuthorizationFailure {
    IdentityNotEstablished,
    PermissionDenied,
}

fn authorize_delta_write_with(
    identity: Option<&AuthenticatedIdentity>,
    collection: &str,
    permissions: &crate::control::security::permission::PermissionStore,
    roles: &crate::control::security::role::RoleStore,
    audit: &dyn crate::control::security::audit::AuditEmitter,
) -> Result<TenantId, DeltaAuthorizationFailure> {
    let identity = identity.ok_or(DeltaAuthorizationFailure::IdentityNotEstablished)?;
    authorize_collection(
        identity,
        identity.default_database.unwrap_or(DatabaseId::DEFAULT),
        collection,
        Permission::Write,
        permissions,
        roles,
        audit,
    )
    .map_err(|_| DeltaAuthorizationFailure::PermissionDenied)?;
    Ok(identity.tenant_id)
}

pub(in crate::control::server::sync) fn permission_denied_delta_reject(
    delta_msg: &DeltaPushMsg,
) -> Option<SyncFrame> {
    let reject = DeltaRejectMsg {
        mutation_id: delta_msg.mutation_id,
        reason: "permission denied".into(),
        compensation: Some(CompensationHint::PermissionDenied),
    };
    SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::NoopAuditEmitter;
    use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;
    use crate::control::security::identity::AuthMethod;
    use crate::control::security::permission::PermissionStore;
    use crate::control::security::role::RoleStore;

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "writer",
            TenantId::new(9),
            AuthMethod::ApiKey,
            Vec::new(),
            None,
            AuthenticatedIdentity::default_database_set(false),
        )
    }

    fn delta() -> DeltaPushMsg {
        DeltaPushMsg {
            collection: "orders".into(),
            document_id: "order-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 42,
            device_id: 0,
            delta_signature: [0; 32],
            collection_incarnation: nodedb_types::Hlc::new(1_000, 0),
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    #[test]
    fn ungranted_delta_is_denied_and_audited_once() {
        let identity = identity();
        let permissions = PermissionStore::new();
        let roles = RoleStore::new();
        let audit = CapturingEmitter::new();

        let result =
            authorize_delta_write_with(Some(&identity), "orders", &permissions, &roles, &audit);

        assert_eq!(result, Err(DeltaAuthorizationFailure::PermissionDenied));
        assert_eq!(audit.recorded().len(), 1);
        let frame =
            permission_denied_delta_reject(&delta()).expect("permission rejection must encode");
        assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
        let reject: DeltaRejectMsg = frame
            .decode_body()
            .expect("permission rejection must decode");
        assert_eq!(
            reject.compensation,
            Some(CompensationHint::PermissionDenied)
        );
    }

    #[test]
    fn missing_identity_fails_closed_without_audit_principal() {
        let permissions = PermissionStore::new();
        let roles = RoleStore::new();
        let audit = CapturingEmitter::new();

        assert_eq!(
            authorize_delta_write_with(None, "orders", &permissions, &roles, &audit),
            Err(DeltaAuthorizationFailure::IdentityNotEstablished)
        );
        assert!(audit.recorded().is_empty());
    }

    #[test]
    fn write_granted_delta_uses_handshake_identity_tenant() {
        let identity = identity();
        let permissions = PermissionStore::new();
        permissions
            .grant(
                "collection:0:9:orders",
                "user:writer",
                Permission::Write,
                "admin",
                None,
            )
            .expect("in-memory grant must succeed");
        let roles = RoleStore::new();
        let tenant_id = authorize_delta_write_with(
            Some(&identity),
            "orders",
            &permissions,
            &roles,
            &NoopAuditEmitter,
        )
        .expect("write grant must authorize dispatch");

        assert_eq!(tenant_id, TenantId::new(9));
    }
}
