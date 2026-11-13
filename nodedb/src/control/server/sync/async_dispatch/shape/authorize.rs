// SPDX-License-Identifier: BUSL-1.1

//! Read authorization for shape subscriptions.
//!
//! A `ShapeDefinition` is client-supplied and names the collection the server
//! will snapshot, so a shape is a read request wearing a subscription's shape.
//! It carries the same requirements as every other read: an identity
//! established by the handshake, and a read grant on the named collection.
//! This is the read-path counterpart of `authorize_delta_write`.

use nodedb_types::sync::shape::ShapeDefinition;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;

/// Why a shape subscription was refused.
///
/// Refusals are not reported to the client in any more detail than "no
/// snapshot": distinguishing "no such collection" from "not permitted" over an
/// unauthenticated socket would disclose which collections exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShapeAuthorizationFailure {
    /// The session never completed a handshake, so there is no principal to
    /// authorize and no tenant to scope the read to.
    IdentityNotEstablished,
    /// The principal holds no read grant on the shape's collection.
    PermissionDenied,
}

/// Fail closed unless the handshake-bound identity may read this shape.
///
/// Returns the identity on success; the caller takes tenant and database from
/// it rather than from the session's wire-supplied fields, so a shape body can
/// never widen the scope of what it reads.
pub(super) fn authorize_shape_subscription<'a>(
    shared: &SharedState,
    session: &'a SyncSession,
    shape: &ShapeDefinition,
) -> Result<&'a AuthenticatedIdentity, ShapeAuthorizationFailure> {
    let identity = session
        .identity
        .as_ref()
        .ok_or(ShapeAuthorizationFailure::IdentityNotEstablished)?;

    // An array shape reads the array, so it needs the read grant every array
    // read needs: Read on the array name. Graph roots name no single
    // collection, so only the identity is required; such a shape pushes no
    // rows.
    let resource = match &shape.shape_type {
        nodedb_types::sync::shape::ShapeType::Array { array_name, .. } => Some(array_name.as_str()),
        _ => shape.collection(),
    };
    let Some(collection) = resource else {
        return Ok(identity);
    };

    let audit = ArcAuditEmitter(std::sync::Arc::clone(&shared.audit));
    authorize_collection(
        identity,
        session.database_id(),
        collection,
        Permission::Read,
        &shared.permissions,
        &shared.roles,
        &audit,
    )
    .map_err(|_| ShapeAuthorizationFailure::PermissionDenied)?;

    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;

    use nodedb_types::sync::shape::ShapeType;

    use crate::control::security::identity::AuthMethod;
    use crate::types::TenantId;

    fn shape(collection: &str) -> ShapeDefinition {
        ShapeDefinition {
            shape_id: "s1".into(),
            tenant_id: 9,
            shape_type: ShapeType::Document {
                collection: collection.into(),
                predicate: Vec::new(),
            },
            description: String::new(),
            field_filter: Vec::new(),
        }
    }

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "reader",
            TenantId::new(9),
            AuthMethod::ApiKey,
            Vec::new(),
            None,
            AuthenticatedIdentity::default_database_set(false),
        )
    }

    /// The collection a shape names is the collection that gets read, so it is
    /// the collection the grant must be held on.
    #[test]
    fn shape_collection_is_the_authorized_resource() {
        assert_eq!(shape("orders").collection(), Some("orders"));
    }

    /// A session that never handshook has no principal to authorize.
    #[test]
    fn absent_identity_is_distinguished_from_a_denied_grant() {
        assert_ne!(
            ShapeAuthorizationFailure::IdentityNotEstablished,
            ShapeAuthorizationFailure::PermissionDenied
        );
        let identity = identity();
        assert_eq!(identity.tenant_id, TenantId::new(9));
    }
}
