//! System identity used to execute trigger bodies.
//!
//! Triggers run with superuser privileges (SECURITY DEFINER model): trigger
//! bodies are database-defined code, not user-submitted queries. The
//! creator's identity is captured separately on `StoredTrigger.owner`.

use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::types::TenantId;

pub(super) fn trigger_identity(tenant_id: TenantId) -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 0,
        username: "_system_trigger".into(),
        tenant_id,
        auth_method: AuthMethod::Trust,
        roles: vec![Role::Superuser],
        is_superuser: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_identity_is_superuser() {
        let id = trigger_identity(TenantId::new(5));
        assert!(id.is_superuser);
        assert_eq!(id.tenant_id, TenantId::new(5));
    }
}
