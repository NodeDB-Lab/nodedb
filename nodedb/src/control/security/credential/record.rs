// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::id::DatabaseId;

use crate::config::auth::Argon2Config;
use crate::types::TenantId;

use super::super::catalog::StoredUser;
use super::super::identity::Role;
use super::hash::{VerifyOutcome, verify_argon2_with_rehash};

const CREDENTIAL_INTEGRITY_DETAIL: &str = "stored credential integrity check failed";

/// Validate persisted password credentials before they enter the in-memory cache.
///
/// Service accounts intentionally authenticate without a password, so their
/// password material is not subject to this check.
pub(in crate::control::security::credential) fn validate_stored_user_credentials(
    stored: &StoredUser,
    argon2_config: &Argon2Config,
) -> crate::Result<()> {
    if stored.is_service_account {
        return Ok(());
    }

    if stored.password_hash.is_empty()
        || matches!(
            verify_argon2_with_rehash(&stored.password_hash, "", argon2_config),
            VerifyOutcome::Ok { rehash: _ }
        )
    {
        return Err(crate::Error::BadRequest {
            detail: CREDENTIAL_INTEGRITY_DETAIL.into(),
        });
    }

    Ok(())
}

/// A stored user record (in-memory cache).
#[derive(Debug, Clone)]
pub struct UserRecord {
    pub user_id: u64,
    pub username: String,
    pub tenant_id: TenantId,
    /// Argon2id password hash (PHC string format).
    pub password_hash: String,
    /// Salt used for SCRAM-SHA-256 (16 bytes).
    pub scram_salt: Vec<u8>,
    /// SCRAM-SHA-256 salted password (for pgwire auth).
    pub scram_salted_password: Vec<u8>,
    pub roles: Vec<Role>,
    pub is_superuser: bool,
    pub is_active: bool,
    /// True if this is a service account (no password, API key auth only).
    pub is_service_account: bool,
    /// Unix timestamp (seconds) when the user was created.
    pub created_at: u64,
    /// Unix timestamp (seconds) when the user was last modified.
    pub updated_at: u64,
    /// Unix timestamp (seconds) when the password expires. 0 = no expiry.
    pub password_expires_at: u64,
    /// If true, the user must change their password before logging in.
    pub must_change_password: bool,
    /// Unix timestamp (seconds) when the password was last changed.
    pub password_changed_at: u64,
    /// The database ID this user connects to by default. `0` means server default.
    pub default_database_id: u64,
    /// Database IDs this account may access. Ignored for regular users (they use
    /// `_system.database_grants`). For service accounts: authoritative. Empty
    /// reads as `[DatabaseId::DEFAULT]` at auth time.
    pub accessible_databases: Vec<DatabaseId>,
}

impl UserRecord {
    pub(in crate::control::security::credential) fn to_stored(&self) -> StoredUser {
        StoredUser {
            user_id: self.user_id,
            username: self.username.clone(),
            tenant_id: self.tenant_id.as_u64(),
            password_hash: self.password_hash.clone(),
            scram_salt: self.scram_salt.clone(),
            scram_salted_password: self.scram_salted_password.clone(),
            roles: self.roles.iter().map(|r| r.to_string()).collect(),
            is_superuser: self.is_superuser,
            is_active: self.is_active,
            is_service_account: self.is_service_account,
            created_at: self.created_at,
            updated_at: self.updated_at,
            password_expires_at: self.password_expires_at,
            must_change_password: self.must_change_password,
            password_changed_at: self.password_changed_at,
            default_database_id: self.default_database_id,
            accessible_databases: self
                .accessible_databases
                .iter()
                .map(|id| id.as_u64())
                .collect(),
        }
    }

    pub(in crate::control::security::credential) fn from_stored(s: StoredUser) -> Self {
        let roles: Vec<Role> = s
            .roles
            .iter()
            .map(|r| r.parse().unwrap_or(Role::ReadOnly))
            .collect();
        let accessible_databases = s
            .accessible_databases
            .iter()
            .map(|&id| DatabaseId::new(id))
            .collect();
        Self {
            user_id: s.user_id,
            username: s.username,
            tenant_id: TenantId::new(s.tenant_id),
            password_hash: s.password_hash,
            scram_salt: s.scram_salt,
            scram_salted_password: s.scram_salted_password,
            is_superuser: s.is_superuser,
            is_active: s.is_active,
            is_service_account: s.is_service_account,
            created_at: s.created_at,
            updated_at: s.updated_at,
            password_expires_at: s.password_expires_at,
            must_change_password: s.must_change_password,
            password_changed_at: s.password_changed_at,
            default_database_id: s.default_database_id,
            accessible_databases,
            roles,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `StoredUser` map without the trailing fields fails to decode.
    #[test]
    fn stored_user_missing_fields_is_refused() {
        let created_at_epoch: u64 = 1_700_000_000;
        let mut buf = Vec::new();
        rmpv::encode::write_value(
            &mut buf,
            &rmpv::Value::Map(vec![
                (
                    rmpv::Value::String("user_id".into()),
                    rmpv::Value::Integer(rmpv::Integer::from(42u64)),
                ),
                (
                    rmpv::Value::String("username".into()),
                    rmpv::Value::String("partial_user".into()),
                ),
                (
                    rmpv::Value::String("tenant_id".into()),
                    rmpv::Value::Integer(rmpv::Integer::from(1u32)),
                ),
                (
                    rmpv::Value::String("password_hash".into()),
                    rmpv::Value::String("$argon2id$fake_hash".into()),
                ),
                (
                    rmpv::Value::String("scram_salt".into()),
                    rmpv::Value::Binary(vec![1, 2, 3]),
                ),
                (
                    rmpv::Value::String("scram_salted_password".into()),
                    rmpv::Value::Binary(vec![4, 5, 6]),
                ),
                (
                    rmpv::Value::String("roles".into()),
                    rmpv::Value::Array(vec![rmpv::Value::String("read_write".into())]),
                ),
                (
                    rmpv::Value::String("is_superuser".into()),
                    rmpv::Value::Boolean(false),
                ),
                (
                    rmpv::Value::String("is_active".into()),
                    rmpv::Value::Boolean(true),
                ),
                (
                    rmpv::Value::String("created_at".into()),
                    rmpv::Value::Integer(rmpv::Integer::from(created_at_epoch)),
                ),
            ]),
        )
        .expect("encode partial StoredUser");

        assert!(zerompk::from_msgpack::<StoredUser>(&buf).is_err());
    }
}
