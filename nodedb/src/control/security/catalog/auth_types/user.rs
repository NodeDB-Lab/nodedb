// SPDX-License-Identifier: BUSL-1.1

//! Persisted local user identity.

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, PartialEq, Eq)]
#[msgpack(map)]
pub struct StoredUser {
    pub user_id: u64,
    pub username: String,
    pub tenant_id: u64,
    pub password_hash: String,
    pub scram_salt: Vec<u8>,
    pub scram_salted_password: Vec<u8>,
    pub roles: Vec<String>,
    pub is_superuser: bool,
    pub is_active: bool,
    pub is_service_account: bool,
    pub created_at: u64,
    pub updated_at: u64,
    pub password_expires_at: u64,
    pub must_change_password: bool,
    pub password_changed_at: u64,
    pub default_database_id: u64,
    pub accessible_databases: Vec<u64>,
}
