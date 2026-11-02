// SPDX-License-Identifier: BUSL-1.1

//! Persisted API-key identity.

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct StoredApiKey {
    pub key_id: String,
    pub secret_hash: Vec<u8>,
    pub username: String,
    pub user_id: u64,
    pub tenant_id: u64,
    pub expires_at: u64,
    pub is_revoked: bool,
    pub created_at: u64,
    pub scope: Vec<String>,
    pub accessible_databases: Vec<u64>,
}
