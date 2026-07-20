// SPDX-License-Identifier: BUSL-1.1

//! Persisted JIT-provisioned external identity.

#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map, allow_unknown_fields)]
pub struct StoredAuthUser {
    pub id: String,
    pub username: String,
    #[msgpack(default)]
    pub email: String,
    pub tenant_id: u64,
    pub provider: String,
    pub first_seen: u64,
    pub last_seen: u64,
    pub is_active: bool,
    #[msgpack(default = "default_status")]
    pub status: String,
    #[msgpack(default = "default_true")]
    pub is_external: bool,
    #[msgpack(default)]
    pub synced_claims: std::collections::HashMap<String, String>,
}

fn default_status() -> String {
    "active".into()
}

fn default_true() -> bool {
    true
}
