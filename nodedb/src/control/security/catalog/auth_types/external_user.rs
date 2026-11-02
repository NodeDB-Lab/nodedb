// SPDX-License-Identifier: BUSL-1.1

//! Persisted JIT-provisioned external identity.

#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StoredAuthUser {
    pub id: String,
    pub username: String,
    pub email: String,
    pub tenant_id: u64,
    pub provider: String,
    pub first_seen: u64,
    pub last_seen: u64,
    pub is_active: bool,
    pub status: String,
    pub is_external: bool,
    pub synced_claims: std::collections::HashMap<String, String>,
    /// How many times auto-escalation has suspended this account.
    ///
    /// Persisted so the suspend → ban ladder survives a restart: the rolling
    /// violation counters behind it are process-local by design, but the rung
    /// the account has already reached is an enforcement decision and must
    /// not reset when the process does.
    pub escalation_suspensions: u32,
}
