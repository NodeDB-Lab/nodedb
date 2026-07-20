// SPDX-License-Identifier: BUSL-1.1

//! Persisted authentication blacklist entry.

#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredBlacklistEntry {
    pub key: String,
    pub kind: String,
    pub reason: String,
    pub created_by: String,
    pub created_at: u64,
    pub expires_at: u64,
}
