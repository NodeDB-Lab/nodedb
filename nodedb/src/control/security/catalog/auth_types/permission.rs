// SPDX-License-Identifier: BUSL-1.1

//! Persisted permission grant.

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
pub struct StoredPermission {
    pub target: String,
    pub grantee: String,
    pub permission: String,
    pub granted_by: String,
    pub granted_at: u64,
}
