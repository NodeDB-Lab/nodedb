// SPDX-License-Identifier: BUSL-1.1

//! Persisted custom role.

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
pub struct StoredRole {
    pub name: String,
    pub tenant_id: u64,
    pub parent: String,
    pub created_at: u64,
}
