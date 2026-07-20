// SPDX-License-Identifier: BUSL-1.1

//! Persisted audit-log entry.

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map, allow_unknown_fields)]
pub struct StoredAuditEntry {
    pub seq: u64,
    pub timestamp_us: u64,
    pub event: String,
    pub tenant_id: Option<u64>,
    #[msgpack(default)]
    pub database_id: Option<u64>,
    pub source: String,
    pub detail: String,
    #[msgpack(default)]
    pub prev_hash: String,
}
