// SPDX-License-Identifier: BUSL-1.1

//! Persisted object ownership.

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
pub struct StoredOwner {
    pub object_type: String,
    pub object_name: String,
    pub tenant_id: u64,
    pub owner_username: String,
}

pub mod object_type {
    pub const COLLECTION: &str = "collection";
    pub const FUNCTION: &str = "function";
    pub const PROCEDURE: &str = "procedure";
    pub const TRIGGER: &str = "trigger";
    pub const MATERIALIZED_VIEW: &str = "materialized_view";
    pub const SEQUENCE: &str = "sequence";
    pub const SCHEDULE: &str = "schedule";
    pub const CHANGE_STREAM: &str = "change_stream";
    pub const CONTINUOUS_AGGREGATE: &str = "continuous_aggregate";
    pub const INDEX: &str = "index";
}
