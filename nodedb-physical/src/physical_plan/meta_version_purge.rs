// SPDX-License-Identifier: Apache-2.0

//! Types carried by `MetaOp::PurgeVersionsBelow`.

/// One collection whose history the cleanup horizon does not govern: a
/// `bitemporal=true` collection, which keeps history for its audit window.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RetainedHistory {
    pub database_id: u64,
    pub tenant_id: u64,
    /// The collection's bare catalog name.
    pub collection: String,
    /// The ordinal its history is purged below: the last ordinal of
    /// `now - audit_retain_ms`, capped at the cleanup horizon. `None` keeps
    /// the whole history.
    pub horizon: Option<i64>,
}

/// One read-only mirror's limit on the cleanup horizon. A mirror's reads stay at
/// or below its applied cut, however far that trails the clock, so no store of
/// the database purges past this ordinal.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct MirrorHistoryFloor {
    pub database_id: u64,
    pub floor: i64,
}

/// The key of the count of stores still due that every core answers a
/// `MetaOp::PurgeVersionsBelow` with.
pub const PURGE_REMAINING_KEY: &str = "remaining";
