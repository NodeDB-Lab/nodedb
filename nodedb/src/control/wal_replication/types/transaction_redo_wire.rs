// SPDX-License-Identifier: BUSL-1.1

//! Wire shapes carried by [`super::ReplicatedWrite::TransactionRedo`].

use nodedb_physical::physical_plan::CalvinReplySpec;

use crate::event::EventSource;

/// What a committed Calvin slice's redo carries beyond the redo record.
/// The record's `calvin_stamp` names the slice's `(epoch, position)`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CalvinRedoMeta {
    /// The epoch's deterministic instant.
    pub epoch_system_ms: i64,
    /// The reply the leader's stage decided. Every replica renders it after
    /// its install.
    pub reply: CalvinReplySpec,
    /// Whether the slice writes a row the statement names, not only a
    /// derived row.
    pub primary_write: bool,
}

/// One `(collection, primary key) → surrogate` identity a committed
/// transaction's writes carry. Every replica binds it into its own catalog
/// before the redo applies, so a later write by primary key on any replica
/// resolves to the row the redo installed.
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
pub struct ReplicatedIdentity {
    pub collection: String,
    /// The binding key: a document id's bytes, a KV key, a node id's bytes, an
    /// encoded array coordinate, or a surrogate's own big-endian bytes for a
    /// row with no user key.
    pub pk_bytes: Vec<u8>,
    pub surrogate: u32,
}

/// The source a committed entry's writes carry into the Event Plane.
///
/// Every replica stamps the same source, so a transaction a trigger issued
/// does not re-fire that trigger on any replica, and a restored row fires no
/// AFTER trigger on any replica.
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
pub enum ReplicatedEventSource {
    User,
    Trigger,
    RaftFollower,
    CrdtSync,
    Deferred,
    Restore,
    ImplicitClient,
}

impl From<EventSource> for ReplicatedEventSource {
    fn from(source: EventSource) -> Self {
        match source {
            EventSource::User => Self::User,
            EventSource::Trigger => Self::Trigger,
            EventSource::RaftFollower => Self::RaftFollower,
            EventSource::CrdtSync => Self::CrdtSync,
            EventSource::Deferred => Self::Deferred,
            EventSource::Restore => Self::Restore,
            EventSource::ImplicitClient => Self::ImplicitClient,
        }
    }
}

impl From<ReplicatedEventSource> for EventSource {
    fn from(source: ReplicatedEventSource) -> Self {
        match source {
            ReplicatedEventSource::User => Self::User,
            ReplicatedEventSource::Trigger => Self::Trigger,
            ReplicatedEventSource::RaftFollower => Self::RaftFollower,
            ReplicatedEventSource::CrdtSync => Self::CrdtSync,
            ReplicatedEventSource::Deferred => Self::Deferred,
            ReplicatedEventSource::Restore => Self::Restore,
            ReplicatedEventSource::ImplicitClient => Self::ImplicitClient,
        }
    }
}
