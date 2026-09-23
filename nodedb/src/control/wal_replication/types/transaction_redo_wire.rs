// SPDX-License-Identifier: BUSL-1.1

//! Wire shapes carried by [`super::ReplicatedWrite::TransactionRedo`].

use crate::event::EventSource;

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

/// The source a committed transaction's writes carry into the Event Plane.
///
/// Every replica stamps the same source, so a transaction a trigger issued
/// does not re-fire that trigger on any replica.
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
}

impl From<EventSource> for ReplicatedEventSource {
    fn from(source: EventSource) -> Self {
        match source {
            EventSource::User => Self::User,
            EventSource::Trigger => Self::Trigger,
            EventSource::RaftFollower => Self::RaftFollower,
            EventSource::CrdtSync => Self::CrdtSync,
            EventSource::Deferred => Self::Deferred,
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
        }
    }
}
