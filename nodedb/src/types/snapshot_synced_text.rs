// SPDX-License-Identifier: BUSL-1.1

//! The stored sync-frame text a snapshot carries.

/// One stored sync-frame text version of a document.
///
/// A Lite client's `FtsIndex` frame carries text whose body the document
/// store does not always hold. The receiving core stores every frame as a
/// version, and a text-index rebuild replays them. A snapshot carries them
/// so a restored or joined node re-indexes that text with its history.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Default,
)]
pub struct SyncedTextEntry {
    /// Database the collection lives in.
    pub database_id: u64,
    /// Tenant owning the collection.
    pub tenant_id: u64,
    /// The collection as the Data Plane stores it.
    pub collection: String,
    /// The document's surrogate.
    pub surrogate: u32,
    /// The system-time ordinal the frame indexed at.
    pub ordinal: i64,
    /// Every `(field, text)` pair the frame carried. Empty for a tombstone:
    /// a sync delete, or a frame with no pairs.
    pub fields: Vec<(String, String)>,
}
