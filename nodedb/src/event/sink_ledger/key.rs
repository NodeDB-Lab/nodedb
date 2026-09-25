// SPDX-License-Identifier: BUSL-1.1

//! The identity a sink remembers an applied event by.
//!
//! An event that reproduces a WAL record carries its record position. The
//! position, the row, and the write kind name the event on both delivery
//! paths and across a restart, so a sink that stored this key with its
//! effect can tell a redelivery from a new event. The core that delivered
//! the event is part of the key: each core persists its own watermark, and a
//! key at or below that core's watermark is never delivered again.

use crate::event::record_numbering::is_delete;
use crate::event::types::{RowId, WriteEvent};

/// How a row identity is spelled, so identities of different kinds with the
/// same text stay distinct.
fn row_kind(row: &RowId) -> u8 {
    match row {
        RowId::Row(_) => 0,
        RowId::Batch => 1,
        RowId::Edge(_) => 2,
        RowId::Heartbeat => 3,
    }
}

/// The durable identity of one delivered event.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SinkEventKey {
    pub core: u32,
    pub lsn: u64,
    pub occurrence: u32,
    pub delete: bool,
    pub collection: String,
    pub row_kind: u8,
    pub row: String,
}

impl SinkEventKey {
    /// The key of `event`, delivered by core `core`. `None` for an event that
    /// reproduces no WAL record: it is delivered only once, from the ring,
    /// and never replayed.
    pub fn of(core: usize, event: &WriteEvent) -> Option<Self> {
        let record = event.record?;
        Some(Self {
            core: u32::try_from(core).unwrap_or(u32::MAX),
            lsn: record.lsn.as_u64(),
            occurrence: record.occurrence,
            delete: is_delete(event.op),
            collection: event.collection.to_string(),
            row_kind: row_kind(&event.row_id),
            row: event.row_id.as_str().to_owned(),
        })
    }

    /// A byte encoding that sorts by core, then LSN.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(26 + self.collection.len() + self.row.len());
        out.extend_from_slice(&self.core.to_be_bytes());
        out.extend_from_slice(&self.lsn.to_be_bytes());
        out.extend_from_slice(&self.occurrence.to_be_bytes());
        out.push(u8::from(self.delete));
        out.push(self.row_kind);
        let collection_len = u32::try_from(self.collection.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&collection_len.to_be_bytes());
        out.extend_from_slice(self.collection.as_bytes());
        out.extend_from_slice(self.row.as_bytes());
        out
    }

    /// Decode [`Self::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let core = u32::from_be_bytes(bytes.get(0..4)?.try_into().ok()?);
        let lsn = u64::from_be_bytes(bytes.get(4..12)?.try_into().ok()?);
        let occurrence = u32::from_be_bytes(bytes.get(12..16)?.try_into().ok()?);
        let delete = *bytes.get(16)? != 0;
        let row_kind = *bytes.get(17)?;
        let collection_len = u32::from_be_bytes(bytes.get(18..22)?.try_into().ok()?) as usize;
        let collection_end = 22usize.checked_add(collection_len)?;
        let collection = std::str::from_utf8(bytes.get(22..collection_end)?).ok()?;
        let row = std::str::from_utf8(bytes.get(collection_end..)?).ok()?;
        Some(Self {
            core,
            lsn,
            occurrence,
            delete,
            collection: collection.to_owned(),
            row_kind,
            row: row.to_owned(),
        })
    }

    /// The first key of `core` above `lsn`, for range scans.
    pub fn floor_bytes(core: u32, lsn: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(12);
        out.extend_from_slice(&core.to_be_bytes());
        out.extend_from_slice(&lsn.to_be_bytes());
        out
    }

    /// A hex token of the key, for text that carries it.
    pub fn to_token(&self) -> String {
        self.to_bytes().iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Decode [`Self::to_token`].
    pub fn from_token(token: &str) -> Option<Self> {
        if !token.len().is_multiple_of(2) {
            return None;
        }
        let bytes: Option<Vec<u8>> = (0..token.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(token.get(i..i + 2)?, 16).ok())
            .collect();
        Self::from_bytes(&bytes?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SinkEventKey {
        SinkEventKey {
            core: 2,
            lsn: 77,
            occurrence: 1,
            delete: true,
            collection: "orders".into(),
            row_kind: 0,
            row: "o:1 with spaces".into(),
        }
    }

    #[test]
    fn a_key_survives_bytes_and_tokens() {
        assert_eq!(SinkEventKey::from_bytes(&key().to_bytes()), Some(key()));
        assert_eq!(SinkEventKey::from_token(&key().to_token()), Some(key()));
        assert_eq!(SinkEventKey::from_token("zz"), None);
    }

    #[test]
    fn bytes_sort_by_core_then_lsn() {
        let mut later = key();
        later.lsn = 78;
        assert!(key().to_bytes() < later.to_bytes());
        assert!(SinkEventKey::floor_bytes(2, 77) <= key().to_bytes());
        assert!(SinkEventKey::floor_bytes(2, 78) > key().to_bytes());
    }
}
