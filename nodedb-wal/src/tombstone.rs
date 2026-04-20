//! `CollectionTombstoned` record payload codec.
//!
//! Fixed little-endian wire format (no serde dep pulled into `nodedb-wal`):
//!
//! ```text
//! ┌─────────────┬─────────────┬─────────────┐
//! │ name_len u32│  name bytes │ purge_lsn u64│
//! └─────────────┴─────────────┴─────────────┘
//! ```
//!
//! Tenant id lives on the record header, so it is not repeated here.

use crate::error::{Result, WalError};

/// Maximum collection name length accepted in a tombstone payload.
///
/// Matches the catalog's collection-name limit. Kept here to avoid a
/// cross-crate dep back into `nodedb-types`.
pub const MAX_COLLECTION_NAME_LEN: usize = 255;

/// Parsed tombstone payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionTombstonePayload {
    pub collection: String,
    pub purge_lsn: u64,
}

impl CollectionTombstonePayload {
    pub fn new(collection: impl Into<String>, purge_lsn: u64) -> Self {
        Self {
            collection: collection.into(),
            purge_lsn,
        }
    }

    /// Encoded size in bytes.
    pub fn wire_size(&self) -> usize {
        4 + self.collection.len() + 8
    }

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let name_bytes = self.collection.as_bytes();
        if name_bytes.len() > MAX_COLLECTION_NAME_LEN {
            return Err(WalError::PayloadTooLarge {
                size: name_bytes.len(),
                max: MAX_COLLECTION_NAME_LEN,
            });
        }
        let mut buf = Vec::with_capacity(self.wire_size());
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&self.purge_lsn.to_le_bytes());
        Ok(buf)
    }

    /// Deserialize from bytes. Fails on truncation, oversize name, or non-UTF8.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 4 {
            return Err(WalError::CorruptRecord {
                lsn: 0,
                detail: "tombstone payload shorter than name_len header".into(),
            });
        }
        let name_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if name_len > MAX_COLLECTION_NAME_LEN {
            return Err(WalError::CorruptRecord {
                lsn: 0,
                detail: format!("tombstone name_len {name_len} exceeds max"),
            });
        }
        let need = 4 + name_len + 8;
        if buf.len() < need {
            return Err(WalError::CorruptRecord {
                lsn: 0,
                detail: format!(
                    "tombstone payload truncated: need {need} bytes, have {}",
                    buf.len()
                ),
            });
        }
        let name = std::str::from_utf8(&buf[4..4 + name_len])
            .map_err(|e| WalError::CorruptRecord {
                lsn: 0,
                detail: format!("tombstone name not UTF-8: {e}"),
            })?
            .to_string();
        let purge_lsn = u64::from_le_bytes([
            buf[4 + name_len],
            buf[4 + name_len + 1],
            buf[4 + name_len + 2],
            buf[4 + name_len + 3],
            buf[4 + name_len + 4],
            buf[4 + name_len + 5],
            buf[4 + name_len + 6],
            buf[4 + name_len + 7],
        ]);
        Ok(Self {
            collection: name,
            purge_lsn,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let p = CollectionTombstonePayload::new("users", 42);
        let bytes = p.to_bytes().unwrap();
        assert_eq!(bytes.len(), p.wire_size());
        let decoded = CollectionTombstonePayload::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, p);
    }

    #[test]
    fn rejects_oversize_name() {
        let p = CollectionTombstonePayload::new("x".repeat(MAX_COLLECTION_NAME_LEN + 1), 1);
        assert!(matches!(
            p.to_bytes(),
            Err(WalError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn detects_truncation() {
        let p = CollectionTombstonePayload::new("users", 42);
        let bytes = p.to_bytes().unwrap();
        let short = &bytes[..bytes.len() - 1];
        assert!(matches!(
            CollectionTombstonePayload::from_bytes(short),
            Err(WalError::CorruptRecord { .. })
        ));
    }

    #[test]
    fn detects_corrupt_name_len() {
        let mut bytes = CollectionTombstonePayload::new("users", 1)
            .to_bytes()
            .unwrap();
        bytes[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            CollectionTombstonePayload::from_bytes(&bytes),
            Err(WalError::CorruptRecord { .. })
        ));
    }

    #[test]
    fn empty_name_ok() {
        let p = CollectionTombstonePayload::new("", 7);
        let decoded = CollectionTombstonePayload::from_bytes(&p.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, p);
    }
}
