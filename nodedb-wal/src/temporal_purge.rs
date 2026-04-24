//! `TemporalPurge` record payload codec.
//!
//! Emitted when the Control Plane's bitemporal-retention scheduler runs
//! an audit-retention pass on a bitemporal collection and successfully
//! drops some number of *superseded* versions below `cutoff_system_ms`.
//!
//! Distinct from `RecordType::Delete`: a `TemporalPurge` never removes
//! the live / current state of a row — only history older than the
//! audit-retain window. Crash recovery MUST treat these separately so a
//! mid-flight purge that was interrupted does not resurface as a delete
//! of the surviving latest version.
//!
//! Fixed little-endian wire format (no serde dep):
//!
//! ```text
//! ┌────────────┬────────────┬───────────┬──────────────────┬────────────┐
//! │engine_tag  │name_len u32│name bytes │ cutoff_ms i64    │ count u64  │
//! │    u8      │            │           │                  │            │
//! └────────────┴────────────┴───────────┴──────────────────┴────────────┘
//! ```
//!
//! Tenant id lives on the record header, so it is not repeated here.

use crate::error::{Result, WalError};
use crate::tombstone::MAX_COLLECTION_NAME_LEN;

/// Which engine produced the purge. Wire-stable — do not renumber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TemporalPurgeEngine {
    EdgeStore = 1,
    DocumentStrict = 2,
    Columnar = 3,
}

impl TemporalPurgeEngine {
    pub fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            1 => Some(Self::EdgeStore),
            2 => Some(Self::DocumentStrict),
            3 => Some(Self::Columnar),
            _ => None,
        }
    }
}

/// Parsed temporal-purge payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalPurgePayload {
    pub engine: TemporalPurgeEngine,
    pub collection: String,
    pub cutoff_system_ms: i64,
    pub purged_count: u64,
}

impl TemporalPurgePayload {
    pub fn new(
        engine: TemporalPurgeEngine,
        collection: impl Into<String>,
        cutoff_system_ms: i64,
        purged_count: u64,
    ) -> Self {
        Self {
            engine,
            collection: collection.into(),
            cutoff_system_ms,
            purged_count,
        }
    }

    pub fn wire_size(&self) -> usize {
        1 + 4 + self.collection.len() + 8 + 8
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let name_bytes = self.collection.as_bytes();
        if name_bytes.len() > MAX_COLLECTION_NAME_LEN {
            return Err(WalError::PayloadTooLarge {
                size: name_bytes.len(),
                max: MAX_COLLECTION_NAME_LEN,
            });
        }
        let mut buf = Vec::with_capacity(self.wire_size());
        buf.push(self.engine as u8);
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&self.cutoff_system_ms.to_le_bytes());
        buf.extend_from_slice(&self.purged_count.to_le_bytes());
        Ok(buf)
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 1 + 4 {
            return Err(WalError::CorruptRecord {
                lsn: 0,
                detail: "temporal-purge payload shorter than engine_tag + name_len".into(),
            });
        }
        let engine =
            TemporalPurgeEngine::from_raw(buf[0]).ok_or_else(|| WalError::CorruptRecord {
                lsn: 0,
                detail: format!("temporal-purge unknown engine_tag {}", buf[0]),
            })?;
        let name_len = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        if name_len > MAX_COLLECTION_NAME_LEN {
            return Err(WalError::CorruptRecord {
                lsn: 0,
                detail: format!("temporal-purge name_len {name_len} exceeds max"),
            });
        }
        let need = 1 + 4 + name_len + 8 + 8;
        if buf.len() < need {
            return Err(WalError::CorruptRecord {
                lsn: 0,
                detail: format!(
                    "temporal-purge payload truncated: need {need} bytes, have {}",
                    buf.len()
                ),
            });
        }
        let name_end = 5 + name_len;
        let collection = std::str::from_utf8(&buf[5..name_end])
            .map_err(|e| WalError::CorruptRecord {
                lsn: 0,
                detail: format!("temporal-purge collection not utf8: {e}"),
            })?
            .to_string();
        let cutoff_system_ms = i64::from_le_bytes(
            buf[name_end..name_end + 8]
                .try_into()
                .expect("bounded above"),
        );
        let purged_count = u64::from_le_bytes(
            buf[name_end + 8..name_end + 16]
                .try_into()
                .expect("bounded above"),
        );
        Ok(Self {
            engine,
            collection,
            cutoff_system_ms,
            purged_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let p =
            TemporalPurgePayload::new(TemporalPurgeEngine::EdgeStore, "users", 1_000_000_000, 42);
        let bytes = p.to_bytes().unwrap();
        let decoded = TemporalPurgePayload::from_bytes(&bytes).unwrap();
        assert_eq!(p, decoded);
    }

    #[test]
    fn all_engine_tags_roundtrip() {
        for e in [
            TemporalPurgeEngine::EdgeStore,
            TemporalPurgeEngine::DocumentStrict,
            TemporalPurgeEngine::Columnar,
        ] {
            let p = TemporalPurgePayload::new(e, "c", 0, 0);
            let b = p.to_bytes().unwrap();
            assert_eq!(TemporalPurgePayload::from_bytes(&b).unwrap().engine, e);
        }
    }

    #[test]
    fn rejects_unknown_engine_tag() {
        let mut buf = TemporalPurgePayload::new(TemporalPurgeEngine::EdgeStore, "c", 0, 0)
            .to_bytes()
            .unwrap();
        buf[0] = 99;
        assert!(TemporalPurgePayload::from_bytes(&buf).is_err());
    }

    #[test]
    fn rejects_truncated() {
        let full = TemporalPurgePayload::new(TemporalPurgeEngine::Columnar, "users", 1, 1)
            .to_bytes()
            .unwrap();
        for cut in 0..full.len() {
            assert!(TemporalPurgePayload::from_bytes(&full[..cut]).is_err());
        }
    }

    #[test]
    fn rejects_oversize_name() {
        let long = "a".repeat(MAX_COLLECTION_NAME_LEN + 1);
        let p = TemporalPurgePayload::new(TemporalPurgeEngine::EdgeStore, long, 0, 0);
        assert!(p.to_bytes().is_err());
    }
}
