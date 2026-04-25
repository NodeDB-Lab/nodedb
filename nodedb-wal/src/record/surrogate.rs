//! Surrogate allocator high-watermark payload.
//!
//! Emitted by `nodedb::control::surrogate::SurrogateRegistry::flush` to make
//! the global surrogate counter crash-recoverable. Replay advances the
//! in-memory counter past `hi`, guaranteeing post-restart allocations never
//! collide with pre-restart ones.
//!
//! Payload layout (fixed 4 bytes, little-endian):
//!
//! ```text
//! ┌────────┐
//! │ hi u32 │
//! └────────┘
//! ```
//!
//! No msgpack framing — the record type already disambiguates the payload,
//! and a fixed LE encoding keeps replay zero-allocation.

use crate::error::{Result, WalError};

/// Size of a surrogate-alloc payload on disk.
pub const SURROGATE_PAYLOAD_SIZE: usize = 4;

/// Surrogate allocator high-watermark — the largest surrogate the allocator
/// has handed out (or will hand out next, depending on flush semantics) at
/// the moment the record was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SurrogateAllocPayload {
    /// High-watermark surrogate value.
    pub hi: u32,
}

impl SurrogateAllocPayload {
    pub const fn new(hi: u32) -> Self {
        Self { hi }
    }

    pub fn to_bytes(&self) -> [u8; SURROGATE_PAYLOAD_SIZE] {
        self.hi.to_le_bytes()
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() != SURROGATE_PAYLOAD_SIZE {
            return Err(WalError::InvalidPayload {
                detail: format!(
                    "SurrogateAlloc payload must be {SURROGATE_PAYLOAD_SIZE} bytes, got {}",
                    buf.len()
                ),
            });
        }
        let hi = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        Ok(Self { hi })
    }
}

/// Surrogate ↔ PK binding payload — emitted on every successful
/// `SurrogateAssigner::assign` allocation so the binding is durable
/// independent of the redb `_system.surrogate_pk{,_rev}` rows.
///
/// On replay, the bind is re-applied to the catalog (idempotent) so a
/// crash between the catalog write and the next checkpoint never
/// loses the binding.
///
/// Wire format (little-endian, no serde dep on `nodedb-wal`):
///
/// ```text
/// ┌──────────────┬──────────────┬──────────────┬──────────────┬───────────┐
/// │ surrogate u32│ name_len u32 │ name bytes   │ pk_len u32   │ pk bytes  │
/// └──────────────┴──────────────┴──────────────┴──────────────┴───────────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurrogateBindPayload {
    pub surrogate: u32,
    pub collection: String,
    pub pk_bytes: Vec<u8>,
}

impl SurrogateBindPayload {
    pub fn new(
        surrogate: u32,
        collection: impl Into<String>,
        pk_bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            surrogate,
            collection: collection.into(),
            pk_bytes: pk_bytes.into(),
        }
    }

    pub fn wire_size(&self) -> usize {
        4 + 4 + self.collection.len() + 4 + self.pk_bytes.len()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let name_bytes = self.collection.as_bytes();
        if name_bytes.len() > u32::MAX as usize {
            return Err(WalError::InvalidPayload {
                detail: format!(
                    "SurrogateBind collection name too long: {}",
                    name_bytes.len()
                ),
            });
        }
        if self.pk_bytes.len() > u32::MAX as usize {
            return Err(WalError::InvalidPayload {
                detail: format!("SurrogateBind pk too long: {}", self.pk_bytes.len()),
            });
        }
        let mut buf = Vec::with_capacity(self.wire_size());
        buf.extend_from_slice(&self.surrogate.to_le_bytes());
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&(self.pk_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.pk_bytes);
        Ok(buf)
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let need = |n: usize, off: usize| {
            if buf.len() < off + n {
                Err(WalError::InvalidPayload {
                    detail: format!(
                        "SurrogateBind truncated: need {n} bytes at offset {off}, have {}",
                        buf.len()
                    ),
                })
            } else {
                Ok(())
            }
        };
        need(4, 0)?;
        let surrogate = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        need(4, 4)?;
        let name_len = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        need(name_len, 8)?;
        let name_end = 8 + name_len;
        let collection =
            std::str::from_utf8(&buf[8..name_end]).map_err(|e| WalError::InvalidPayload {
                detail: format!("SurrogateBind collection not utf8: {e}"),
            })?;
        need(4, name_end)?;
        let pk_len = u32::from_le_bytes([
            buf[name_end],
            buf[name_end + 1],
            buf[name_end + 2],
            buf[name_end + 3],
        ]) as usize;
        let pk_start = name_end + 4;
        need(pk_len, pk_start)?;
        let pk_bytes = buf[pk_start..pk_start + pk_len].to_vec();
        if pk_start + pk_len != buf.len() {
            return Err(WalError::InvalidPayload {
                detail: format!(
                    "SurrogateBind trailing bytes: parsed {}, buffer {}",
                    pk_start + pk_len,
                    buf.len()
                ),
            });
        }
        Ok(Self {
            surrogate,
            collection: collection.to_string(),
            pk_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surrogate_roundtrip() {
        let p = SurrogateAllocPayload::new(0xDEAD_BEEF);
        let bytes = p.to_bytes();
        assert_eq!(SurrogateAllocPayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn surrogate_zero() {
        let p = SurrogateAllocPayload::new(0);
        assert_eq!(SurrogateAllocPayload::from_bytes(&p.to_bytes()).unwrap(), p);
    }

    #[test]
    fn surrogate_max() {
        let p = SurrogateAllocPayload::new(u32::MAX);
        assert_eq!(SurrogateAllocPayload::from_bytes(&p.to_bytes()).unwrap(), p);
    }

    #[test]
    fn surrogate_wrong_size_rejected() {
        assert!(SurrogateAllocPayload::from_bytes(&[0u8; 3]).is_err());
        assert!(SurrogateAllocPayload::from_bytes(&[0u8; 5]).is_err());
    }

    #[test]
    fn bind_payload_roundtrip() {
        let p = SurrogateBindPayload::new(42u32, "users", b"alice".to_vec());
        let bytes = p.to_bytes().unwrap();
        let decoded = SurrogateBindPayload::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, p);
    }

    #[test]
    fn bind_payload_empty_pk_roundtrip() {
        let p = SurrogateBindPayload::new(1u32, "c", Vec::<u8>::new());
        let bytes = p.to_bytes().unwrap();
        assert_eq!(SurrogateBindPayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn bind_payload_binary_pk_roundtrip() {
        let pk: Vec<u8> = (0..=255u8).collect();
        let p = SurrogateBindPayload::new(u32::MAX, "binary", pk);
        let bytes = p.to_bytes().unwrap();
        assert_eq!(SurrogateBindPayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn bind_payload_rejects_garbage() {
        assert!(SurrogateBindPayload::from_bytes(&[0xFFu8; 7]).is_err());
    }
}
