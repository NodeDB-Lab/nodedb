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
}
