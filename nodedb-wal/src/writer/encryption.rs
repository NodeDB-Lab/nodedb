// SPDX-License-Identifier: Apache-2.0

//! Payload encryption setup for a writer's segment.

use crate::error::{Result, WalError};
use crate::preamble::SegmentPreamble;

use super::core::WalWriter;

impl WalWriter {
    pub(crate) fn can_set_encryption_ring(&self) -> bool {
        self.file_offset == 0 && self.buffer.is_empty()
    }

    /// Set the encryption key. When set, all subsequent records will have
    /// their payloads encrypted with AES-256-GCM.
    ///
    /// Writes the 16-byte WAL segment preamble at the current file offset.
    /// Must be called before the first `append`. Calling it after records
    /// have already been written to this file returns an error.
    pub fn set_encryption_key(&mut self, key: crate::crypto::WalEncryptionKey) -> Result<()> {
        self.set_encryption_ring(crate::crypto::KeyRing::new(key))
    }

    /// Set the key ring directly (for key rotation with dual-key reads).
    ///
    /// Writes the 16-byte WAL segment preamble at the current file offset.
    /// Must be called before the first `append`. Calling it after records
    /// have already been written to this file returns an error.
    pub fn set_encryption_ring(&mut self, ring: crate::crypto::KeyRing) -> Result<()> {
        if self.file_offset != 0 || !self.buffer.is_empty() {
            return Err(WalError::EncryptionError {
                detail: "set_encryption_ring must be called before writing any records".into(),
            });
        }
        let epoch = *ring.current().epoch();
        let preamble = SegmentPreamble::new_wal(epoch);
        let preamble_bytes = preamble.to_bytes();

        // Write preamble into the buffer so it gets flushed with the first
        // record batch (or on the next sync).
        self.buffer.write(&preamble_bytes);

        self.encryption_ring = Some(ring);
        self.segment_preamble = Some(preamble);
        Ok(())
    }

    /// Access the key ring (for decryption during replay).
    pub fn encryption_ring(&self) -> Option<&crate::crypto::KeyRing> {
        self.encryption_ring.as_ref()
    }

    /// The preamble for this segment, if encryption was enabled.
    pub fn segment_preamble(&self) -> Option<&SegmentPreamble> {
        self.segment_preamble.as_ref()
    }
}
