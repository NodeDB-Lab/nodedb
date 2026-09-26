// SPDX-License-Identifier: Apache-2.0

//! `WalRecord` — header + payload with encryption + checksum helpers.

use super::header::{
    ENCRYPTED_FLAG, HEADER_SIZE, MAX_WAL_PAYLOAD_SIZE, NO_EVENT_SOURCE, RecordHeader,
    WAL_FORMAT_VERSION, WAL_MAGIC,
};
use crate::error::{Result, WalError};
use crate::preamble::PREAMBLE_SIZE;

/// A complete WAL record: header + payload.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub header: RecordHeader,
    pub payload: Vec<u8>,
}

/// The header fields of a record its caller decides: its type, scope and
/// event source. The writer assigns the LSN.
#[derive(Debug, Clone, Copy)]
pub struct RecordTarget {
    pub record_type: u32,
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub database_id: u64,
    /// The event source code of the row write the record carries.
    /// [`NO_EVENT_SOURCE`] for a record that carries no row write.
    pub event_source: u8,
}

/// The header fields that tie a record to the write that appended it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordStamp {
    /// The idempotency key of the replicated proposal whose apply appended
    /// the record. `0` when no proposal apply appended it.
    pub apply_key: u64,
    /// The event source code of the row write the record carries.
    pub event_source: u8,
}

impl RecordStamp {
    /// No proposal key and no row write.
    pub const NONE: Self = Self {
        apply_key: 0,
        event_source: NO_EVENT_SOURCE,
    };
}

/// Parameters for [`WalRecord::new`].
pub struct WalRecordArgs<'a> {
    pub record_type: u32,
    pub lsn: u64,
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub database_id: u64,
    pub payload: Vec<u8>,
    pub encryption_key: Option<&'a crate::crypto::WalEncryptionKey>,
    pub preamble_bytes: Option<&'a [u8; PREAMBLE_SIZE]>,
}

impl WalRecord {
    /// Create a new WAL record with computed CRC32C.
    ///
    /// If `encryption_key` is provided, the payload is encrypted before
    /// CRC computation. The ciphertext includes a 16-byte auth tag.
    ///
    /// `preamble_bytes` — when encryption is active, the 16-byte segment
    /// preamble that was written at offset 0 of this segment file. It is
    /// concatenated with the record header bytes to form the AAD, binding
    /// the ciphertext to its segment (preamble-swap defense). Pass `None`
    /// for unencrypted records (the argument is ignored in that case).
    ///
    /// `database_id` is stored in header bytes 34-41 (previously reserved,
    /// zero-filled). Pre-existing records with zeros decode to `DatabaseId(0)`
    /// (the default database), preserving backward compatibility.
    pub fn new(args: WalRecordArgs<'_>) -> Result<Self> {
        Self::new_stamped(args, RecordStamp::NONE)
    }

    /// [`Self::new`] with the proposal key and event source of `stamp`. Both
    /// ride the header, inside the CRC and the encryption AAD, so the record
    /// and its stamp are durable together.
    pub fn new_stamped(args: WalRecordArgs<'_>, stamp: RecordStamp) -> Result<Self> {
        let RecordStamp {
            apply_key,
            event_source,
        } = stamp;
        let WalRecordArgs {
            record_type,
            lsn,
            tenant_id,
            vshard_id,
            database_id,
            payload,
            encryption_key,
            preamble_bytes,
        } = args;
        if payload.len() > MAX_WAL_PAYLOAD_SIZE {
            return Err(WalError::PayloadTooLarge {
                size: payload.len(),
                max: MAX_WAL_PAYLOAD_SIZE,
            });
        }

        let (final_payload, encrypted) = if let Some(key) = encryption_key {
            let temp_header = RecordHeader {
                magic: WAL_MAGIC,
                format_version: WAL_FORMAT_VERSION,
                record_type,
                lsn,
                tenant_id,
                vshard_id,
                payload_len: 0,
                database_id,
                apply_key,
                event_source,
                crc32c: 0,
            };
            let header_bytes = temp_header.to_bytes();
            // AAD = preamble_bytes || header_bytes — binds ciphertext to both
            // the segment it lives in and the record header it belongs to.
            let aad = build_aad(preamble_bytes, &header_bytes);
            let ciphertext = key.encrypt_aad(lsn, &aad, &payload)?;
            (ciphertext, true)
        } else {
            (payload, false)
        };

        let record_type = if encrypted {
            record_type | ENCRYPTED_FLAG
        } else {
            record_type
        };

        let mut header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type,
            lsn,
            tenant_id,
            vshard_id,
            payload_len: final_payload.len() as u32,
            database_id,
            apply_key,
            event_source,
            crc32c: 0,
        };

        header.crc32c = header.compute_checksum(&final_payload);

        Ok(Self {
            header,
            payload: final_payload,
        })
    }

    /// The idempotency key of the proposal whose apply appended this record,
    /// `0` when no proposal apply appended it.
    pub fn apply_key(&self) -> u64 {
        self.header.apply_key
    }

    /// The event source code of the row write this record carries.
    /// [`NO_EVENT_SOURCE`] for a record that carries no row write.
    pub fn event_source(&self) -> u8 {
        self.header.event_source
    }

    /// Decrypt the payload if the record is encrypted.
    ///
    /// `epoch` must come from the on-disk segment preamble, not from the
    /// current in-memory key. `preamble_bytes` must be the same 16-byte
    /// preamble that was used as part of the AAD during encryption.
    pub fn decrypt_payload(
        &self,
        epoch: &[u8; 4],
        preamble_bytes: Option<&[u8; PREAMBLE_SIZE]>,
        encryption_key: Option<&crate::crypto::WalEncryptionKey>,
    ) -> Result<Vec<u8>> {
        if !self.is_encrypted() {
            return Ok(self.payload.clone());
        }

        let key = encryption_key.ok_or_else(|| WalError::EncryptionError {
            detail: "record is encrypted but no decryption key provided".into(),
        })?;

        let mut aad_header = self.header;
        aad_header.record_type &= !ENCRYPTED_FLAG;
        aad_header.payload_len = 0;
        aad_header.crc32c = 0;
        let header_bytes = aad_header.to_bytes();
        let aad = build_aad(preamble_bytes, &header_bytes);

        key.decrypt_aad(epoch, self.header.lsn, &aad, &self.payload)
    }

    /// Decrypt the payload using a key ring (supports dual-key rotation).
    ///
    /// `epoch` must come from the on-disk segment preamble. `preamble_bytes`
    /// must match the preamble bytes written at the start of this segment.
    pub fn decrypt_payload_ring(
        &self,
        epoch: &[u8; 4],
        preamble_bytes: Option<&[u8; PREAMBLE_SIZE]>,
        ring: Option<&crate::crypto::KeyRing>,
    ) -> Result<Vec<u8>> {
        if !self.is_encrypted() {
            return Ok(self.payload.clone());
        }

        let ring = ring.ok_or_else(|| WalError::EncryptionError {
            detail: "record is encrypted but no decryption key ring provided".into(),
        })?;

        let mut aad_header = self.header;
        aad_header.record_type &= !ENCRYPTED_FLAG;
        aad_header.payload_len = 0;
        aad_header.crc32c = 0;
        let header_bytes = aad_header.to_bytes();
        let aad = build_aad(preamble_bytes, &header_bytes);

        ring.decrypt_aad(epoch, self.header.lsn, &aad, &self.payload)
    }

    /// Consume an encrypted record and return the equivalent plaintext record.
    ///
    /// The result is indistinguishable from a record that was written without
    /// encryption in the first place: `ENCRYPTED_FLAG` is cleared, `payload_len`
    /// matches the plaintext, and the CRC is recomputed over the new header and
    /// payload so [`Self::verify_checksum`] still passes and re-serialization
    /// stays coherent.
    ///
    /// Recomputing the CRC does not weaken any integrity check. The on-disk CRC
    /// was already verified against the on-disk bytes by the reader before this
    /// call, and the AES-GCM auth tag independently binds the ciphertext to the
    /// segment preamble and the record header. Keeping the on-disk CRC after
    /// rewriting two header fields and the payload would leave a record that
    /// fails its own checksum.
    ///
    /// A record that is not encrypted is returned untouched, so this is safe to
    /// apply to a mixed stream.
    pub fn into_decrypted(
        mut self,
        epoch: &[u8; 4],
        preamble_bytes: Option<&[u8; PREAMBLE_SIZE]>,
        ring: Option<&crate::crypto::KeyRing>,
    ) -> Result<Self> {
        if !self.is_encrypted() {
            return Ok(self);
        }

        let plaintext = self.decrypt_payload_ring(epoch, preamble_bytes, ring)?;
        // The ciphertext is longer than the plaintext and its length already
        // fit `u32` in the header this record was read from, so the conversion
        // cannot overflow; it is still checked rather than asserted.
        let payload_len = u32::try_from(plaintext.len()).map_err(|_| WalError::CorruptRecord {
            lsn: self.header.lsn,
            detail: "decrypted payload length does not fit the record header".into(),
        })?;

        self.header.record_type &= !ENCRYPTED_FLAG;
        self.header.payload_len = payload_len;
        self.payload = plaintext;
        self.header.crc32c = self.header.compute_checksum(&self.payload);
        Ok(self)
    }

    /// Whether this record's payload is encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.header.record_type & ENCRYPTED_FLAG != 0
    }

    /// Logical record type with the encryption flag stripped.
    pub fn logical_record_type(&self) -> u32 {
        self.header.record_type & !ENCRYPTED_FLAG
    }

    /// Verify the CRC32C checksum.
    pub fn verify_checksum(&self) -> Result<()> {
        let expected = self.header.crc32c;
        let actual = self.header.compute_checksum(&self.payload);
        if expected != actual {
            return Err(WalError::ChecksumMismatch {
                lsn: self.header.lsn,
                expected,
                actual,
            });
        }
        Ok(())
    }

    /// Total size on disk: header + payload.
    pub fn wire_size(&self) -> usize {
        HEADER_SIZE + self.payload.len()
    }
}

/// Build the AAD buffer: `preamble_bytes || header_bytes`.
///
/// When `preamble_bytes` is `None` (no encryption or legacy path), the AAD
/// is just the header bytes. When present, the preamble is prepended.
pub(crate) fn build_aad(
    preamble_bytes: Option<&[u8; PREAMBLE_SIZE]>,
    header_bytes: &[u8; HEADER_SIZE],
) -> Vec<u8> {
    match preamble_bytes {
        Some(p) => {
            let mut aad = Vec::with_capacity(PREAMBLE_SIZE + HEADER_SIZE);
            aad.extend_from_slice(p);
            aad.extend_from_slice(header_bytes);
            aad
        }
        None => header_bytes.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::RecordType;
    use super::*;

    #[test]
    fn checksum_roundtrip() {
        let payload = b"hello nodedb";
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: payload.to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap();
        record.verify_checksum().unwrap();
    }

    #[test]
    fn checksum_detects_corruption() {
        let payload = b"hello nodedb";
        let mut record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: payload.to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap();
        record.payload[0] ^= 0xFF;
        assert!(matches!(
            record.verify_checksum(),
            Err(WalError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn payload_too_large_rejected() {
        let big_payload = vec![0u8; MAX_WAL_PAYLOAD_SIZE + 1];
        assert!(matches!(
            WalRecord::new(WalRecordArgs {
                record_type: RecordType::Put as u32,
                lsn: 1,
                tenant_id: 0,
                vshard_id: 0,
                database_id: 0,
                payload: big_payload,
                encryption_key: None,
                preamble_bytes: None,
            }),
            Err(WalError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn anchor_payload_in_record() {
        use super::super::anchor::LsnMsAnchorPayload;
        let anchor = LsnMsAnchorPayload::new(42, 1_700_000_000_000);
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::LsnMsAnchor as u32,
            lsn: 42,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: anchor.to_bytes().to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap();
        record.verify_checksum().unwrap();
        assert_eq!(record.logical_record_type(), RecordType::LsnMsAnchor as u32);
        let decoded = LsnMsAnchorPayload::from_bytes(&record.payload).unwrap();
        assert_eq!(decoded, anchor);
    }

    #[test]
    fn a_stamped_record_keeps_its_event_source_and_checksum() {
        for code in [NO_EVENT_SOURCE, 1, 2, 3, 4, 5, 6, u8::MAX] {
            let record = WalRecord::new_stamped(
                WalRecordArgs {
                    record_type: 1,
                    lsn: 9,
                    tenant_id: 1,
                    vshard_id: 0,
                    database_id: 0,
                    payload: b"row".to_vec(),
                    encryption_key: None,
                    preamble_bytes: None,
                },
                RecordStamp {
                    apply_key: 7,
                    event_source: code,
                },
            )
            .expect("record");
            assert_eq!(record.event_source(), code);
            assert_eq!(record.apply_key(), 7);
            record
                .verify_checksum()
                .expect("checksum covers the source");
            let decoded = RecordHeader::from_bytes(&record.header.to_bytes());
            assert_eq!(decoded.event_source, code);
        }
    }
}
