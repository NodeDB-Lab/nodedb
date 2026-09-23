// SPDX-License-Identifier: Apache-2.0

//! WAL record header: fixed 54-byte prefix + constants.

use crate::error::{Result, WalError};

/// Magic number identifying a NodeDB WAL record.
pub const WAL_MAGIC: u32 = 0x5359_4E57; // "SYNW"

/// Current WAL format version.
///
/// v2 introduces bitemporal record layout: `LsnMsAnchor` records (type 102)
/// provide stable LSN↔wall-clock interpolation, and engine-level writers emit
/// `system_from_ms` in versioned keys.
///
/// v3 introduces the 16-byte segment preamble (`WALP` magic) written at offset
/// 0 of every WAL segment file.
///
/// v4 widens `record_type` u16→u32 and `vshard_id` u16→u32, adds 16 reserved
/// bytes (covered by CRC32C) before the checksum, and bumps `HEADER_SIZE` to
/// 50 bytes. Pre-release — no v1/v2/v3 readers supported.
///
/// v1 is the initial shipped format with 54-byte headers (u64 tenant_id,
/// u16 vshard_id, u32 payload_len, u16 reserved, u32 crc32c).
pub const WAL_FORMAT_VERSION: u16 = 1;

/// Maximum WAL record payload size (64 MiB). Distinct from cluster RPC's limit.
pub const MAX_WAL_PAYLOAD_SIZE: usize = 64 * 1024 * 1024;

/// Size of the record header in bytes.
///
/// Layout (all little-endian):
///   magic(4) | format_version(2) | record_type(4) | lsn(8) | tenant_id(8)
///   | vshard_id(4) | payload_len(4) | database_id(8) | reserved(8) | crc32c(4)
///
/// `database_id` occupies bytes 34–41 (previously part of the 16-byte reserved
/// field). `apply_key` occupies bytes 42–49. Bytes 34–41 were zero-filled in
/// prior records, so `database_id == 0` maps to `DatabaseId(0)` (the default
/// database), preserving backward compatibility without a format-version bump.
pub const HEADER_SIZE: usize = 54;

/// Bit 14 in `record_type` signals the payload is AES-256-GCM encrypted.
/// Separate from bit 15 (required flag). Both bits keep their positions;
/// the type is now u32 so the constants are widened accordingly.
pub const ENCRYPTED_FLAG: u32 = 0x0000_4000;

/// Bit 15: required-flag. Records with this bit set and an unknown type
/// must not be silently skipped.
pub const REQUIRED_FLAG: u32 = 0x0000_8000;

/// WAL record header (fixed 54 bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub magic: u32,
    pub format_version: u16,
    pub record_type: u32,
    pub lsn: u64,
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub payload_len: u32,
    /// Database scope for this record. Stored as a raw `u64`; callers convert
    /// to/from `DatabaseId`. Pre-Tier-2 records had zeros here, so `0` maps to
    /// `DatabaseId(0)` (the default database) — fully backward compatible.
    ///
    /// Occupies bytes 34–41 of the on-disk header (previously part of reserved).
    pub database_id: u64,
    /// The idempotency key of the replicated proposal whose apply appended
    /// this record, `0` for a record no proposal apply appended. The record
    /// and the key are durable together, so a node recovers which proposals
    /// it applied from the records themselves. Covered by CRC32C. Occupies
    /// bytes 42–49.
    pub apply_key: u64,
    pub crc32c: u32,
}

impl RecordHeader {
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        buf[6..10].copy_from_slice(&self.record_type.to_le_bytes());
        buf[10..18].copy_from_slice(&self.lsn.to_le_bytes());
        buf[18..26].copy_from_slice(&self.tenant_id.to_le_bytes());
        buf[26..30].copy_from_slice(&self.vshard_id.to_le_bytes());
        buf[30..34].copy_from_slice(&self.payload_len.to_le_bytes());
        buf[34..42].copy_from_slice(&self.database_id.to_le_bytes());
        buf[42..50].copy_from_slice(&self.apply_key.to_le_bytes());
        buf[50..54].copy_from_slice(&self.crc32c.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]),
            format_version: u16::from_le_bytes([buf[4], buf[5]]),
            record_type: u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]),
            lsn: u64::from_le_bytes([
                buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17],
            ]),
            tenant_id: u64::from_le_bytes([
                buf[18], buf[19], buf[20], buf[21], buf[22], buf[23], buf[24], buf[25],
            ]),
            vshard_id: u32::from_le_bytes([buf[26], buf[27], buf[28], buf[29]]),
            payload_len: u32::from_le_bytes([buf[30], buf[31], buf[32], buf[33]]),
            database_id: u64::from_le_bytes([
                buf[34], buf[35], buf[36], buf[37], buf[38], buf[39], buf[40], buf[41],
            ]),
            apply_key: u64::from_le_bytes([
                buf[42], buf[43], buf[44], buf[45], buf[46], buf[47], buf[48], buf[49],
            ]),
            crc32c: u32::from_le_bytes([buf[50], buf[51], buf[52], buf[53]]),
        }
    }

    /// CRC32C over header (excluding the crc32c field) + payload.
    ///
    /// The apply key is included in the CRC so it cannot be silently
    /// modified without detection.
    pub fn compute_checksum(&self, payload: &[u8]) -> u32 {
        let header_bytes = self.to_bytes();
        let mut digest = crc32c::crc32c(&header_bytes[..HEADER_SIZE - 4]);
        digest = crc32c::crc32c_append(digest, payload);
        digest
    }

    /// Logical record type with the encryption flag stripped.
    pub fn logical_record_type(&self) -> u32 {
        self.record_type & !ENCRYPTED_FLAG
    }

    pub fn validate(&self, offset: u64) -> Result<()> {
        if self.magic != WAL_MAGIC {
            return Err(WalError::InvalidMagic {
                offset,
                expected: WAL_MAGIC,
                actual: self.magic,
            });
        }
        if self.format_version != WAL_FORMAT_VERSION {
            return Err(WalError::UnsupportedVersion {
                version: self.format_version,
                supported: WAL_FORMAT_VERSION,
            });
        }
        let payload_len =
            usize::try_from(self.payload_len).map_err(|_| WalError::PayloadTooLarge {
                size: usize::MAX,
                max: MAX_WAL_PAYLOAD_SIZE,
            })?;
        if payload_len > MAX_WAL_PAYLOAD_SIZE {
            return Err(WalError::PayloadTooLarge {
                size: payload_len,
                max: MAX_WAL_PAYLOAD_SIZE,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_header(record_type: u32, vshard_id: u32) -> RecordHeader {
        RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type,
            lsn: 42,
            tenant_id: 7,
            vshard_id,
            payload_len: 100,
            database_id: 0,
            apply_key: 0,
            crc32c: 0xDEAD_BEEF,
        }
    }

    #[test]
    fn header_roundtrip() {
        let header = make_header(1 | REQUIRED_FLAG, 3);
        let bytes = header.to_bytes();
        assert_eq!(header, RecordHeader::from_bytes(&bytes));
    }

    #[test]
    fn header_golden_54_bytes_exact_offsets() {
        // magic at 0..4, format_version at 4..6, record_type at 6..10,
        // lsn at 10..18, tenant_id at 18..26, vshard_id at 26..30,
        // payload_len at 30..34, database_id at 34..42, apply_key at 42..50,
        // crc32c at 50..54.
        let header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type: 1,
            lsn: 0x0102_0304_0506_0708,
            tenant_id: 0xDEAD_BEEF_CAFE_1234,
            vshard_id: 0xCAFE_BABE,
            payload_len: 256,
            database_id: 0xABCD_0000_1234_5678,
            apply_key: 0,
            crc32c: 0x1234_5678,
        };
        let b = header.to_bytes();
        assert_eq!(b.len(), 54);
        // magic
        assert_eq!(&b[0..4], &WAL_MAGIC.to_le_bytes());
        // format_version
        assert_eq!(&b[4..6], &WAL_FORMAT_VERSION.to_le_bytes());
        // record_type
        assert_eq!(&b[6..10], &1u32.to_le_bytes());
        // lsn
        assert_eq!(&b[10..18], &0x0102_0304_0506_0708u64.to_le_bytes());
        // tenant_id (u64, 8 bytes)
        assert_eq!(&b[18..26], &0xDEAD_BEEF_CAFE_1234u64.to_le_bytes());
        // vshard_id
        assert_eq!(&b[26..30], &0xCAFE_BABEu32.to_le_bytes());
        // payload_len
        assert_eq!(&b[30..34], &256u32.to_le_bytes());
        // database_id
        assert_eq!(&b[34..42], &0xABCD_0000_1234_5678u64.to_le_bytes());
        // apply_key — zero
        assert_eq!(&b[42..50], &[0u8; 8]);
        // crc32c
        assert_eq!(&b[50..54], &0x1234_5678u32.to_le_bytes());
    }

    #[test]
    fn database_id_roundtrip() {
        // Non-zero database_id survives to_bytes → from_bytes.
        let header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type: 1,
            lsn: 1,
            tenant_id: 42,
            vshard_id: 0,
            payload_len: 0,
            database_id: 7,
            apply_key: 0,
            crc32c: 0,
        };
        let bytes = header.to_bytes();
        let decoded = RecordHeader::from_bytes(&bytes);
        assert_eq!(decoded.database_id, 7);
    }

    #[test]
    fn pre_tier2_zero_database_id_compat() {
        // A record written before Tier 2 has zeros at bytes 34..42.
        // from_bytes must decode that as database_id == 0 (the default database).
        let mut raw = [0u8; HEADER_SIZE];
        raw[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
        raw[4..6].copy_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
        raw[6..10].copy_from_slice(&1u32.to_le_bytes()); // record_type
        // bytes 34..50 stay zero (pre-Tier-2 reserved field)
        let decoded = RecordHeader::from_bytes(&raw);
        assert_eq!(decoded.database_id, 0);
    }

    #[test]
    fn tenant_id_above_u32_max_roundtrip() {
        // Verify u64 tenant_id with a value > u32::MAX is preserved exactly.
        let tid = u32::MAX as u64 + 1;
        let header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: WAL_FORMAT_VERSION,
            record_type: 1,
            lsn: 1,
            tenant_id: tid,
            vshard_id: 0,
            payload_len: 0,
            database_id: 0,
            apply_key: 0,
            crc32c: 0,
        };
        let bytes = header.to_bytes();
        let decoded = RecordHeader::from_bytes(&bytes);
        assert_eq!(decoded.tenant_id, tid);
    }

    #[test]
    fn invalid_magic_detected() {
        let mut header = make_header(0, 0);
        header.magic = 0xBAD0_F00D;
        assert!(matches!(
            header.validate(0),
            Err(WalError::InvalidMagic { .. })
        ));
    }

    #[test]
    fn payload_larger_than_limit_is_rejected_before_reading() {
        let mut header = make_header(0, 0);
        header.payload_len = (MAX_WAL_PAYLOAD_SIZE + 1) as u32;
        assert!(matches!(
            header.validate(0),
            Err(WalError::PayloadTooLarge {
                size,
                max: MAX_WAL_PAYLOAD_SIZE,
            }) if size == MAX_WAL_PAYLOAD_SIZE + 1
        ));
    }

    #[test]
    fn unsupported_version_detected() {
        let mut header = make_header(0, 0);
        header.format_version = WAL_FORMAT_VERSION + 1;
        assert!(matches!(
            header.validate(0),
            Err(WalError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn version_4_rejected() {
        // Regression: bumping from v4 to v5 — a v4 header must be rejected.
        let mut header = make_header(0, 0);
        header.format_version = 4;
        assert!(matches!(
            header.validate(0),
            Err(WalError::UnsupportedVersion { version: 4, .. })
        ));
    }

    #[test]
    fn large_vshard_id_roundtrip() {
        // 0x1234_5678 is well above old u16::MAX; ensure no truncation.
        let header = make_header(1, 0x1234_5678);
        let bytes = header.to_bytes();
        let decoded = RecordHeader::from_bytes(&bytes);
        assert_eq!(decoded.vshard_id, 0x1234_5678u32);
    }

    #[test]
    fn encrypted_flag_is_u32() {
        let header = make_header(1 | ENCRYPTED_FLAG, 0);
        assert_eq!(header.logical_record_type(), 1);
        assert!(header.record_type & ENCRYPTED_FLAG != 0);
    }

    #[test]
    fn large_record_type_roundtrip() {
        // 0x0001_0001 has bits above old u16 max set; verify u32 width preserved.
        let header = make_header(0x0001_0001, 0);
        let bytes = header.to_bytes();
        let decoded = RecordHeader::from_bytes(&bytes);
        assert_eq!(decoded.record_type, 0x0001_0001u32);
        // Flags in bit-14 and bit-15 positions still work alongside high bits.
        let with_flags = make_header(0x0001_0001 | ENCRYPTED_FLAG | REQUIRED_FLAG, 0);
        let bytes2 = with_flags.to_bytes();
        let decoded2 = RecordHeader::from_bytes(&bytes2);
        assert_eq!(
            decoded2.record_type,
            0x0001_0001 | ENCRYPTED_FLAG | REQUIRED_FLAG
        );
        assert_eq!(decoded2.logical_record_type(), 0x0001_0001 | REQUIRED_FLAG);
    }
}
