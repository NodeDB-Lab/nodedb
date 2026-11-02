// SPDX-License-Identifier: BUSL-1.1

//! Single-file packed partition format with versioned binary footer.
//!
//! Combines all column files, schema, sparse index, and metadata into a
//! single immutable file for cold/S3 storage. The footer at the end of
//! the file contains exact byte ranges per column, enabling:
//!
//! - HTTP range requests for individual columns (no full-file download)
//! - Projection pushdown at the storage layer
//! - Zero-copy mmap of specific columns
//!
//! ## Plaintext layout
//!
//! ```text
//! ┌───────────────────────────────────────┐
//! │  Column 0 data (compressed bytes)     │
//! │  Column 1 data (compressed bytes)     │
//! │  ...                                  │
//! ├───────────────────────────────────────┤
//! │  Sparse index data                    │
//! ├───────────────────────────────────────┤
//! │  Footer body (msgpack PackedFooter)   │
//! ├───────────────────────────────────────┤
//! │  footer_len   : u32 LE   (4 bytes)    │
//! │  crc32c       : u32 LE   (4 bytes)    │  ← CRC over footer body
//! │  version      : u16 LE=1 (2 bytes)    │
//! │  magic "NDPK" : 4 bytes  (4 bytes)    │  ← always at EOF
//! └───────────────────────────────────────┘
//! ```
//!
//! ## Encrypted layout (KEK present)
//!
//! The columns + sparse index payload is wrapped in a `SEGT` AES-256-GCM
//! envelope. The NDPK footer (msgpack body + 14-byte tail) is left **in
//! plaintext** so that footer-only range requests remain usable without
//! decryption. The `PackedFooter::column_ranges` byte offsets refer to
//! positions within the *decrypted* payload blob, not the on-disk file.
//!
//! ```text
//! ┌───────────────────────────────────────┐
//! │  SEGT preamble (36 bytes)             │  ← salt + random nonce, all AAD
//! │  AES-256-GCM ciphertext               │  ← columns + sparse index
//! │    (includes 16B auth tag)            │
//! ├───────────────────────────────────────┤
//! │  Footer body (msgpack PackedFooter)   │  ← plaintext
//! ├───────────────────────────────────────┤
//! │  footer_len   : u32 LE   (4 bytes)    │
//! │  crc32c       : u32 LE   (4 bytes)    │
//! │  version      : u16 LE=1 (2 bytes)    │
//! │  magic "NDPK" : 4 bytes  (4 bytes)    │  ← always at EOF
//! └───────────────────────────────────────┘
//! ```
//!
//! Tail is 14 bytes total. Reader: read last 4 bytes for magic, version at
//! -6..-4, CRC at -10..-6, footer_len at -14..-10, then read body and
//! verify CRC before deserializing. `read_column` additionally checks for
//! the `SEGT` preamble and decrypts when a KEK is supplied.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use nodedb_codec::ColumnCodec;
use nodedb_types::timeseries::PartitionMeta;
use nodedb_wal::crypto::WalEncryptionKey;

use super::columnar_segment::encrypt::{decrypt_file, encrypt_file, is_encrypted};
use crate::engine::timeseries::columnar_segment::SegmentError;

/// Magic bytes at the end of a packed partition file (at EOF).
pub const PACKED_MAGIC: [u8; 4] = *b"NDPK";
/// Format version written to the tail.
pub const PACKED_VERSION: u16 = 1;
/// Fixed byte size of the tail (footer_len + crc + version + magic).
pub const PACKED_TAIL_SIZE: usize = 14;

// ── Cursor helper ─────────────────────────────────────────────────────────────

struct TailCursor<'a> {
    data: &'a [u8],
    /// Current position measured from the *end* of the slice.
    from_end: usize,
}

impl<'a> TailCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, from_end: 0 }
    }

    /// Read `n` bytes backwards, returning them in logical order.
    fn read_back(&mut self, n: usize) -> Option<&'a [u8]> {
        let new_from_end = self.from_end + n;
        if new_from_end > self.data.len() {
            return None;
        }
        let end = self.data.len() - self.from_end;
        let start = end - n;
        self.from_end = new_from_end;
        Some(&self.data[start..end])
    }

    fn read_back_u32_le(&mut self) -> Option<u32> {
        let b = self.read_back(4)?;
        Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_back_u16_le(&mut self) -> Option<u16> {
        let b = self.read_back(2)?;
        Some(u16::from_le_bytes([b[0], b[1]]))
    }

    /// Bytes consumed from the end so far (== current tail offset).
    fn consumed(&self) -> usize {
        self.from_end
    }
}

// ── Types ─────────────────────────────────────────────────────────────────────

/// Footer of a packed partition file.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PackedFooter {
    /// Per-column byte ranges: column_name → (offset, length).
    /// Offsets are relative to the start of the *payload* (decrypted blob).
    pub column_ranges: HashMap<String, (u64, u64)>,
    /// Sparse index byte range (offset, length) within the payload.
    pub sparse_index_range: Option<(u64, u64)>,
    /// Schema: column names, types, codecs.
    pub schema: Vec<PackedColumnSchema>,
    /// Partition metadata.
    pub meta: PartitionMeta,
    /// True when the payload (columns + sparse index) is encrypted.
    #[serde(default)]
    pub payload_encrypted: bool,
}

/// Schema entry for a column in the packed format.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PackedColumnSchema {
    pub name: String,
    pub col_type: String,
    pub codec: ColumnCodec,
}

/// Error type for packed partition operations.
#[derive(thiserror::Error, Debug)]
pub enum PackedError {
    #[error("packed partition I/O: {0}")]
    Io(String),
    #[error("packed partition corrupt: {0}")]
    Corrupt(String),
    #[error("unsupported packed partition format version: {version}")]
    UnsupportedVersion { version: u16 },
    #[error("packed partition footer CRC mismatch: stored={stored:#010x} calc={calc:#010x}")]
    InvalidFooterCrc { stored: u32, calc: u32 },
    #[error("packed partition payload is encrypted (SEGT) but no KEK was provided")]
    MissingKek,
    #[error("packed partition KEK provided but payload is not encrypted")]
    UnexpectedPlaintext,
    #[error("packed partition payload encryption failed: {0}")]
    EncryptionFailed(String),
    #[error("packed partition payload decryption failed: {0}")]
    DecryptionFailed(String),
}

impl From<SegmentError> for PackedError {
    fn from(e: SegmentError) -> Self {
        match e {
            SegmentError::MissingKek => PackedError::MissingKek,
            SegmentError::UnexpectedPlaintext => PackedError::UnexpectedPlaintext,
            SegmentError::EncryptionFailed(m) => PackedError::EncryptionFailed(m),
            SegmentError::DecryptionFailed(m) => PackedError::DecryptionFailed(m),
            SegmentError::Io(m) => PackedError::Io(m),
            SegmentError::Corrupt(m) => PackedError::Corrupt(m),
        }
    }
}

// ── Writer ────────────────────────────────────────────────────────────────────

/// Write a packed partition file from individual column files.
///
/// When `kek` is `Some`, the columns + sparse index payload region is wrapped
/// in a `SEGT` AES-256-GCM envelope. The NDPK footer remains in plaintext to
/// allow footer-only range requests without decryption.
///
/// The `column_ranges` in the footer record offsets within the *decrypted*
/// payload blob, not the on-disk ciphertext, so `read_column` can pass them
/// through after decryption.
pub fn write_packed(
    output_path: &Path,
    columns: &[(String, Vec<u8>)], // (column_name, compressed_bytes)
    sparse_index: Option<&[u8]>,
    schema: &[PackedColumnSchema],
    meta: &PartitionMeta,
    kek: Option<&WalEncryptionKey>,
) -> Result<u64, PackedError> {
    let mut payload = Vec::new();
    let mut column_ranges = HashMap::new();

    // Build the payload (columns + sparse index).
    for (name, data) in columns {
        let offset = payload.len() as u64;
        payload.extend_from_slice(data);
        column_ranges.insert(name.clone(), (offset, data.len() as u64));
    }

    let sparse_index_range = sparse_index.map(|idx_data| {
        let offset = payload.len() as u64;
        payload.extend_from_slice(idx_data);
        (offset, idx_data.len() as u64)
    });

    // Optionally encrypt the payload.
    let payload_encrypted = kek.is_some();
    let payload_on_disk: Vec<u8> = if let Some(key) = kek {
        encrypt_file(key, &payload).map_err(PackedError::from)?
    } else {
        payload
    };

    // Build footer (plaintext).
    let footer = PackedFooter {
        column_ranges,
        sparse_index_range,
        schema: schema.to_vec(),
        meta: meta.clone(),
        payload_encrypted,
    };
    let footer_body = zerompk::to_msgpack_vec(&footer)
        .map_err(|e| PackedError::Io(format!("footer encode: {e}")))?;

    // Compose the final file.
    let mut buf = Vec::with_capacity(payload_on_disk.len() + footer_body.len() + PACKED_TAIL_SIZE);
    buf.extend_from_slice(&payload_on_disk);
    buf.extend_from_slice(&footer_body);

    let footer_len = footer_body.len() as u32;
    let crc = crc32c::crc32c(&footer_body);

    // Tail: footer_len (4B) | crc (4B) | version (2B) | magic (4B)
    buf.extend_from_slice(&footer_len.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&PACKED_VERSION.to_le_bytes());
    buf.extend_from_slice(&PACKED_MAGIC);

    let total_size = buf.len() as u64;
    std::fs::write(output_path, &buf)
        .map_err(|e| PackedError::Io(format!("write {}: {e}", output_path.display())))?;

    Ok(total_size)
}

// ── Reader ────────────────────────────────────────────────────────────────────

/// Read the footer from a packed partition file.
///
/// The NDPK footer is always plaintext. No KEK needed for footer reads.
pub fn read_footer(file_path: &Path) -> Result<PackedFooter, PackedError> {
    let data = std::fs::read(file_path)
        .map_err(|e| PackedError::Io(format!("read {}: {e}", file_path.display())))?;
    read_footer_from_bytes(&data)
}

/// Read the footer from in-memory bytes (for testing or mmap'd files).
pub fn read_footer_from_bytes(data: &[u8]) -> Result<PackedFooter, PackedError> {
    if data.len() < PACKED_TAIL_SIZE {
        return Err(PackedError::Corrupt(format!(
            "file too small for packed tail: {} bytes",
            data.len()
        )));
    }

    let mut tail = TailCursor::new(data);

    // Read tail fields in reverse order (right → left).
    let magic = tail
        .read_back(4)
        .expect("invariant: PACKED_TAIL_SIZE guard at line 271 ensures tail bytes available");
    if magic != PACKED_MAGIC {
        return Err(PackedError::Corrupt(format!(
            "invalid magic: expected NDPK, got {magic:?}"
        )));
    }

    let version = tail
        .read_back_u16_le()
        .expect("invariant: PACKED_TAIL_SIZE guard at line 271 ensures tail bytes available");
    if version != PACKED_VERSION {
        return Err(PackedError::UnsupportedVersion { version });
    }

    let crc_stored = tail
        .read_back_u32_le()
        .expect("invariant: PACKED_TAIL_SIZE guard at line 271 ensures tail bytes available");
    let footer_len = tail
        .read_back_u32_le()
        .expect("invariant: PACKED_TAIL_SIZE guard at line 271 ensures tail bytes available")
        as usize;

    // tail.consumed() == PACKED_TAIL_SIZE at this point.
    let tail_offset = data.len() - tail.consumed();
    if footer_len > tail_offset {
        return Err(PackedError::Corrupt(format!(
            "footer_len {footer_len} exceeds available bytes {tail_offset}"
        )));
    }

    let body_start = tail_offset - footer_len;
    let footer_body = &data[body_start..body_start + footer_len];

    // Verify CRC over body.
    let crc_calc = crc32c::crc32c(footer_body);
    if crc_stored != crc_calc {
        return Err(PackedError::InvalidFooterCrc {
            stored: crc_stored,
            calc: crc_calc,
        });
    }

    zerompk::from_msgpack(footer_body)
        .map_err(|e| PackedError::Corrupt(format!("footer body decode: {e}")))
}

/// Read a single column's data from a packed file using byte range.
///
/// When `footer.payload_encrypted` is true, `kek` must be `Some`; the payload
/// region (everything before the footer body) is decrypted first, then the
/// column range is extracted from the plaintext. The NDPK footer itself is
/// always plaintext.
pub fn read_column(
    file_path: &Path,
    footer: &PackedFooter,
    column_name: &str,
    kek: Option<&WalEncryptionKey>,
) -> Result<Vec<u8>, PackedError> {
    let (offset, length) = footer
        .column_ranges
        .get(column_name)
        .ok_or_else(|| PackedError::Corrupt(format!("column '{column_name}' not in footer")))?;

    let data = std::fs::read(file_path).map_err(|e| PackedError::Io(format!("read: {e}")))?;
    read_column_from_bytes(&data, footer, *offset, *length, kek)
}

/// Read a single column from in-memory file bytes (for testing).
fn read_column_from_bytes(
    data: &[u8],
    footer: &PackedFooter,
    offset: u64,
    length: u64,
    kek: Option<&WalEncryptionKey>,
) -> Result<Vec<u8>, PackedError> {
    // Determine where the payload ends: everything before the footer body.
    // We need to find the start of the footer body in `data`.
    // Footer body is immediately before the tail (PACKED_TAIL_SIZE bytes).
    // We already have footer.column_ranges so we know footer_len indirectly;
    // instead just re-derive payload_end from the file structure.
    //
    // Payload ends right before the footer body. The footer body starts at
    // `data.len() - PACKED_TAIL_SIZE - footer_body_len`. We don't have
    // footer_body_len cached, so compute it by re-reading from the tail.
    let tail_size = PACKED_TAIL_SIZE;
    if data.len() < tail_size {
        return Err(PackedError::Corrupt("file too small".into()));
    }
    // Read footer_len from tail (bytes -14..-10).
    let footer_len_bytes: [u8; 4] = data[data.len() - tail_size..data.len() - tail_size + 4]
        .try_into()
        .map_err(|_| PackedError::Corrupt("footer_len read failed".into()))?;
    let footer_body_len = u32::from_le_bytes(footer_len_bytes) as usize;
    let payload_end = data.len() - tail_size - footer_body_len;

    let payload_region = &data[..payload_end];

    let plaintext: Vec<u8> = if footer.payload_encrypted {
        match kek {
            Some(key) => {
                if !is_encrypted(payload_region)
                    .map_err(|e| PackedError::Corrupt(format!("sniff: {e}")))?
                {
                    return Err(PackedError::Corrupt(
                        "footer says payload_encrypted but no SEGT preamble found".into(),
                    ));
                }
                decrypt_file(key, payload_region).map_err(PackedError::from)?
            }
            None => return Err(PackedError::MissingKek),
        }
    } else {
        match kek {
            None => payload_region.to_vec(),
            Some(_) => {
                // Only reject when the footer explicitly says not encrypted.
                return Err(PackedError::UnexpectedPlaintext);
            }
        }
    };

    let start = offset as usize;
    let end = start + length as usize;
    if end > plaintext.len() {
        return Err(PackedError::Corrupt(
            "column range exceeds payload size".into(),
        ));
    }

    Ok(plaintext[start..end].to_vec())
}

/// Generate HTTP Range header values for fetching specific columns.
///
/// Returns `(column_name, "bytes=offset-end")` pairs for use with
/// S3 GetObject or HTTP range requests.
///
/// Note: for encrypted packed files, these range headers address the
/// ciphertext. Column-level reads require full payload decryption
/// on the client side.
pub fn http_range_headers(footer: &PackedFooter, columns: &[&str]) -> Vec<(String, String)> {
    columns
        .iter()
        .filter_map(|&name| {
            footer.column_ranges.get(name).map(|&(offset, length)| {
                let end = offset + length - 1; // HTTP ranges are inclusive.
                (name.to_string(), format!("bytes={offset}-{end}"))
            })
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::timeseries::PartitionState;
    use tempfile::TempDir;

    fn test_meta() -> PartitionMeta {
        PartitionMeta {
            min_ts: 1000,
            max_ts: 2000,
            row_count: 100,
            size_bytes: 0,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: 86_400_000,
            last_flushed_wal_lsn: 0,
            column_stats: HashMap::new(),
            max_system_ts: 0,
            ended_at: None,
        }
    }

    fn test_kek() -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[0x37u8; 32]).unwrap()
    }

    #[test]
    fn write_and_read_footer() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ndpk");

        let columns = vec![
            ("timestamp".to_string(), vec![1u8, 2, 3, 4, 5]),
            ("value".to_string(), vec![10, 20, 30]),
        ];
        let schema = vec![
            PackedColumnSchema {
                name: "timestamp".into(),
                col_type: "timestamp".into(),
                codec: ColumnCodec::DeltaFastLanesLz4,
            },
            PackedColumnSchema {
                name: "value".into(),
                col_type: "float64".into(),
                codec: ColumnCodec::AlpFastLanesLz4,
            },
        ];

        let size = write_packed(&path, &columns, None, &schema, &test_meta(), None).unwrap();
        assert!(size > 0);

        let footer = read_footer(&path).unwrap();
        assert_eq!(footer.column_ranges.len(), 2);
        assert_eq!(footer.column_ranges["timestamp"], (0, 5));
        assert_eq!(footer.column_ranges["value"], (5, 3));
        assert_eq!(footer.meta.row_count, 100);
        assert!(!footer.payload_encrypted);
    }

    #[test]
    fn read_single_column() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ndpk");

        let ts_data = vec![1u8, 2, 3, 4, 5];
        let val_data = vec![10u8, 20, 30];
        let columns = vec![
            ("timestamp".to_string(), ts_data.clone()),
            ("value".to_string(), val_data.clone()),
        ];

        write_packed(&path, &columns, None, &[], &test_meta(), None).unwrap();

        let footer = read_footer(&path).unwrap();
        let ts_bytes = read_column(&path, &footer, "timestamp", None).unwrap();
        assert_eq!(ts_bytes, ts_data);

        let val_bytes = read_column(&path, &footer, "value", None).unwrap();
        assert_eq!(val_bytes, val_data);
    }

    #[test]
    fn http_range_headers_for_projection() {
        let mut column_ranges = HashMap::new();
        column_ranges.insert("timestamp".to_string(), (0u64, 1000));
        column_ranges.insert("cpu".to_string(), (1000, 500));
        column_ranges.insert("host".to_string(), (1500, 200));

        let footer = PackedFooter {
            column_ranges,
            sparse_index_range: None,
            schema: vec![],
            meta: test_meta(),
            payload_encrypted: false,
        };

        let ranges = http_range_headers(&footer, &["cpu"]);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].0, "cpu");
        assert_eq!(ranges[0].1, "bytes=1000-1499");
    }

    #[test]
    fn with_sparse_index() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ndpk");

        let sparse_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let columns = vec![("ts".to_string(), vec![1, 2, 3])];

        write_packed(&path, &columns, Some(&sparse_data), &[], &test_meta(), None).unwrap();

        let footer = read_footer(&path).unwrap();
        assert!(footer.sparse_index_range.is_some());
        let (offset, length) = footer.sparse_index_range.unwrap();
        assert_eq!(offset, 3); // After the 3-byte ts column.
        assert_eq!(length, 4);
    }

    #[test]
    fn invalid_magic() {
        let data = vec![0u8; 100]; // No magic at EOF.
        assert!(read_footer_from_bytes(&data).is_err());
    }

    #[test]
    fn projection_pushdown_skips_columns() {
        let mut ranges = HashMap::new();
        ranges.insert("ts".to_string(), (0u64, 10000));
        ranges.insert("cpu".to_string(), (10000, 5000));
        ranges.insert("mem".to_string(), (15000, 5000));
        ranges.insert("host".to_string(), (20000, 2000));

        let footer = PackedFooter {
            column_ranges: ranges,
            sparse_index_range: None,
            schema: vec![],
            meta: test_meta(),
            payload_encrypted: false,
        };

        let headers = http_range_headers(&footer, &["ts", "cpu"]);
        assert_eq!(headers.len(), 2);

        let fetched: u64 = headers
            .iter()
            .map(|(name, _)| footer.column_ranges[name].1)
            .sum();
        assert_eq!(fetched, 15000);
    }

    // ── packed-partition golden tests ────────────────────────────────────

    /// Verify tail layout byte by byte.
    #[test]
    fn packed_golden_tail_bytes() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("golden.ndpk");
        let columns = vec![("ts".to_string(), vec![1u8, 2, 3])];
        write_packed(&path, &columns, None, &[], &test_meta(), None).unwrap();

        let data = std::fs::read(&path).unwrap();
        let n = data.len();

        // Last 4 bytes: magic
        assert_eq!(&data[n - 4..], b"NDPK");

        // Bytes -6..-4: version = 1 LE
        let version = u16::from_le_bytes([data[n - 6], data[n - 5]]);
        assert_eq!(version, PACKED_VERSION);

        // Bytes -10..-6: crc32c
        let crc_stored = u32::from_le_bytes([data[n - 10], data[n - 9], data[n - 8], data[n - 7]]);

        // Bytes -14..-10: footer_len
        let footer_len =
            u32::from_le_bytes([data[n - 14], data[n - 13], data[n - 12], data[n - 11]]) as usize;

        // Footer body is immediately before the tail.
        let body_end = n - PACKED_TAIL_SIZE;
        let body_start = body_end - footer_len;
        let footer_body = &data[body_start..body_end];

        // CRC must be internally consistent.
        let crc_calc = crc32c::crc32c(footer_body);
        assert_eq!(
            crc_stored, crc_calc,
            "golden: stored CRC does not match re-computed CRC over body"
        );

        // Body must deserialize correctly.
        let footer: PackedFooter = zerompk::from_msgpack(footer_body).unwrap();
        assert_eq!(footer.column_ranges["ts"], (0, 3));
    }

    /// Version != 1 must be rejected.
    #[test]
    fn packed_rejects_unsupported_version() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("v2.ndpk");
        let columns = vec![("ts".to_string(), vec![1u8])];
        write_packed(&path, &columns, None, &[], &test_meta(), None).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        let n = data.len();
        // Patch version bytes (-6..-4) to version = 2.
        let v2 = 2u16.to_le_bytes();
        data[n - 6] = v2[0];
        data[n - 5] = v2[1];

        let err = read_footer_from_bytes(&data).unwrap_err();
        assert!(
            matches!(err, PackedError::UnsupportedVersion { version: 2 }),
            "expected UnsupportedVersion {{version: 2}}, got {err:?}"
        );
    }

    /// Corrupt CRC must be rejected.
    #[test]
    fn packed_rejects_bad_crc() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad_crc.ndpk");
        let columns = vec![("ts".to_string(), vec![1u8])];
        write_packed(&path, &columns, None, &[], &test_meta(), None).unwrap();

        let mut data = std::fs::read(&path).unwrap();
        let n = data.len();
        // Flip a byte in the CRC field at -10..-6.
        data[n - 10] ^= 0xFF;

        let err = read_footer_from_bytes(&data).unwrap_err();
        assert!(
            matches!(err, PackedError::InvalidFooterCrc { .. }),
            "expected InvalidFooterCrc, got {err:?}"
        );
    }

    // ── packed partition encryption tests ────────────────────────────────

    use nodedb_wal::crypto::SEGMENT_ENVELOPE_PREAMBLE_SIZE as SEGT_PREAMBLE_SIZE;

    use super::super::columnar_segment::encrypt::SEGT_MAGIC;

    #[test]
    fn packed_partition_payload_encrypted_footer_plaintext() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("enc.ndpk");
        let kek = test_kek();

        let columns = vec![
            ("timestamp".to_string(), vec![1u8, 2, 3, 4]),
            ("value".to_string(), vec![10u8, 20, 30, 40]),
        ];
        write_packed(&path, &columns, None, &[], &test_meta(), Some(&kek)).unwrap();

        let data = std::fs::read(&path).unwrap();

        // NDPK footer magic still present at EOF (plaintext).
        let n = data.len();
        assert_eq!(&data[n - 4..], b"NDPK");

        // Footer is readable without KEK.
        let footer = read_footer(&path).unwrap();
        assert!(footer.payload_encrypted);
        assert_eq!(footer.column_ranges.len(), 2);

        // Payload starts with SEGT magic.
        let footer_len = {
            let bytes: [u8; 4] = data[n - PACKED_TAIL_SIZE..n - PACKED_TAIL_SIZE + 4]
                .try_into()
                .unwrap();
            u32::from_le_bytes(bytes) as usize
        };
        let payload_end = n - PACKED_TAIL_SIZE - footer_len;
        assert_eq!(&data[..4], &SEGT_MAGIC, "payload must start with SEGT");
        assert!(
            payload_end > SEGT_PREAMBLE_SIZE,
            "payload must include preamble + ciphertext"
        );
    }

    #[test]
    fn packed_partition_read_column_decrypts() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("enc.ndpk");
        let kek = test_kek();

        let ts_data = vec![0xAAu8, 0xBB, 0xCC];
        let val_data = vec![0x11u8, 0x22];
        let columns = vec![
            ("timestamp".to_string(), ts_data.clone()),
            ("value".to_string(), val_data.clone()),
        ];
        write_packed(&path, &columns, None, &[], &test_meta(), Some(&kek)).unwrap();

        let footer = read_footer(&path).unwrap();
        assert!(footer.payload_encrypted);

        let ts_read = read_column(&path, &footer, "timestamp", Some(&kek)).unwrap();
        assert_eq!(ts_read, ts_data);

        let val_read = read_column(&path, &footer, "value", Some(&kek)).unwrap();
        assert_eq!(val_read, val_data);
    }
}
