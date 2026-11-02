// SPDX-License-Identifier: BUSL-1.1

use std::io::{Read, Write};
use std::path::Path;

use nodedb_wal::crypto::{
    SEGMENT_ENVELOPE_MAX_PLAINTEXT_BYTES, decrypt_segment_envelope, encrypt_segment_envelope,
};

use crate::types::Lsn;

/// Magic bytes identifying a NodeDB segment file.
const SEGMENT_MAGIC: [u8; 4] = *b"SYNS";

/// Segment format version. A footer stamped with any other version is refused.
const FORMAT_VERSION: u16 = 1;

/// Domain-specific magic for storage-segment AEAD envelopes.
///
/// This is intentionally distinct from every engine's segment magic so an
/// authenticated payload cannot be replayed into another envelope consumer.
const STORAGE_SEGMENT_ENVELOPE_MAGIC: [u8; 4] = *b"SSEG";

/// Footer size in bytes: magic(4) + version(2) + created_by(32) + checksum(4) + min_lsn(8) + max_lsn(8) = 58.
const FOOTER_SIZE: usize = 58;

/// Segment file footer.
///
/// All persistent files embed this footer for crash-safe validation.
/// Footer is written at the end of the file; readers seek to `file_len - FOOTER_SIZE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFooter {
    /// Format version. Always `FORMAT_VERSION`, checked on read.
    pub format_version: u16,
    /// Identifier of the process/node that created this segment.
    pub created_by: [u8; 32],
    /// CRC32C checksum of the segment data (excluding footer).
    pub checksum: u32,
    /// Minimum LSN of records in this segment.
    pub min_lsn: Lsn,
    /// Maximum LSN of records in this segment.
    pub max_lsn: Lsn,
}

impl SegmentFooter {
    /// Create a new footer for a segment.
    pub fn new(created_by: &str, checksum: u32, min_lsn: Lsn, max_lsn: Lsn) -> Self {
        let mut cb = [0u8; 32];
        let bytes = created_by.as_bytes();
        let len = bytes.len().min(32);
        cb[..len].copy_from_slice(&bytes[..len]);

        Self {
            format_version: FORMAT_VERSION,
            created_by: cb,
            checksum,
            min_lsn,
            max_lsn,
        }
    }

    /// Serialize the footer to bytes.
    pub fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..4].copy_from_slice(&SEGMENT_MAGIC);
        buf[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        buf[6..38].copy_from_slice(&self.created_by);
        buf[38..42].copy_from_slice(&self.checksum.to_le_bytes());
        buf[42..50].copy_from_slice(&self.min_lsn.as_u64().to_le_bytes());
        buf[50..58].copy_from_slice(&self.max_lsn.as_u64().to_le_bytes());
        buf
    }

    /// Deserialize a footer from bytes.
    pub fn from_bytes(buf: &[u8; FOOTER_SIZE]) -> crate::Result<Self> {
        if buf[0..4] != SEGMENT_MAGIC {
            return Err(crate::Error::SegmentCorrupted {
                detail: "invalid segment magic".into(),
            });
        }

        let format_version = u16::from_le_bytes([buf[4], buf[5]]);
        if format_version != FORMAT_VERSION {
            return Err(crate::Error::SegmentCorrupted {
                detail: format!(
                    "unsupported segment format version {format_version}, expected {FORMAT_VERSION}"
                ),
            });
        }
        let mut created_by = [0u8; 32];
        created_by.copy_from_slice(&buf[6..38]);
        let checksum = u32::from_le_bytes([buf[38], buf[39], buf[40], buf[41]]);
        let min_lsn = Lsn::new(u64::from_le_bytes([
            buf[42], buf[43], buf[44], buf[45], buf[46], buf[47], buf[48], buf[49],
        ]));
        let max_lsn = Lsn::new(u64::from_le_bytes([
            buf[50], buf[51], buf[52], buf[53], buf[54], buf[55], buf[56], buf[57],
        ]));

        Ok(Self {
            format_version,
            created_by,
            checksum,
            min_lsn,
            max_lsn,
        })
    }

    /// Append the footer to an existing segment file, durably.
    ///
    /// `File::flush` is a no-op on `File` — it only drains a userspace buffer,
    /// of which there is none — so it never made the footer survive power loss.
    /// The footer is the marker that makes a segment readable at all, so it
    /// must reach stable storage before this returns.
    pub fn write_to(&self, path: &Path) -> crate::Result<()> {
        let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
        file.write_all(&self.to_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    /// Read the footer from the end of a file.
    pub fn read_from(path: &Path) -> crate::Result<Self> {
        let mut file = std::fs::File::open(path)?;
        let file_len = file.metadata()?.len() as usize;
        if file_len < FOOTER_SIZE {
            return Err(crate::Error::SegmentCorrupted {
                detail: "file too small for segment footer".into(),
            });
        }

        use std::io::Seek;
        file.seek(std::io::SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut buf)?;

        Self::from_bytes(&buf)
    }

    /// Footer size in bytes.
    pub const fn size() -> usize {
        FOOTER_SIZE
    }
}

/// Write the local segment file `name` inside `dir`, with optional encryption.
///
/// Local plaintext files remain supported only when `key` is absent. Encrypted
/// files are a current authenticated envelope whose plaintext is
/// `[data || footer]`; the footer is never exposed outside the AEAD payload.
///
/// Published through the shared atomic helper: `File::create` + `write_all` +
/// `flush` left the final name pointing at a truncated or partial segment
/// across a crash, and `flush` on a `File` provides no durability at all. The
/// helper also rejects a `name` that is not one plain path component.
pub fn write_encrypted_segment(
    dir: &Path,
    name: &str,
    data: &[u8],
    footer: &SegmentFooter,
    key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<()> {
    let bytes = match key {
        Some(key) => encrypt_untrusted_segment_bytes(data, footer, key)?,
        None => plaintext_segment_bytes(data, footer)?,
    };
    nodedb_wal::segment::atomic_write_fsync(dir, name, &bytes).map_err(|e| crate::Error::Storage {
        engine: "segment".into(),
        detail: format!("publish segment {}: {e}", dir.join(name).display()),
    })
}

/// Encrypt a segment for an untrusted or object-store boundary.
///
/// The key is deliberately non-optional: plaintext objects and legacy epoch
/// envelopes are not valid interchange formats.
pub fn encrypt_untrusted_segment_bytes(
    data: &[u8],
    footer: &SegmentFooter,
    key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let plaintext = plaintext_segment_bytes(data, footer)?;
    encrypt_segment_envelope(key, &STORAGE_SEGMENT_ENVELOPE_MAGIC, &plaintext).map_err(|error| {
        crate::Error::Storage {
            engine: "segment".into(),
            detail: format!("segment envelope encryption failed: {error}"),
        }
    })
}

/// Decrypt an untrusted or object-store segment envelope.
pub fn decrypt_untrusted_segment_bytes(
    raw: &[u8],
    key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let plaintext =
        decrypt_segment_envelope(key, &STORAGE_SEGMENT_ENVELOPE_MAGIC, raw).map_err(|error| {
            crate::Error::SegmentCorrupted {
                detail: format!("invalid authenticated segment envelope: {error}"),
            }
        })?;
    split_plaintext_segment(&plaintext).map(|(data, _)| data.to_vec())
}

fn plaintext_segment_bytes(data: &[u8], footer: &SegmentFooter) -> crate::Result<Vec<u8>> {
    let plaintext_len =
        data.len()
            .checked_add(FOOTER_SIZE)
            .ok_or_else(|| crate::Error::SegmentCorrupted {
                detail: "segment plaintext length overflow".into(),
            })?;
    if plaintext_len > SEGMENT_ENVELOPE_MAX_PLAINTEXT_BYTES {
        return Err(crate::Error::SegmentCorrupted {
            detail: "segment plaintext exceeds authenticated-envelope limit".into(),
        });
    }
    let mut plaintext = Vec::with_capacity(plaintext_len);
    plaintext.extend_from_slice(data);
    plaintext.extend_from_slice(&footer.to_bytes());
    Ok(plaintext)
}

fn split_plaintext_segment(plaintext: &[u8]) -> crate::Result<(&[u8], SegmentFooter)> {
    let footer_start =
        plaintext
            .len()
            .checked_sub(FOOTER_SIZE)
            .ok_or_else(|| crate::Error::SegmentCorrupted {
                detail: "authenticated segment plaintext is too small for footer".into(),
            })?;
    let footer_bytes: [u8; FOOTER_SIZE] =
        plaintext[footer_start..]
            .try_into()
            .map_err(|_| crate::Error::SegmentCorrupted {
                detail: "authenticated segment footer is truncated".into(),
            })?;
    let footer = SegmentFooter::from_bytes(&footer_bytes)?;
    Ok((&plaintext[..footer_start], footer))
}

/// Read a local segment file's data portion.
///
/// A key requires a current authenticated envelope. With no key, this retains
/// the local-only plaintext `[data || footer]` behavior.
pub fn read_encrypted_segment(
    path: &Path,
    key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<Vec<u8>> {
    let raw = std::fs::read(path)?;
    match key {
        Some(key) => decrypt_untrusted_segment_bytes(&raw, key),
        None => split_plaintext_segment(&raw).map(|(data, _)| data.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x42u8; 32]).unwrap()
    }

    #[test]
    fn roundtrip_bytes() {
        let footer = SegmentFooter::new("node-1", 0xDEADBEEF, Lsn::new(10), Lsn::new(99));
        let bytes = footer.to_bytes();
        let parsed = SegmentFooter::from_bytes(&bytes).unwrap();
        assert_eq!(footer, parsed);
    }

    #[test]
    fn invalid_magic_rejected() {
        let mut bytes = [0u8; FOOTER_SIZE];
        bytes[0..4].copy_from_slice(b"NOPE");
        assert!(SegmentFooter::from_bytes(&bytes).is_err());
    }

    #[test]
    fn another_format_version_rejected() {
        let footer = SegmentFooter::new("node-1", 0, Lsn::new(1), Lsn::new(2));
        let mut bytes = footer.to_bytes();
        bytes[4..6].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        let err = SegmentFooter::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, crate::Error::SegmentCorrupted { .. }),
            "expected SegmentCorrupted, got {err:?}"
        );
    }

    #[test]
    fn write_and_read_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.seg");

        // Write some data + footer (unencrypted path).
        std::fs::write(&path, b"segment data here").unwrap();
        let footer = SegmentFooter::new("test", 42, Lsn::new(1), Lsn::new(50));
        footer.write_to(&path).unwrap();

        // Read back.
        let read_footer = SegmentFooter::read_from(&path).unwrap();
        assert_eq!(read_footer.checksum, 42);
        assert_eq!(read_footer.min_lsn, Lsn::new(1));
        assert_eq!(read_footer.max_lsn, Lsn::new(50));
    }

    #[test]
    fn write_and_read_file_encrypted_restart_roundtrip() {
        // equivalent for segment-store: write encrypted, simulate restart
        // by creating a new key instance (same bytes, fresh in-memory epoch),
        // and verify decryption still succeeds using the on-disk preamble epoch.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enc.seg");

        let data = b"secret segment payload";
        let footer = SegmentFooter::new("node-1", 0xABCD, Lsn::new(5), Lsn::new(100));

        // Write with key_v1 (epoch chosen randomly at construction).
        let key_v1 = test_key();
        write_encrypted_segment(dir.path(), "enc.seg", data, &footer, Some(&key_v1)).unwrap();

        // Simulate restart: new key instance with same bytes but different
        // in-memory epoch. Decryption MUST use the epoch from the on-disk preamble.
        let key_v2 = test_key(); // fresh random epoch
        let plaintext = read_encrypted_segment(&path, Some(&key_v2)).unwrap();
        assert_eq!(plaintext, data);
    }

    #[test]
    fn envelope_authenticates_data_and_footer() {
        let data = b"layout test";
        let footer = SegmentFooter::new("n", 0, Lsn::new(1), Lsn::new(1));
        let key = test_key();
        let raw = encrypt_untrusted_segment_bytes(data, &footer, &key).unwrap();

        assert_eq!(&raw[..4], &STORAGE_SEGMENT_ENVELOPE_MAGIC);
        assert_eq!(
            raw.len(),
            nodedb_wal::crypto::SEGMENT_ENVELOPE_PREAMBLE_SIZE
                + data.len()
                + FOOTER_SIZE
                + nodedb_wal::crypto::AUTH_TAG_SIZE
        );
        assert!(!raw.ends_with(b"SYNS"), "footer must be inside AEAD");
        assert_eq!(decrypt_untrusted_segment_bytes(&raw, &key).unwrap(), data);

        let mut tampered = raw;
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(decrypt_untrusted_segment_bytes(&tampered, &key).is_err());
    }

    #[test]
    fn identical_segments_use_independent_current_envelopes() {
        let data = b"same data, same lsn";
        let footer = SegmentFooter::new("n", 0, Lsn::ZERO, Lsn::ZERO);
        let key = test_key();
        let first = encrypt_untrusted_segment_bytes(data, &footer, &key).unwrap();
        let second = encrypt_untrusted_segment_bytes(data, &footer, &key).unwrap();
        assert_ne!(first, second);
        assert_eq!(decrypt_untrusted_segment_bytes(&first, &key).unwrap(), data);
        assert_eq!(
            decrypt_untrusted_segment_bytes(&second, &key).unwrap(),
            data
        );
    }

    #[test]
    fn plaintext_and_legacy_epoch_layout_are_rejected_at_untrusted_boundary() {
        let footer = SegmentFooter::new("n", 0, Lsn::ZERO, Lsn::ZERO);
        let key = test_key();
        let plaintext = plaintext_segment_bytes(b"crc-valid plaintext", &footer).unwrap();
        assert!(decrypt_untrusted_segment_bytes(&plaintext, &key).is_err());

        let mut legacy = vec![0u8; 16];
        legacy[..4].copy_from_slice(b"SEGP");
        legacy.extend_from_slice(&plaintext);
        assert!(decrypt_untrusted_segment_bytes(&legacy, &key).is_err());
    }

    #[test]
    fn unencrypted_segment_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.seg");

        let data = b"plain data";
        let footer = SegmentFooter::new("n", 99, Lsn::new(1), Lsn::new(5));

        write_encrypted_segment(dir.path(), "plain.seg", data, &footer, None).unwrap();
        let read_back = read_encrypted_segment(&path, None).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn created_by_truncates_long_names() {
        let footer = SegmentFooter::new(
            "this-is-a-very-long-node-name-that-exceeds-32-bytes",
            0,
            Lsn::ZERO,
            Lsn::ZERO,
        );
        // Should not panic, truncates to 32 bytes.
        let bytes = footer.to_bytes();
        let parsed = SegmentFooter::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.created_by[..4], *b"this");
    }

    /// The segment is published by rename, so the destination is never left
    /// truncated and the staging file never survives.
    #[test]
    fn write_publishes_atomically_and_leaves_no_staging_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.seg");
        let footer = SegmentFooter::new("n", 1, Lsn::new(1), Lsn::new(2));

        write_encrypted_segment(
            dir.path(),
            "plain.seg",
            b"first version, longer",
            &footer,
            None,
        )
        .unwrap();
        write_encrypted_segment(dir.path(), "plain.seg", b"second", &footer, None).unwrap();

        assert_eq!(read_encrypted_segment(&path, None).unwrap(), b"second");
        let entries: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter_map(|e| e.file_name().to_str().map(str::to_string))
            .collect();
        assert_eq!(entries, vec!["plain.seg".to_string()]);
    }

    #[test]
    fn lsn_ordering_preserved() {
        let footer = SegmentFooter::new("n", 0, Lsn::new(100), Lsn::new(200));
        assert!(footer.min_lsn < footer.max_lsn);
    }
}
